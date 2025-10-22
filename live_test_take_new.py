import json
import time
import requests
from datetime import datetime, timezone
import time, json, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import re
import random, time
import os, json, time, math
from datetime import datetime, timedelta, timezone
import gzip
import glob
import time, traceback, sys
import faulthandler, signal
import os, sys, time, json, requests, signal, traceback
import psutil
from datetime import datetime, timezone, timedelta

# --- Crash tracing & signals (keep file handle open!) ---
FAULT_FH = open("faulthandler.log", "a", buffering=1, encoding="utf-8")
import faulthandler, signal, psutil, sys, os, time
faulthandler.enable(FAULT_FH)

# Graceful stop on SIGTERM (Linux/macOS). On Windows, use CTRL_BREAK_EVENT for services.
try:
    signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))
except Exception:
    pass  # not available on some platforms

# One place to decide log directory (no blocking input)
name_log = input("name log: ")
if name_log:
    LOG_DIR = os.path.join("logs", name_log)
else:
    LOG_DIR = os.path.join("logs", f"logs_run_{int(time.time())}")

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_BOOK = "https://clob.polymarket.com/book"
DATA_TRADES = "https://data-api.polymarket.com/trades"
TRADE_LOG_BASE = "trades_taken.jsonl"
RUN_SNAP_BASE  = "run_snapshots.jsonl"
DECISION_LOG_BASE = "decisions.jsonl"
DECISION_LOG_SAMPLE = 0.001  # 15% sampling
DECISION_LOG_SNAPSHOT = 0.001
VERBOSE = True
SHOW_DEBUG_BOOKS = False  # Set True to fetch YES/NO books on skipped markets for inspection
RETAIN_DAYS = 7           # delete logs older than this
COMPRESS_AFTER_DAYS = 1   # gzip logs older than this (but not today's)
_created_cutoff = None

# ----- full-ticket helpers -----

FULL_TICKET_DOLLARS = 100.0         # your per-market bet size
VM_GRACE_SECONDS    = 2              # require best ask under limit this long before "fill"
VM_LIMIT_PAD        = 1.00           # 1.00 = exactly at cap; e.g. 1.02 means 2% looser than cap (capped at 1.00)
VM_TIMEOUT_SECONDS  = 60 * 45        # cancel virtual maker if not filled after this long

# Reservations so we don't over-commit while an order is "waiting"
reserved_by_cid = {}   # cid -> dollars reserved for the resting virtual maker

def available_bank(bank: float) -> float:
    return bank - sum(reserved_by_cid.values())

def reserve_for_vm(cid: str, dollars: float):
    reserved_by_cid[cid] = dollars

def release_vm_reservation(cid: str):
    reserved_by_cid.pop(cid, None)

def vm_limit_from_cap(price_cap_inc_fee: float) -> float:
    return min(1.0, price_cap_inc_fee * VM_LIMIT_PAD)


# ----------------------------------------------------------------------
#network helpers
# ----------------------------------------------------------------------
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,  # 0.5, 1.0, 2.0, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "research-bot/1.0"})
    return s
SESSION = make_session()

# --- Simple global RPS limiter ---------------------------------------
RPS_TARGET = 10.0     # choose 2–5 to stay very safe
_last_tokens_ts = time.monotonic()
_bucket = RPS_TARGET

def _rate_limit():
    global _last_tokens_ts, _bucket
    now = time.monotonic()
    # refill bucket
    _bucket = min(RPS_TARGET, _bucket + (now - _last_tokens_ts) * RPS_TARGET)
    _last_tokens_ts = now
    if _bucket < 1.0:
        # need to wait for 1 token
        need = (1.0 - _bucket) / RPS_TARGET
        time.sleep(need)
        now2 = time.monotonic()
        _bucket = min(RPS_TARGET, _bucket + (now2 - _last_tokens_ts) * RPS_TARGET)
        _last_tokens_ts = now2
    _bucket -= 1.0

NET_LOG_BASE = "net_usage.jsonl"
bytes_in_total = 0

def log_net_usage():
    append_jsonl(NET_LOG_BASE, {
        "ts": datetime.now(timezone.utc).isoformat(),
        "bytes_in_total": bytes_in_total
    })

# ----------------------------------------------------------------------
# VirtualMaker: local simulation of maker bids
# ----------------------------------------------------------------------
class VirtualMaker:
    def __init__(self, mode="optimistic", queue_grace_s=2.0):
        """
        mode: "optimistic" = fill as soon as best ask <= limit_inc (after grace)
              "conservative" = fill only after cumulative sell volume >= your order
        queue_grace_s: seconds best ask must stay <= your limit before we count it as filled
        """
        self.mode = mode
        self.queue_grace_s = queue_grace_s
        self.orders = {}  # cid -> dict(order info)

    def place_no_bid(self, condition_id: str, token_id: str,
                     limit_inc: float, budget_usd: float, now_dt):
        """Register a simulated maker bid."""
        if not condition_id or not token_id:
            return False
        ts = int(now_dt.timestamp())
        self.orders[condition_id] = {
            "token_id": token_id,
            "limit_inc": limit_inc,
            "budget_usd": budget_usd,
            "placed_ts": ts,
            "placed_iso": now_dt.isoformat(),
            "first_seen_under": None,  # when best ask first dipped under limit
            "filled": False,
            "fill_ts": None,
        }
        return True

    def on_book(self, condition_id: str, book: dict, now_dt, last_seen_under_dict=None):
        """
        Called each time we fetch a fresh book for a condition_id.
        Returns a fill dict {fill_px, fill_shares, fill_cost} if the order
        would have filled, otherwise None.
        """
        rec = self.orders.get(condition_id)
        if not rec or rec.get("filled"):
            return None

        best_bid, best_ask = best_of_book(book)
        if best_ask is None:
            return None

        # did market move through our limit?
        if best_ask <= rec["limit_inc"]:
            if rec["first_seen_under"] is None:
                rec["first_seen_under"] = int(now_dt.timestamp())
            elapsed = int(now_dt.timestamp()) - rec["first_seen_under"]
            if elapsed >= self.queue_grace_s:
                # mark as filled
                rec["filled"] = True
                rec["fill_ts"] = int(now_dt.timestamp())
                fill_px = best_ask
                fill_cost = rec["budget_usd"]
                fill_shares = fill_cost / fill_px
                return {
                    "fill_px": fill_px,
                    "fill_shares": fill_shares,
                    "fill_cost": fill_cost,
                    "mode": self.mode,
                }
        else:
            # price went back above limit, reset grace timer
            rec["first_seen_under"] = None
        return None

#---------------------------------------------------------------
# vm
#---------------------------------------------------------------
def place_or_keep_vm_order(cid: str, market: dict, maker, mgr, desired_dollars: float):
    """
    Ensure exactly one resting VM order exists for the full ticket on this cid.
    If not exist, place it and reserve funds. If already exists, do nothing.
    """
    if cid in maker.orders:
        return  # already resting

    no_tok = get_no_token_id(market)
    if not no_tok:
        return

    limit_inc = vm_limit_from_cap(mgr.max_no_price)
    ok = maker.place_no_bid(
        condition_id=cid,
        token_id=no_tok,
        limit_inc=limit_inc,
        budget_usd=desired_dollars,
        now_dt=datetime.now(timezone.utc),
    )
    if ok:
        reserve_for_vm(cid, desired_dollars)
        vprint(f"    [VM] placed ${desired_dollars:.2f} @ {limit_inc:.4f} (inc fee)")


def vm_maybe_fill_on_book(cid: str, market: dict, book: dict, maker, bank_ref):
    """
    Check if the virtual maker would have filled on this new book snapshot.
    If so, deduct bank, create position (full ticket), log, and clean up reservation.
    Returns True if a full-ticket fill was recorded.
    """
    vm_fill = maker.on_book(
        condition_id=cid,
        book=book,
        now_dt=datetime.now(timezone.utc),
        last_seen_under_dict=_last_under_seen,   # you already have this dict
    )
    if not vm_fill:
        # optional timeout clean-up
        rec = maker.orders.get(cid)
        if rec and (int(datetime.now(timezone.utc).timestamp()) - rec["placed_ts"] >= VM_TIMEOUT_SECONDS):
            vprint(f"    [VM] timeout → cancel ${rec['budget_usd']:.2f}")
            maker.orders.pop(cid, None)
            release_vm_reservation(cid)
        return False

    # ---- Full-ticket maker "fill" ----
    spent_after = float(vm_fill["fill_cost"])
    shares      = float(vm_fill["fill_shares"])
    avg_price   = float(vm_fill["fill_px"])     # price (inc fee for maker if you included it in limit_inc)

    # Bank/accounting
    bank_ref["value"] -= spent_after
    positions_by_id[cid] = {"shares": shares, "side": "NO", "cost": spent_after}
    release_vm_reservation(cid)

    # Peaks + snapshots (uses your existing helpers/globals)
    global first_trade_dt, locked_now, peak_locked, peak_locked_time
    if first_trade_dt is None:
        first_trade_dt = datetime.now(timezone.utc)
    locked_now = compute_locked_now()
    if locked_now > peak_locked:
        peak_locked = locked_now
        peak_locked_time = datetime.now(timezone.utc)

    # Logs
    print(f"\n✅ ENTER NO (VM) | {market.get('question')}")
    print(f"   spent(after)={spent_after:.2f} | shares={shares:.2f} | avg_price={avg_price:.4f} | bank={bank_ref['value']:.2f} | locked_now={locked_now:.2f}")

    # best-of-book for the log line
    bb, ba = best_of_book(book or {})
    append_jsonl(TRADE_LOG_BASE, {
        "ts": datetime.now(timezone.utc).isoformat(),
        "market_id": market.get("conditionId"),
        "market_slug": market.get("slug"),
        "question": market.get("question"),
        "side": "NO",
        "token_id": get_no_token_id(market),
        "spent_after": round(spent_after, 6),
        "shares": round(shares, 6),
        "avg_price_eff": round(avg_price, 6),
        "book_best_bid": bb, "book_best_ask": ba,
        "locked_now": round(float(locked_now), 6),
        "potential_value_if_all_win": round(compute_potential_value_if_all_win(), 6),
        "bank_after_open": round(float(bank_ref["value"]), 6),
        "fill_mode": "virtual_maker",
    })

    append_jsonl("virtual_fills.jsonl", {
        "ts": datetime.now(timezone.utc).isoformat(),
        "market_id": cid,
        "question": market.get("question"),
        "mode": "optimistic",   # or "conservative" depending on your VM config
        "fill_px": avg_price,
        "fill_shares": shares,
        "fill_cost": spent_after,
        "placed_iso": maker.orders[cid]["placed_iso"] if cid in maker.orders else None,
        "fill_iso": datetime.now(timezone.utc).isoformat(),
    })

    # Clean VM state for this cid
    maker.orders.pop(cid, None)
    return True

def open_position_fn_strict_full_ticket(cid, market, side, dollars, best_ask, book,
                                        *, maker, mgr, bank_ref, fee, slip, price_cap):
    """
    Enforce all-or-nothing full ticket:
      - Try TAKING the whole ticket now under cap (inc fee/slip).
      - If not possible, place/keep a single VirtualMaker order for the FULL_TICKET_DOLLARS and return False.
    `bank_ref` is a dict like {"value": <float>} so we can mutate it from closures.
    """
    if side != "NO":
        return False

    desired = float(FULL_TICKET_DOLLARS)

    # Don't start new tickets if we don't have free cash after reservations
    if available_bank(bank_ref["value"]) < desired - 1e-6:
        vprint("    [SKIP] not enough available bank after reservations")
        return False

    # ---- TAKER attempt for the full ticket
    spent_after, shares, avg_price = simulate_take_from_asks(
        book, desired, fee=fee, slip=slip, price_cap=price_cap
    )

    if spent_after >= desired - 1e-6 and shares > 0:
        # Full ticket filled instantly
        bank_ref["value"] -= spent_after
        positions_by_id[cid] = {"shares": shares, "side": "NO", "cost": spent_after}

        global first_trade_dt, locked_now, peak_locked, peak_locked_time
        if first_trade_dt is None:
            first_trade_dt = datetime.now(timezone.utc)
        locked_now = compute_locked_now()
        if locked_now > peak_locked:
            peak_locked = locked_now
            peak_locked_time = datetime.now(timezone.utc)

        print(f"\n✅ ENTER NO (TAKER) | {market.get('question')}")
        print(f"   spent(after)={spent_after:.2f} | shares={shares:.2f} | avg_price={avg_price:.4f} | bank={bank_ref['value']:.2f} | locked_now={locked_now:.2f}")

        log_open_trade(market, "NO", get_no_token_id(market), book_used=book,
                       spent_after=spent_after, shares=shares,
                       avg_price_eff=avg_price, bank_after_open=bank_ref["value"])

        return True

    # ---- Could not fill instantly → ensure a full-ticket VM order exists
    place_or_keep_vm_order(cid, market, maker, mgr, desired)
    return False

# --------------------------------------------------------------------
# Book fetch
# --------------------------------------------------------------------
def fetch_book(token_id: str, depth: int = 80, session=SESSION):
    _rate_limit()
    r = session.get(BASE_BOOK, params={"token_id": token_id}, timeout=15)
    r.raise_for_status()
    # count compressed payload bytes
    body = r.content
    book = r.json() if body else {}
    # normalize & trim
    book["bids"] = (book.get("bids") or [])[:depth]
    book["asks"] = (book.get("asks") or [])[:depth]

    # bump counters
    global bytes_in_total
    bytes_in_total += len(body) if body else 0
    return book


def outcome_map_from_market(market: dict) -> dict:
    """
    Returns {"YES": <token_id_yes>, "NO": <token_id_no>}.
    Handles Gamma shapes where outcomes/clobTokenIds might be strings or lists.
    """
    outs = market.get("outcomes")
    toks = market.get("clobTokenIds")

    if isinstance(outs, str):
        try: outs = json.loads(outs)
        except: outs = None
    if isinstance(toks, str):
        try: toks = json.loads(toks)
        except: toks = None

    # Common case: 2 outcomes named Yes/No
    if isinstance(outs, list) and isinstance(toks, list) and len(outs) == 2 and len(toks) == 2:
        o0, o1 = str(outs[0]).strip().upper(), str(outs[1]).strip().upper()
        if (o0, o1) == ("YES", "NO"):
            return {"YES": toks[0], "NO": toks[1]}
        if (o0, o1) == ("NO", "YES"):
            return {"YES": toks[1], "NO": toks[0]}
        # fuzzy
        if "NO" in o0 and "YES" in o1:  # "No/Yes"
            return {"YES": toks[1], "NO": toks[0]}
        if "YES" in o0 and "NO" in o1:  # "Yes/No"
            return {"YES": toks[0], "NO": toks[1]}

    # Fallbacks
    if isinstance(toks, list) and len(toks) == 2:
        # last resort: assume [YES, NO]
        return {"YES": toks[0], "NO": toks[1]}

    raise ValueError(f"Cannot resolve YES/NO token ids for market {market.get('id') or market.get('conditionId')}")


def get_no_token_id(market: dict) -> str | None:
    try:
        return outcome_map_from_market(market)["NO"]
    except Exception:
        return None
    
def _dt(s):
    try:
        return datetime.fromisoformat(str(s).replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

_last_under_seen = {}

# --------------------------------------------------------------------
# Watchlist Manager
# --------------------------------------------------------------------
def vprint(*a, **k):
    if VERBOSE:
        print(*a, **k)

class WatchlistManager:
    def __init__(self,
                 max_no_price=0.40,
                 min_notional=20.0,
                 fee_bps=600, slip_bps=200,
                 dust_price=0.02, dust_min_notional=20.0,
                 poll_every=3,            # more aggressive for debugging
                 backoff_first=4,
                 backoff_base=8,
                 backoff_max=120,
                 jitter=2):
        self.max_no_price = max_no_price
        self.min_notional = min_notional
        self.fee = fee_bps / 10_000.0
        self.slip = slip_bps / 10_000.0
        self.dust_price = dust_price
        self.dust_min_notional = dust_min_notional
        self.poll_every = poll_every
        self.backoff_first = backoff_first
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.jitter = jitter

        self.watch = {}       # conditionId -> {m, no_token, next_check, fails, last_quote}
        self.entered = set()  # conditionIds already entered

    def seed_from_gamma(self, markets: list[dict]):
        now = int(time.time())
        added = 0
        for m in markets:
            cid = m.get("conditionId")
            if not cid or cid in self.watch or cid in self.entered:
                continue
            no_token = get_no_token_id(m)
            if not no_token:
                continue
            self.watch[cid] = {
                "m": m,
                "no_token": no_token,
                "next_check": now + random.randint(0, 2),
                "fails": 0,
                "last_quote": None,
                "last_seen_ts": now,
                "last_seq": None,
                "last_book_sig": None,   # optional
                "last_liq_sig": None,    # optional
                "ever_under_cap": False,
                "seeded_ts": now,
            }
            added += 1
        vprint(f"[SEED] added {added} markets (watch size now {len(self.watch)})")

    def due_ids(self, now_ts: int):
        return [cid for cid, st in self.watch.items()
                if st["next_check"] <= now_ts and cid not in self.entered]

    def _backoff(self, fails: int) -> int:
        if fails <= 1:
            base = self.backoff_first
        else:
            base = min(self.backoff_base * (2 ** (fails - 1)), self.backoff_max)
        return max(1, base + random.randint(0, self.jitter))

    def _effective_price(self, p: float) -> float:
        return p * (1.0 + self.fee + self.slip)

    def _valid_no_from_book(self, book: dict):
        """
        Walk asks (print them), aggregate pre-fee dollars under cap, and total shares.
        Returns (takeable_notional_ex_fee, best_ask_price, shares_at_cap, reasons[])
        """
        asks = book.get("asks") or []
        reasons = []
        if not asks:
            reasons.append("no_asks")
            return 0.0, None, 0.0, reasons

        # Print all visible asks
        for idx, a in enumerate(asks):
            try:
                p = float(a["price"]); s = float(a["size"])
                ep = self._effective_price(p)
                vprint(f"   [ASK {idx}] p={p:.4f} ep={ep:.4f} size={s:.6f}")
            except Exception:
                vprint(f"   [ASK {idx}] {a!r}")

        takeable_ex = 0.0
        shares = 0.0
        best_price = None
        skipped = 0

        for a in asks:
            try:
                p = float(a["price"]); s = float(a["size"])
            except Exception:
                continue
            ep = self._effective_price(p)
            if ep > self.max_no_price:
                skipped += 1
                continue
            if best_price is None:
                best_price = p
            takeable_ex += p * s
            shares += s

        if skipped:
            reasons.append(f"skipped_{skipped}_over_cap")
        if best_price is None:
            reasons.append("no_asks_under_cap")

        return takeable_ex, best_price, shares, reasons

    def step(self,
         now_ts: int,
         fetch_book_fn,
         open_position_fn,
         bet_size_fn,
         *,
         maker=None,
         bank_ref=None,
         max_checks_per_tick=200,
         min_probe_when_idle=None,
         probe_strategy="newest"):
        """
        Returns (opened_count, checked_count).
        - If too few are due, we proactively probe a few more (min_probe_when_idle)
          so you can chew through a 3-day backlog quickly.
        """
        due = self.due_ids(now_ts)
        # Prioritize newest markets (and most-recently created) first
        due.sort(key=lambda cid: (
            self.watch[cid].get("seeded_ts", 0),
            self.watch[cid]["m"].get("startDate") or self.watch[cid]["m"].get("createdAt") or ""
        ), reverse=True)
        if min_probe_when_idle is None:
            min_probe_when_idle = max(40, min(200, len(self.watch)//5))
        # If backlog is large but not many due, force-probe a slice:
        if len(due) < min_probe_when_idle:
            # pick some IDs deterministically
            all_ids = list(self.watch.keys())
            # strategy: "newest" or "oldest" by startDate
            if probe_strategy in ("newest", "oldest"):
                def sd(cid):
                    m = self.watch[cid]["m"]
                    return (m.get("startDate") or m.get("createdAt") or "")
                all_ids.sort(key=sd, reverse=(probe_strategy == "newest"))
            probe_ids = [cid for cid in all_ids if cid not in self.entered][:min_probe_when_idle]
            # mark them due “now”
            for cid in probe_ids:
                self.watch[cid]["next_check"] = now_ts
            due = self.due_ids(now_ts)

        opened = 0
        checked = 0
        due.sort(key=lambda cid: (not self.watch[cid].get("ever_under_cap", False),
                          self.watch[cid].get("next_check", 0)))
        vprint(f"[STEP] due={len(due)} watch={len(self.watch)} entered={len(self.entered)}")
        for cid in due[:max_checks_per_tick]:
            st = self.watch.get(cid)
            if not st:
                continue
            checked += 1
            m = st["m"]
            vprint(f" -> checking {cid} | {m.get('question')[:80]}")

            try:
                book = fetch_book_fn(st["no_token"])
                st["last_quote"] = book
                st["last_seen_ts"] = now_ts

                if maker is not None and bank_ref is not None:
                    if vm_maybe_fill_on_book(cid, m, book, maker, bank_ref):
                        self.entered.add(cid)
                        self.watch.pop(cid, None)
                        vprint("    ✅ VM FILLED; removed from watch")
                        continue

                # 1) Build change signals
                seq      = book.get("sequence") or book.get("version")
                prev_seq = st.get("last_seq")

                curr_book_sig = book_fingerprint(book, depth=10)
                curr_liq_sig  = liquidity_signature_under_cap(book, self.max_no_price, self.fee, self.slip)

                prev_book_sig = st.get("last_book_sig")
                prev_liq_sig  = st.get("last_liq_sig")

                changed, reason = book_changed(prev_book_sig, curr_book_sig, prev_liq_sig, curr_liq_sig, prev_seq, seq)

                # Persist signatures/seq for next tick
                st["last_seq"]      = seq or prev_seq
                st["last_book_sig"] = curr_book_sig
                st["last_liq_sig"]  = curr_liq_sig

                if changed:
                    st["fails"] = 0
                    st["next_check"] = now_ts + max(1, self.backoff_first // 2)
                    vprint(f"    [BOOK UPDATE] reason={reason}")

                # 2) Evaluate under-cap liquidity for this tick
                takeable, best_ask, shares, reasons = self._valid_no_from_book(book)
                if best_ask is not None and shares > 0:
                    st["ever_under_cap"] = True
                vprint(f"    best_ask={best_ask} | takeable=${takeable:.2f} | shares_at_cap={shares:.2f}")

                if SHOW_DEBUG_BOOKS and (best_ask is None or "no_asks_under_cap" in reasons):
                    debug_show_books_for_market(m)

                maybe_log_decision(
                    m, self.max_no_price, bet_size_fn(takeable), book,
                    takeable, best_ask, shares, reasons,
                    result=None
                )

                # 3) Act if there’s takeable size
                if takeable > 0:
                    dollars = bet_size_fn(takeable)
                    vprint(f"    TRY OPEN NO for ${dollars:.2f} (<= takeable)")
                    ok = open_position_fn(cid, m, "NO", dollars, best_ask, book)
                    if ok:
                        maybe_log_decision(
                            m, self.max_no_price, dollars, book,
                            takeable, best_ask, shares, reasons,
                            result={"ok": True}
                        )
                        self.entered.add(cid)
                        self.watch.pop(cid, None)
                        opened += 1
                        vprint("    ✅ OPENED; removed from watch")
                        continue
                    else:
                        maybe_log_decision(
                            m, self.max_no_price, dollars, book,
                            takeable, best_ask, shares, reasons,
                            result={"ok": False, "why": "open_position_fn_false"}
                        )
                        st["fails"] = 0
                        st["next_check"] = now_ts + max(1, self.backoff_first // 2)
                        vprint("    ⚠️ had_liquidity_but_no_fill → quick retry scheduled")
                        continue

                # 4) No liquidity → backoff (fast if close)
                st["fails"] += 1
                fast = ("skipped_" in " ".join(reasons)) or ("no_asks" not in reasons)
                next_in = max(1, (self.backoff_first if fast else self._backoff(st["fails"])))
                st["next_check"] = now_ts + next_in
                vprint(f"    SKIP (reasons={reasons}) → fails={st['fails']} next_check=+{next_in}s")

            except Exception as e:
                st["fails"] += 1
                next_in = self._backoff(st["fails"])
                st["next_check"] = now_ts + next_in
                vprint(f"    [ERR] {e} → fails={st['fails']} next_check=+{next_in}s")

        return opened, checked

    def purge_stale(self, now_ts: int, ttl_seconds: int = 48*3600, protect_if_ever_under_cap: bool = True):
        """
        Remove markets we haven't 'seen' (book fetched) in ttl_seconds.
        If protect_if_ever_under_cap is True, keep markets that *once* had under-cap liquidity.
        """
        stale = []
        for cid, st in list(self.watch.items()):
            last_seen = st.get("last_seen_ts", now_ts)
            if now_ts - last_seen > ttl_seconds:
                if protect_if_ever_under_cap and st.get("ever_under_cap"):
                    continue  # keep; it was interesting at least once
                stale.append(cid)

        for cid in stale:
            self.watch.pop(cid, None)

        if stale:
            vprint(f"[PURGE] removed {len(stale)} stale markets (ttl={ttl_seconds}s)")


def book_fingerprint(book: dict, depth: int = 10):
    """
    Return a deterministic fingerprint of top-N levels on both sides.
    Use rounding to suppress micro-churn.
    """
    def norm_side(levels):
        out = []
        for lv in (levels or [])[:depth]:
            try:
                p = round(float(lv.get("price", 0.0)), 5)
                s = round(float(lv.get("size",  0.0)), 6)
                if p > 0 and s > 0:
                    out.append((p, s))
            except Exception:
                continue
        return tuple(out)

    bids = norm_side(book.get("bids"))
    asks = norm_side(book.get("asks"))
    return (bids, asks)

def liquidity_signature_under_cap(book: dict, cap_inc_fee: float, fee: float, slip: float):
    """
    Summarize only what's useful for your NO-taking:
      - best ask under cap
      - shares under cap
      - notional ex-fee under cap
    """
    fee_mult = 1.0 + float(fee) + float(slip)
    cap_ex = cap_inc_fee / fee_mult

    best = None
    shares = 0.0
    notional_ex = 0.0

    for a in (book.get("asks") or []):
        try:
            p = float(a.get("price", 0.0))
            s = float(a.get("size",  0.0))
        except Exception:
            continue
        if p <= 0 or s <= 0:
            continue
        if p <= cap_ex:
            if best is None or p < best:
                best = p
            shares      += s
            notional_ex += p * s

    # round to stabilize the signature
    best_r   = round(best, 5) if best is not None else None
    shares_r = round(shares, 6)
    ex_r     = round(notional_ex, 4)
    return (best_r, shares_r, ex_r)

def book_changed(prev_book_sig, curr_book_sig, prev_liq_sig, curr_liq_sig, prev_seq, curr_seq):
    """
    Decide whether to treat this as 'new info'.
    Priority: sequence change OR liquidity-under-cap change OR whole-book change.
    """
    if curr_seq and (curr_seq != prev_seq):
        return True, "sequence_changed"
    if curr_liq_sig != prev_liq_sig:
        return True, "liq_under_cap_changed"
    if curr_book_sig != prev_book_sig:
        return True, "top_depth_changed"
    return False, ""


def cap_for_raw(raw, fee_bps, slip_bps):
    return raw * (1 + fee_bps/10000 + slip_bps/10000)

def compute_locked_now():
    # Total cost basis still tied up in open positions
    return sum(float(p.get("cost", 0.0)) for p in positions_by_id.values())

def probe_under_cap_sample(mgr, n=30, depth=20):
    ok = 0
    total = 0
    samples = []
    for cid, st in list(mgr.watch.items())[:n]:
        total += 1
        try:
            book = fetch_book(st["no_token"], depth=depth)
            asks = book.get("asks") or []
            fee_mult = 1.0 + mgr.fee + mgr.slip
            under = [
                (float(a["price"]), float(a["size"]))
                for a in asks
                if float(a.get("price", 0)) > 0 and float(a.get("size", 0)) > 0
                and (float(a["price"]) * fee_mult) <= mgr.max_no_price
            ]
            if under:
                ok += 1
                # keep a tiny sample line for visibility
                p0, s0 = under[0]
                samples.append((cid, p0, s0))
        except Exception:
            pass
    vprint(f"[PROBE] under-cap markets in sample: {ok}/{total}")
    for cid, p, s in samples[:5]:
        vprint(f"   -> {cid} first under-cap ask ~ p={p:.4f} size={s:.4f}")

def quick_scan_yes_no(mgr, n=20):
    from math import inf
    fee_mult = 1.0 + mgr.fee + mgr.slip
    cnt_no_under = cnt_yes_under = 0
    for cid, st in list(mgr.watch.items())[:n]:
        m = st["m"]
        om = outcome_map_from_market(m)
        nb = fetch_book(om["NO"], depth=50)
        yb = fetch_book(om["YES"], depth=50)
        min_no = min((float(a["price"]) for a in (nb.get("asks") or []) if a.get("price")), default=inf)
        min_yes = min((float(a["price"]) for a in (yb.get("asks") or []) if a.get("price")), default=inf)
        if min_no < inf and min_no*fee_mult <= mgr.max_no_price: cnt_no_under += 1
        if min_yes < inf and min_yes*fee_mult <= mgr.max_no_price: cnt_yes_under += 1
    print(f"[SCAN] NO under-cap in sample: {cnt_no_under}/{n} | YES under-cap: {cnt_yes_under}/{n}")

def best_of_book(book):
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    try:
        best_bid = max((float(b["price"]) for b in bids if "price" in b), default=None)
    except ValueError:
        best_bid = None
    try:
        best_ask = min((float(a["price"]) for a in asks if "price" in a), default=None)
    except ValueError:
        best_ask = None
    return best_bid, best_ask

# --------------------------------------------------------------------
# Fast open-market fetch
# --------------------------------------------------------------------
def fetch_open_yesno_fast(limit=250, max_pages=10, days_back=90,
                          require_clob=True, min_liquidity=None, min_volume=None,
                          session=SESSION, verbose=True):
    params = {
        "limit": limit,
        "order": "startDate",
        "ascending": False,
        "closed": False,
    }
    if days_back:
        params["start_date_min"] = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
    if require_clob:
        params["enableOrderBook"] = True
    if min_liquidity is not None:
        params["liquidity_num_min"] = float(min_liquidity)
    if min_volume is not None:
        params["volume_num_min"] = float(min_volume)

    all_rows, seen_ids = [], set()
    offset, pages = 0, 0

    while pages < max_pages:
        q = dict(params, offset=offset)
        try:
            r = session.get(BASE_GAMMA, params=q, timeout=20)
            r.raise_for_status()
        except Exception as e:
            if verbose: print(f"[WARN] fetch failed at offset {offset} with {q}: {e}")
            break

        page = r.json() or []
        if not page:
            if verbose: print("[INFO] empty page; done.")
            break

        added = 0
        for m in page:
            outs = m.get("outcomes")
            if isinstance(outs, str):
                try: outs = json.loads(outs)
                except: outs = None
            def is_yesno(lst):
                if not isinstance(lst, list) or len(lst) != 2: return False
                s = [str(x).strip().lower() for x in lst]
                return set(s) == {"yes", "no"}
            if is_yesno(outs):
                mid = m.get("id")
                if mid not in seen_ids:
                    all_rows.append(m)
                    seen_ids.add(mid)
                    added += 1

        if verbose:
            print(f"[PAGE {pages}] got {len(page)} raw, added {added} new, total {len(all_rows)}")

        if len(page) < limit:
            if verbose: print("[INFO] last page (short).")
            break
        pages += 1
        offset += limit

    # sort newest → oldest by startDate or createdAt
    all_rows.sort(key=lambda m: (m.get("startDate") or m.get("createdAt") or ""), reverse=True)
    if verbose:
        print(f"✅ Total open Yes/No markets: {len(all_rows)}")
    return all_rows

#current
def is_actively_tradable(m):
    if not m.get("enableOrderBook"): return False
    toks = m.get("clobTokenIds"); 
    if isinstance(toks, str):
        try: toks=json.loads(toks)
        except: toks=[]
    q = (m.get("question") or "").lower()
    # Skip range/between/greater-than style if you want simpler binarys:
    if any(w in q for w in ["between", "range", "greater than", "less than"]):
        return False
    return isinstance(toks, list) and len(toks) == 2
    #don't filter rn
"""def is_actively_tradable(m):
    if not m.get("enableOrderBook"): return False
    toks = m.get("clobTokenIds"); 
    if isinstance(toks, str):
        try: toks=json.loads(toks)
        except: toks=[]
    return isinstance(toks, list) and len(toks) == 2"""

"""def is_actively_tradable(m):
    if not m.get("enableOrderBook"): return False
    toks = m.get("clobTokenIds"); 
    if isinstance(toks, str):
        try: toks=json.loads(toks)
        except: toks=[]
    q = (m.get("question") or "").lower()
    # Skip crypto
    if is_crypto_market(m):
        return False
    return isinstance(toks, list) and len(toks) == 2"""

def is_crypto_market(market):
    CRYPTO_KEYWORDS = [
    "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
    "xrp", "ripple", "doge", "dogecoin", "avax", "avalanche",
    "matic", "polygon", "ada", "cardano", "ltc", "litecoin",
    "dot", "polkadot", "bch", "tron", "trx", "shib", "shiba",
    "ton", "toncoin", "link", "chainlink", "usdt", "tether",
    "usdc", "dai", "busd", "tusd", "frax", "aave", "uni",
    "op", "optimism", "arb", "arbitrum", "atom", "cosmos",
    "ape", "apecoin", "sand", "sandbox", "mana", "decentraland",
    "pepe", "wbtc", "eth2"
]
    """Return True if the question or slug mentions a crypto ticker."""
    text = (
        (market.get("question") or "") + " " +
        (market.get("market_slug") or "") + " " +
        (market.get("slug") or "")
    ).lower()
    return any(k in text for k in CRYPTO_KEYWORDS)

# --------------------------------------------------------------------
# Open_position “simulation” for live watcher
# --------------------------------------------------------------------
def simulate_take_from_asks(book: dict, dollars: float, fee: float, slip: float, price_cap: float):
    """
    Take NO asks up to 'dollars' while enforcing aggregate VWAP (inc fee+slip) <= price_cap.
    Allows partial fill at the boundary level.
    """
    asks = book.get("asks") or []
    if not asks or dollars <= 0:
        return 0.0, 0.0, 0.0

    # sort by ascending price
    try:
        asks = sorted(
            [
                {"price": float(a["price"]), "size": float(a["size"])}
                for a in asks
                if float(a.get("price", 0)) > 0 and float(a.get("size", 0)) > 0
            ],
            key=lambda a: a["price"],
        )
    except Exception:
        return 0.0, 0.0, 0.0

    fee_mult = 1.0 + float(fee) + float(slip)

    spent_ex = 0.0
    shares = 0.0
    budget_left = float(dollars)

    for lvl in asks:
        px, sz = lvl["price"], lvl["size"]
        lvl_dollars_cap = min(budget_left, px * sz)
        if lvl_dollars_cap <= 0:
            continue

        # Try full slice at this level
        new_spent_ex = spent_ex + lvl_dollars_cap
        new_shares   = shares + (lvl_dollars_cap / px)
        vwap_ex      = new_spent_ex / new_shares
        vwap_inc     = vwap_ex * fee_mult

        if vwap_inc <= price_cap:
            # take full slice
            spent_ex = new_spent_ex
            shares   = new_shares
            budget_left -= lvl_dollars_cap
            if budget_left <= 1e-9:
                break
            continue

        # Otherwise, solve for the *fraction* x we can still take at this px without breaking cap:
        # (spent_ex + x) / (shares + x/px) * fee_mult <= price_cap
        target_ex = price_cap / fee_mult
        denom = 1.0 - (target_ex / px)  # x*(1 - target/px) <= target*shares - spent_ex
        rhs   = target_ex * shares - spent_ex
        if denom <= 0:
            # This price is too high to add even a tiny amount and keep VWAP under the cap
            continue
        x_max = max(0.0, min(lvl_dollars_cap, rhs / denom))
        if x_max > 1e-8:
            spent_ex += x_max
            shares   += x_max / px
            budget_left -= x_max
            break  # boundary hit; further higher levels would only worsen VWAP

    if shares <= 0:
        return 0.0, 0.0, 0.0

    spent_inc = spent_ex * fee_mult
    avg_price_inc = spent_inc / shares
    return spent_inc, shares, avg_price_inc

# --------------------------------------------------------------------
# MAIN: run the watcher
# --------------------------------------------------------------------
positions_by_id = {}   # id -> {"shares": float, "side": "NO"/"YES"}
SETTLE_FEE = 0.01      # 1% winner fee
locked_now = 0.0
peak_locked = 0.0
peak_locked_time = None
first_trade_dt = None
last_settle_dt = None
REFRESH_SEED_EVERY = 300  # 5 minutes
now = datetime.now(timezone.utc) #now = datetime.now(timezone.utc), none if days back
last_seed = 0
def main():
    bank = 5_000_000.0
    desired_bet = 10.0 #100 for full
    total_trades_taken = 0
    global _created_cutoff
    _created_cutoff = datetime.now(timezone.utc) - timedelta(hours=2)

    # 1️⃣ Fetch initial markets
    open_markets = fetch_open_yesno_fast(limit=250, max_pages=4, days_back=0.25, verbose=True)
    recent = [m for m in open_markets if _dt(m.get("createdAt")) and _dt(m["createdAt"]) >= _created_cutoff]
    markets = [m for m in recent if is_actively_tradable(m)]

    print(f"Tradable Yes/No with quotes: {len(markets)}")

    # 2️⃣ Initialize the manager
    mgr = WatchlistManager(
        max_no_price=cap_for_raw(0.8, 600, 200),
        min_notional=50.0,
        fee_bps=600, slip_bps=200,
        dust_price=0.02, dust_min_notional=20.0,
        poll_every=3, backoff_first=3, backoff_base=6,
        backoff_max=60, jitter=3
    )
    mgr.seed_from_gamma(markets)

    #maker
    maker = VirtualMaker(mode="optimistic", queue_grace_s=VM_GRACE_SECONDS)

    # advance cutoff to the newest createdAt we saw (or “now” if none)
    seen_max = max((_dt(m.get("createdAt")) for m in recent if _dt(m.get("createdAt"))), default=datetime.now(timezone.utc))
    _created_cutoff = max(_created_cutoff, seen_max)

    probe_under_cap_sample(mgr, n=30)
    fee_mult = 1.0 + mgr.fee + mgr.slip
    print(f"[CAP] fee_mult={fee_mult:.4f} raw_cap=0.65 -> max_no_price={mgr.max_no_price:.4f} (=> p <= {mgr.max_no_price / fee_mult:.4f} pre-fee)")

    # --- trade sizing and fill simulation ---
    fee, slip, price_cap = mgr.fee, mgr.slip, mgr.max_no_price

    bank_ref = {"value": bank}

    def bet_size_fn(takeable_notional):
        return float(min(desired_bet, takeable_notional))

    def open_position_fn(cid, market, side, dollars, best_ask, book):
        # Enforce ALL-or-NOTHING $100:
        return open_position_fn_strict_full_ticket(
            cid, market, side, dollars, best_ask, book,
            maker=maker, mgr=mgr, bank_ref=bank_ref,
            fee=fee, slip=slip, price_cap=price_cap
        )

    REFRESH_SEED_EVERY = 90  # re-fetch markets every 3 minutes
    last_seed = 0

    try:
        while True:
            try:
                now_ts = int(time.time())

                # periodic reseed to catch new markets
                if now_ts - last_seed >= REFRESH_SEED_EVERY:
                    try:
                        fresh = fetch_open_yesno_fast(limit=250, max_pages=2, days_back=0.25, verbose=False)
                        new_only = [m for m in fresh if _dt(m.get("createdAt")) and _dt(m["createdAt"]) > _created_cutoff]
                        tradable = [m for m in new_only if is_actively_tradable(m)]
                        mgr.seed_from_gamma(tradable)

                        seen_max = max((_dt(m.get("createdAt")) for m in new_only), default=_created_cutoff)
                        _created_cutoff = max(_created_cutoff, seen_max)
                        vprint(f"[REFRESH] watch={len(mgr.watch)} entered={len(mgr.entered)}")
                        mgr.purge_stale(now_ts)   # default ~48h TTL
                        probe_under_cap_sample(mgr, n=30)
                    except Exception as e:
                        print(f"[WARN reseed] {e}")
                    last_seed = now_ts

                opened, checked = mgr.step(
                    now_ts,
                    fetch_book_fn=fetch_book,
                    open_position_fn=open_position_fn,
                    bet_size_fn=bet_size_fn,
                    maker=maker,                 # <-- add
                    bank_ref=bank_ref,           # <-- add
                    max_checks_per_tick=200,
                    min_probe_when_idle=100,
                    probe_strategy="newest",
                )

                bank = bank_ref["value"]
                log_run_snapshot(bank, total_trades_taken)

                # --- every ~5 minutes, log network usage stats
                if now_ts % 300 < mgr.poll_every:   # within a small window so it fires once
                    log_net_usage()
                if now_ts % 600 < mgr.poll_every:   # ~every 10 minutes
                    compress_and_prune_logs()

                if now_ts % 900 < mgr.poll_every:
                    p = psutil.Process(os.getpid())
                    print(f"[HEALTH] rss={p.memory_info().rss/1e6:.1f}MB "
                        f"watch={len(mgr.watch)}")

                # 🧹 optional housekeeping
                if now_ts % 600 < mgr.poll_every:
                    purge_housekeeping(mgr, None, _last_under_seen)

                if opened == 0 and checked == 0:
                    time.sleep(mgr.poll_every)
                else:
                    time.sleep(0.15)
            except Exception as loop_err:
                print(f"[MAIN-LOOP WARN] {loop_err}")
                time.sleep(2.0)  # brief cooloff

    except KeyboardInterrupt:
        print("\nStopped by user.")
        log_run_snapshot(bank, total_trades_taken)

# ----------------------------------------------------------------------
# log logic
# ----------------------------------------------------------------------
def _ensure_logdir():
    os.makedirs(LOG_DIR, exist_ok=True)

MAX_LOG_BYTES = 50 * 1024 * 1024   # 50 MB per part

def _dated_with_part(path_base: str) -> str:
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    name, ext = os.path.splitext(path_base)
    base = os.path.join(LOG_DIR, f"{name}_{day}{ext}")
    # If base exceeds MAX_LOG_BYTES, find next part suffix
    if os.path.exists(base) and os.path.getsize(base) >= MAX_LOG_BYTES:
        i = 1
        while True:
            candidate = os.path.join(LOG_DIR, f"{name}_{day}_part{i}{ext}")
            if not os.path.exists(candidate) or os.path.getsize(candidate) < MAX_LOG_BYTES:
                return candidate
            i += 1
    return base

def append_jsonl(path_base: str, record: dict):
    _ensure_logdir()
    path = _dated_with_part(path_base)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

def compute_potential_value_if_all_win():
    total = 0.0
    for pos in positions_by_id.values():
        total += float(pos["shares"]) * (1.0 - SETTLE_FEE)
    return total

def log_open_trade(market, side, token_id, book_used, spent_after, shares, avg_price_eff, bank_after_open):
    """
    Persist a single line for the trade you just took.
    """
    bb, ba = best_of_book(book_used or {})
    rec = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "market_id": market.get("conditionId"),
        "market_slug": market.get("slug"),
        "question": market.get("question"),
        "side": side,                        # "NO" or "YES"
        "token_id": token_id,
        "spent_after": round(float(spent_after), 6),
        "shares": round(float(shares), 6),
        "avg_price_eff": round(float(avg_price_eff), 6),  # effective avg (after fees/slip used to decide)
        "book_best_bid": bb,
        "book_best_ask": ba,
        "locked_now": round(float(locked_now), 6),
        "potential_value_if_all_win": round(compute_potential_value_if_all_win(), 6),
        "bank_after_open": round(float(bank_after_open), 6),
    }
    append_jsonl(TRADE_LOG_BASE, rec)

def log_run_snapshot(bank, total_trades_count: int):    
    global locked_now, peak_locked, peak_locked_time
    # Recompute locked from positions to avoid drift
    locked_now = compute_locked_now()
    if locked_now > peak_locked:
        peak_locked = locked_now
        peak_locked_time = datetime.now(timezone.utc)

    snap = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "bank": round(float(bank), 6),
        "locked_now": round(float(locked_now), 6),
        "open_positions": len(positions_by_id),
        "potential_value_if_all_win": round(compute_potential_value_if_all_win(), 6),
        "total_trades_taken": int(total_trades_count),
        "peak_locked": round(float(peak_locked), 6),
        "peak_locked_time": peak_locked_time.isoformat() if peak_locked_time else None,
        "first_trade_time": first_trade_dt.isoformat() if first_trade_dt else None,
        "last_settle_time": last_settle_dt.isoformat() if last_settle_dt else None,
    }
    if random.random() < DECISION_LOG_SNAPSHOT:
        append_jsonl(RUN_SNAP_BASE, snap)
    else:
        print(f"[SNAPSHOT] | {snap['ts']} | taken:{snap['total_trades_taken']} | spnet:{snap['peak_locked']}")

def debug_show_books_for_market(market):
    try:
        om = outcome_map_from_market(market)
        yes_token, no_token = om["YES"], om["NO"]
    except Exception:
        # best-effort legacy fallback
        toks = market.get("clobTokenIds")
        outs = market.get("outcomes")
        if isinstance(toks, str):
            try: toks = json.loads(toks)
            except: pass
        if isinstance(outs, str):
            try: outs = json.loads(outs)
            except: pass
        if isinstance(toks, list) and len(toks) == 2:
            yes_token, no_token = toks[0], toks[1]
        else:
            print("[DEBUG BOOKS] cannot resolve tokens")
            return

    yb = fetch_book(yes_token, depth=8)
    nb = fetch_book(no_token,  depth=8)

    def fmt_side(side, book):
        asks = book.get("asks") or []
        bids = book.get("bids") or []
        print(f"  {side} — ASKS (price x size):")
        for i,a in enumerate(asks):
            try:
                print(f"    [{i}] {float(a['price']):.4f} x {float(a['size']):.6f}")
            except Exception:
                print(f"    [{i}] {a}")
        print(f"  {side} — BIDS (price x size):")
        for i,b in enumerate(bids):
            try:
                print(f"    [{i}] {float(b['price']):.4f} x {float(b['size']):.6f}")
            except Exception:
                print(f"    [{i}] {b}")

    print(f"\n[DEBUG BOOKS] {market.get('question')}")
    print(f"  outcomes={market.get('outcomes')}")
    print(f"  YES token={yes_token}")
    fmt_side("YES", yb)
    print(f"  NO  token={no_token}")
    fmt_side("NO", nb)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def maybe_log_decision(*args, **kwargs):
    if random.random() < DECISION_LOG_SAMPLE:
        log_decision(*args, **kwargs)

def log_decision(market, price_cap, budget, book, takeable, best_ask, shares, reasons, result):
    """
    result can include: {'ok': bool, 'spent_after': float, 'avg_price': float, 'shares': float}
    """
    rec = {
        "ts": now_iso(),
        "type": "decision",
        "market_id": market.get("conditionId"),
        "question": market.get("question"),
        "price_cap_inc_fee": price_cap,
        "budget": budget,
        "best_ask": best_ask,
        "book_seq": book.get("sequence") if book else None,
        "under_cap_takeable_ex": takeable,
        "under_cap_shares": shares,
        "reasons": reasons,
        "result": result,
    }
    append_jsonl(DECISION_LOG_BASE, rec)

def _parse_day_from_filename(path):
    base = os.path.basename(path)
    # matches both: name_YYYY-MM-DD.jsonl  and  name_YYYY-MM-DD_partN.jsonl
    m = re.search(r"_(\d{4}-\d{2}-\d{2})(?:_part\d+)?\.jsonl$", base)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except Exception:
        return None

def compress_and_prune_logs(log_dir=LOG_DIR,
                            retain_days=RETAIN_DAYS,
                            compress_after_days=COMPRESS_AFTER_DAYS):
    """Gzip old logs and delete very old ones. Safe to run during writes because files are opened per-append."""
    today = datetime.now(timezone.utc).date()
    now_ts = time.time()

    # Compress *.jsonl older than compress_after_days (skip today's files)
    for path in glob.glob(os.path.join(log_dir, "*.jsonl")):
        d = _parse_day_from_filename(path)
        if not d: 
            continue
        age_days = (today - d).days
        gz_path = path + ".gz"
        if age_days >= compress_after_days and not os.path.exists(gz_path):
            try:
                # Double-check it's not the current day
                if d != today:
                    with open(path, "rb") as fin, gzip.open(gz_path, "wb") as fout:
                        fout.writelines(fin)
                    # Remove the original only after successful gzip write
                    os.remove(path)
                    print(f"[LOG] compressed {os.path.basename(path)}")
            except Exception as e:
                print(f"[LOG WARN] compress failed for {path}: {e}")

    # Delete *.jsonl.gz (and any stray *.jsonl) older than retain_days
    for path in glob.glob(os.path.join(log_dir, "*.jsonl*")):
        d = _parse_day_from_filename(path.replace(".gz", ""))
        if not d:
            continue
        age_days = (today - d).days
        if age_days > retain_days:
            try:
                os.remove(path)
                print(f"[LOG] deleted old log {os.path.basename(path)}")
            except Exception as e:
                print(f"[LOG WARN] delete failed for {path}: {e}")


def purge_housekeeping(mgr, maker, last_under_seen):
    # drop old keys from last_under_seen
    cutoff = time.time() - 24*3600
    for k, t0 in list(last_under_seen.items()):
        if t0 < cutoff:
            last_under_seen.pop(k, None)

    # trim maker.orders for markets already entered or no longer watched
    if maker is not None and hasattr(maker, "orders"):
        for cid in list(maker.orders.keys()):
            if cid in mgr.entered or cid not in mgr.watch:
                maker.orders.pop(cid, None)


def excepthook(exctype, value, tb):
    with open("crash.log","a",encoding="utf-8") as f:
        traceback.print_exception(exctype, value, tb, file=f)
sys.excepthook = excepthook

def run_forever():
    backoff = 5
    while True:
        try:
            main()
        except SystemExit:
            # graceful exit (SIGTERM etc.)
            raise
        except Exception:
            with open("crash.log", "a", encoding="utf-8") as f:
                f.write("="*60 + "\n")
                f.write(time.strftime("%Y-%m-%d %H:%M:%S UTC\n", time.gmtime()))
                traceback.print_exc(file=f)
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)  # cap at 5m
        else:
            break  # main returned normally

if __name__ == "__main__":
    run_forever()

"""
issue partially fills right away
"""