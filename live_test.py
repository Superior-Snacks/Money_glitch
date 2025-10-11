import json
import time
import requests
from datetime import datetime, timezone
import pandas as pd
import matplotlib.pyplot as plt
import time, json, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import bisect
import re
import heapq
import random, time
import os, json, time, math
from datetime import datetime, timedelta, timezone

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_BOOK = "https://clob.polymarket.com/book"
DATA_TRADES = "https://data-api.polymarket.com/trades"
BASE_BOOK = "https://clob.polymarket.com/book"

# --------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------
BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_BOOK = "https://clob.polymarket.com/book"
DATA_TRADES = "https://data-api.polymarket.com/trades"

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

# --------------------------------------------------------------------
# Book fetch
# --------------------------------------------------------------------
def fetch_book(token_id: str, depth: int = 20, session=SESSION):
    """Fetch the CLOB book for one token (YES or NO)."""
    r = session.get(BASE_BOOK, params={"token_id": token_id}, timeout=15)
    r.raise_for_status()
    book = r.json() or {}
    # normalize & trim
    book["bids"] = (book.get("bids") or [])[:depth]
    book["asks"] = (book.get("asks") or [])[:depth]
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

# --------------------------------------------------------------------
# Watchlist Manager
# --------------------------------------------------------------------
VERBOSE = True
def vprint(*a, **k):
    if VERBOSE:
        print(*a, **k)

class WatchlistManager:
    def __init__(self,
                 max_no_price=0.40,
                 min_notional=50.0,
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
                "next_check": now + random.randint(0, 2),  # stagger a bit
                "fails": 0,
                "last_quote": None,
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
             max_checks_per_tick=100,
             min_probe_when_idle=None,
             probe_strategy="newest"):
        """
        Returns (opened_count, checked_count).
        - If too few are due, we proactively probe a few more (min_probe_when_idle)
          so you can chew through a 3-day backlog quickly.
        """
        due = self.due_ids(now_ts)
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
                takeable, best_ask, shares, reasons = self._valid_no_from_book(book)
                vprint(f"    best_ask={best_ask} | takeable=${takeable:.2f} | shares_at_cap={shares:.2f}")

                if best_ask is None or "no_asks_under_cap" in reasons:
                    debug_show_books_for_market(m)

                log_decision(
                    m, self.max_no_price, bet_size_fn(takeable), book,
                    takeable, best_ask, shares, reasons,
                    result=None
                )

                if takeable > 0:
                    dollars = bet_size_fn(takeable)
                    vprint(f"    TRY OPEN NO for ${dollars:.2f} (<= takeable)")
                    ok = open_position_fn(cid, m, "NO", dollars, best_ask, book)
                    if ok:
                        log_decision(
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
                        log_decision(
                            m, self.max_no_price, dollars, book,
                            takeable, best_ask, shares, reasons,
                            result={"ok": False, "why": "open_position_fn_false"}
                        )
                        # liquidity existed but we didn't fill (book moved, race, etc.) → QUICK RETRY
                        st["fails"] = 0
                        st["next_check"] = now_ts + max(1, self.backoff_first // 2)
                        vprint("    ⚠️ had_liquidity_but_no_fill → quick retry scheduled")
                        continue

                # No takeable liquidity right now → backoff, but gently if we were close
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
    
def cap_for_raw(raw, fee_bps, slip_bps):
    return raw * (1 + fee_bps/10000 + slip_bps/10000)

def compute_locked_now():
    # Total cost basis still tied up in open positions
    return sum(float(p.get("cost", 0.0)) for p in positions_by_id.values())

# --------------------------------------------------------------------
# Fast open-market fetch you already had (kept here for completeness)
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

def is_actively_tradable(m):
    if not m.get("enableOrderBook"):
        return False
    toks = m.get("clobTokenIds") or []
    if isinstance(toks, str):
        try: toks = json.loads(toks)
        except: toks = []
    has_quote = (m.get("bestBid") is not None) or (m.get("bestAsk") is not None)
    return bool(toks) and has_quote

# --------------------------------------------------------------------
# Minimal open_position “simulation” for live watcher (replace with yours)
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

# ---- simple open-position ledger (for logging potential value) ----
positions_by_id = {}   # id -> {"shares": float, "side": "NO"/"YES"}
SETTLE_FEE = 0.01      # 1% winner fee
locked_now = 0.0
peak_locked = 0.0
peak_locked_time = None
first_trade_dt = None
last_settle_dt = None
REFRESH_SEED_EVERY = 300  # 5 minutes
last_seed = 0
def main():
    bank = 5_000_000.0
    desired_bet = 100.0
    total_trades_taken = 0

    # 1️⃣ Fetch initial 3 days of markets
    open_markets = fetch_open_yesno_fast(limit=250, max_pages=10, days_back=10, verbose=True)
    markets = [m for m in open_markets if is_actively_tradable(m)]
    print(f"Tradable Yes/No with quotes: {len(markets)}")

    # 2️⃣ Initialize the manager
    mgr = WatchlistManager(
        max_no_price=cap_for_raw(0.60, 600, 200),
        min_notional=50.0,
        fee_bps=600, slip_bps=200,
        dust_price=0.02, dust_min_notional=20.0,
        poll_every=3, backoff_first=6, backoff_base=12,
        backoff_max=120, jitter=3
    )
    mgr.seed_from_gamma(markets)

    # --- trade sizing and fill simulation ---
    fee, slip, price_cap = mgr.fee, mgr.slip, mgr.max_no_price

    def bet_size_fn(takeable_notional):
        return float(min(desired_bet, takeable_notional))

    def open_position_fn(cid, market, side, dollars, best_ask, book):
        nonlocal bank, total_trades_taken
        # Declare globals BEFORE any use or assignment
        global first_trade_dt, locked_now, peak_locked, peak_locked_time

        vprint(f"    open_position_fn: side={side} dollars={dollars:.2f} bank={bank:.2f} best_ask={best_ask}")
        if side != "NO" or dollars <= 0 or bank < dollars:
            vprint("    open_position_fn: rejected (side/budget)")
            return False

        # Resolve NO token early so we can log with it
        toks = market.get("clobTokenIds")
        if isinstance(toks, str):
            try:
                toks = json.loads(toks)
            except Exception:
                toks = []
        no_token_id = (toks or [None, None])[1]

        # Simulate taking from asks
        spent_after, shares, avg_price = simulate_take_from_asks(
            book, dollars, fee=fee, slip=slip, price_cap=price_cap
        )
        vprint(f"    simulate_take_from_asks → shares={shares:.4f} spent_after={spent_after:.2f} avg_price={avg_price:.4f}")

        if shares <= 0:
            vprint("    open_position_fn: no shares (book moved?)")
            return False

        # --- apply bookkeeping (update bank and locked BEFORE logging)
        bank -= spent_after

        # store position with cost so 'locked' = sum(costs)
        positions_by_id[cid] = {"shares": shares, "side": side, "cost": spent_after}

        # first trade timestamp
        if first_trade_dt is None:
            first_trade_dt = datetime.now(timezone.utc)

        # recompute locked and update peak
        locked_now = compute_locked_now()
        if locked_now > peak_locked:
            peak_locked = locked_now
            peak_locked_time = datetime.now(timezone.utc)

        print(f"\n✅ ENTER NO | {market.get('question')}")
        print(f"   spent(after)={spent_after:.2f} | shares={shares:.2f} | avg_price={avg_price:.4f} | bank={bank:.2f} | locked_now={locked_now:.2f}")

        # trade log (after updates so numbers are correct)
        log_open_trade(market, "NO", no_token_id, book_used=book,
                    spent_after=spent_after, shares=shares,
                    avg_price_eff=avg_price, bank_after_open=bank)

        # decision/analytics log for the fill
        append_jsonl(DECISION_LOG_BASE, {
            "ts": datetime.now(timezone.utc).isoformat(),
            "type": "fill_sim",
            "market_id": market.get("conditionId"),
            "market_slug": market.get("slug"),
            "question": market.get("question"),
            "side": side,
            "token_id": no_token_id,
            "spent_after": round(float(spent_after), 6),
            "shares": round(float(shares), 6),
            "avg_price_eff": round(float(avg_price), 6),
            "budget": round(float(dollars), 6),
            "cap_inc_fee": round(float(price_cap), 6),
            "bank_after_open": round(float(bank), 6),
            "locked_now": round(float(locked_now), 6),
            "potential_value_if_all_win": round(compute_potential_value_if_all_win(), 6),
        })

        total_trades_taken += 1
        return True

    REFRESH_SEED_EVERY = 180  # re-fetch markets every 3 minutes
    last_seed = 0

    try:
        while True:
            now_ts = int(time.time())

            # periodic reseed to catch new markets
            if now_ts - last_seed >= REFRESH_SEED_EVERY:
                try:
                    fresh = fetch_open_yesno_fast(limit=250, max_pages=3, days_back=3, verbose=True)
                    tradable = [m for m in fresh if is_actively_tradable(m)]
                    mgr.seed_from_gamma(tradable)
                    vprint(f"[REFRESH] watch={len(mgr.watch)} entered={len(mgr.entered)}")
                except Exception as e:
                    print(f"[WARN reseed] {e}")
                last_seed = now_ts

            opened, checked = mgr.step(
                now_ts,
                fetch_book_fn=fetch_book,
                open_position_fn=open_position_fn,
                bet_size_fn=bet_size_fn,
                max_checks_per_tick=200,     # go harder
                min_probe_when_idle=100,     # and chew backlog
                probe_strategy="newest",     # or "oldest"
            )

            log_run_snapshot(bank, total_trades_taken)

            if opened == 0 and checked == 0:
                time.sleep(mgr.poll_every)   # idle
            else:
                time.sleep(0.15)             # tiny breather

    except KeyboardInterrupt:
        print("\nStopped by user.")
        log_run_snapshot(bank, total_trades_taken)

LOG_DIR = "logs"

def _ensure_logdir():
    os.makedirs(LOG_DIR, exist_ok=True)

def _dated(path_base: str) -> str:
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    name, ext = os.path.splitext(path_base)
    return os.path.join(LOG_DIR, f"{name}_{day}{ext}")

def append_jsonl(path_base: str, record: dict):
    _ensure_logdir()
    path = _dated(path_base)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

TRADE_LOG_BASE = "trades_taken.jsonl"
RUN_SNAP_BASE  = "run_snapshots.jsonl"

def compute_potential_value_if_all_win():
    total = 0.0
    for pos in positions_by_id.values():
        total += float(pos["shares"]) * (1.0 - SETTLE_FEE)
    return total

def log_open_trade(market, side, token_id, book_used, spent_after, shares, avg_price_eff, bank_after_open):
    """
    Persist a single line for the trade you just took.
    """
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
        "book_best_bid": (book_used.get("bids") or [{}])[0].get("price") if book_used else None,
        "book_best_ask": (book_used.get("asks") or [{}])[0].get("price") if book_used else None,
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
    append_jsonl(RUN_SNAP_BASE, snap)

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

DECISION_LOG_BASE = "decisions.jsonl"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

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
if __name__ == "__main__":
    main()

"""


#last 13

def fetch_open_yesno_fast(limit=250, max_pages=20, days_back=90,
                          require_clob=True, min_liquidity=None, min_volume=None,
                          session=SESSION, verbose=True):
    Quickly fetch currently tradable Yes/No markets without walking full history.
    params = {
        "limit": limit,
        "order": "startDate",           # stable ordering so offset works
        "ascending": False,             # newest first
        "closed": False,                # only open
    }
    # time window (reduce dataset)
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

        # filter to exactly Yes/No
        added = 0
        for m in page:
            outs = m.get("outcomes")
            if isinstance(outs, str):
                try: outs = json.loads(outs)
                except Exception: outs = None
            if outs == ["Yes","No"]:
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

    # sort locally for your pipeline
    all_rows.sort(key=lambda m: m.get("startDate") or m.get("createdAt") or "")
    if verbose:
        print(f"✅ Total open Yes/No markets: {len(all_rows)}")
    return all_rows

def is_actively_tradable(m):
    # CLOB-enabled, has token IDs, and has a visible quote
    if not m.get("enableOrderBook"):
        return False
    toks = m.get("clobTokenIds") or []
    if isinstance(toks, str):
        try: toks = json.loads(toks)
        except: toks = []
    has_quote = (m.get("bestBid") is not None) or (m.get("bestAsk") is not None)
    return bool(toks) and has_quote
def new_markets():
    old_markets = load_markets_from_jsonl("old_live_markets.jsonl")
    old_id = []
    for id in old_markets:
        old_id.append(id["id"])
    open_markets = fetch_open_yesno_fast()
    markets = [m for m in open_markets if is_actively_tradable(m)]
    new_markets = [n for n in markets if n["id"] not in old_id]
    return new_markets

def fetch_old(filename):
    result = []
    # ensure parent folder exists
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
    with open(filename, "r", encoding="utf-8") as file:
        # if file already has content, add newline first
        for line in file:
            result.append(line["id"])
    return result

def fech_file(filename):
    result = []
    # ensure parent folder exists
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
    with open(filename, "r", encoding="utf-8") as file:
        # if file already has content, add newline first
        for line in file:
            result.append(line)
    return result

def append_markets_to_file(filename, data, key_field="id"):
    Append markets to a .jsonl file, skipping duplicates based on `key_field`.
    Each line is one JSON object.
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)

    # --- load existing keys to skip duplicates ---
    existing_keys = set()
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    if key_field in record:
                        existing_keys.add(record[key_field])
                except json.JSONDecodeError:
                    continue

    # --- append only new items ---
    added = 0
    with open(filename, "a", encoding="utf-8") as f:
        for market in data:
            key = market.get(key_field)
            if not key or key in existing_keys:
                continue  # skip duplicates or invalid
            f.write(json.dumps(market, ensure_ascii=False) + "\n")
            existing_keys.add(key)
            added += 1

    print(f"✅ Added {added} new markets (skipped {len(data)-added} duplicates) to {filename}")

def load_markets_from_jsonl(filename):
    markets = []
    if not os.path.exists(filename):
        return markets
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                markets.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return markets

def save_line_to_file(filename, data):
    # ensure parent folder exists
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)

    with open(filename, "a", encoding="utf-8") as file:
        # if file already has content, add newline first
        file.seek(0, os.SEEK_END)
        if file.tell() > 0:
            file.write("\n")
        file.write(data + "\n")

def normalize_time(value, default=None):
    Converts various Polymarket-style date/time formats into a UTC datetime.

    Accepts:
      - ISO strings with or without 'Z'
      - 'YYYY-MM-DD' (no time)
      - timestamps (int, float, or numeric strings)
      - None or invalid → returns `default` (or None)

    Returns:
      datetime object (UTC timezone)
    if value is None or value == "":
        return default

    # numeric timestamp (epoch seconds)
    if isinstance(value, (int, float)) or re.match(r"^\d{10,13}$", str(value)):
        try:
            ts = float(value)
            if ts > 1e12:  # milliseconds
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return default

    # string normalization
    val = str(value).strip()

    # Replace common ISO variants
    val = val.replace("Z", "+00:00")  # Z → UTC
    val = re.sub(r"\s+", "T", val)    # space → T

    # Add missing time or timezone if needed
    if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
        val += "T00:00:00+00:00"

    try:
        dt = datetime.fromisoformat(val)
        # ensure UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return default


def interpret_book(market):
    toks = market.get("clobTokenIds")
    if isinstance(toks, str):
        toks = json.loads(toks)

    token = toks[0]  # first token (usually YES)
    book = fetch_book(token)

    best_bid = book["bids"][0]["price"] if book["bids"] else None
    best_ask = book["asks"][0]["price"] if book["asks"] else None

    print(f"\n📊 {market['question']}")
    print(f"Best bid: {best_bid}")
    print(f"Best ask: {best_ask}")
    print(f"Spread:   {round((float(best_ask) - float(best_bid)), 3) if best_bid and best_ask else 'N/A'}")
    print(f"Depth:    {len(book['bids'])} bids / {len(book['asks'])} asks")


def fetch_book(token_id, depth=10):
    url = f"{BASE_BOOK}?token_id={token_id}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    book = r.json()
    
    # optional: trim to depth
    book["bids"] = book.get("bids", [])[:depth]
    book["asks"] = book.get("asks", [])[:depth]
    print(book)
    return book


    def open_position_fn(cid, market, side, dollars, best_ask, book):
        nonlocal bank, total_trades_taken

        vprint(f"    open_position_fn: side={side} dollars={dollars:.2f} bank={bank:.2f} best_ask={best_ask}")
        if side != "NO" or dollars <= 0 or bank < dollars:
            vprint("    open_position_fn: rejected (side/budget)")
            return False

        spent_after, shares, avg_price = simulate_take_from_asks(
            book, dollars, fee=fee, slip=slip, price_cap=price_cap
        )
        append_jsonl(DECISION_LOG_BASE, {
            "ts": datetime.now(timezone.utc).isoformat(),
            "type": "fill_sim",
            "market_id": market.get("conditionId"),
            "question": market.get("question"),
            "spent_after": round(float(spent_after), 6),
            "shares": round(float(shares), 6),
            "avg_price_eff": round(float(avg_price), 6),
            "budget": round(float(dollars), 6),
            "cap_inc_fee": round(float(price_cap), 6),
        })
        vprint(f"    simulate_take_from_asks → shares={shares:.4f} spent_after={spent_after:.2f} avg_price={avg_price:.4f}")

        if shares <= 0:
            vprint("    open_position_fn: no shares (book changed?)")
            return False

        bank -= spent_after
        positions_by_id[cid] = {"shares": shares, "side": side}

        print(f"\n✅ ENTER NO | {market.get('question')}")
        print(f"   spent(after)={spent_after:.2f} | shares={shares:.2f} | avg_price={avg_price:.4f} | bank={bank:.2f}")

        toks = market.get("clobTokenIds")
        if isinstance(toks, str):
            try: toks = json.loads(toks)
            except: toks = []
        no_token_id = (toks or [None, None])[1]

        log_open_trade(market, "NO", no_token_id, book_used=book,
                    spent_after=spent_after, shares=shares,
                    avg_price_eff=avg_price, bank_after_open=bank)

        total_trades_taken += 1
        return True


"""


"""
just start slow on new markets
trding current open not neccisarry, but not bad to check
"""

"""
fyrst sækja alla active markaði, síðan fara yfir book hjáhverjum og einum og sim hvort/hversu mikið er keypt,
savea kaupið og potential winnings og allt info i kringum það í file, síðan á 10 min fresti kanski 1 min sækja alla nýja markaði og ef það er liquidity kaupa,
annars bíða þar til það er liquidity, hafa sér pending file, svo 1 klst fresti checka hvort markaðir hafa klárast.
ef það eru mikið af markets þá byrja þannig ég hef efni á continuous trades á nýjum mörkuðum, (eldri markaðir ekki lokaðir eru oftast "settled")
hafa sterk guards á eldri mörkuðum þannig tap er ekki of hátt.
3 files, bought, pending, log(bottom shows p/l, active, done, bank, lockedup, etmifallsold)
"""

