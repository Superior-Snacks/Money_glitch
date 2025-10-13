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

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_BOOK = "https://clob.polymarket.com/book"
DATA_TRADES = "https://data-api.polymarket.com/trades"
LOG_DIR = f"logs_run_{int(time.time())}"
TRADE_LOG_BASE = "trades_taken.jsonl"
RUN_SNAP_BASE  = "run_snapshots.jsonl"
DECISION_LOG_BASE = "decisions.jsonl"
DECISION_LOG_SAMPLE = 0.15  # 15% sampling
VERBOSE = True
SHOW_DEBUG_BOOKS = False  # Set True to fetch YES/NO books on skipped markets for inspection
RETAIN_DAYS = 7           # delete logs older than this
COMPRESS_AFTER_DAYS = 1   # gzip logs older than this (but not today's)


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

# --------------------------------------------------------------------
# Watchlist Manager
# --------------------------------------------------------------------
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
                "next_check": now + random.randint(0, 2),
                "fails": 0,
                "last_quote": None,
                "last_seen_ts": now,          # <-- add
                "last_seq": None,             # <-- for book-change detection (used below)
                "ever_under_cap": False,
                "seeded_ts": now,      # <-- optional heuristic (explained below)
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
                seq = book.get("sequence") or book.get("version")
                prev_seq = st.get("last_seq")
                if seq and seq != prev_seq:
                    st["last_seq"] = seq
                    # reset backoff to probe sooner since there was fresh activity
                    st["fails"] = 0
                    st["next_check"] = now_ts + max(1, self.backoff_first // 2)

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

"""def is_actively_tradable(m):
    if not m.get("enableOrderBook"): return False
    toks = m.get("clobTokenIds"); 
    if isinstance(toks, str):
        try: toks=json.loads(toks)
        except: toks=[]
    q = (m.get("question") or "").lower()
    # Skip range/between/greater-than style if you want simpler binarys:
    if any(w in q for w in ["between", "range", "greater than", "less than"]):
        return False
    return isinstance(toks, list) and len(toks) == 2"""
    #don't filter rn
def is_actively_tradable(m):
    if not m.get("enableOrderBook"): return False
    toks = m.get("clobTokenIds"); 
    if isinstance(toks, str):
        try: toks=json.loads(toks)
        except: toks=[]
    return isinstance(toks, list) and len(toks) == 2

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
last_seed = 0
def main():
    bank = 5_000_000.0
    desired_bet = 100.0
    total_trades_taken = 0

    # 1️⃣ Fetch initial markets
    open_markets = fetch_open_yesno_fast(limit=250, max_pages=10, days_back=10, verbose=True)
    markets = [m for m in open_markets if is_actively_tradable(m)]
    print(f"Tradable Yes/No with quotes: {len(markets)}")

    # 2️⃣ Initialize the manager
    mgr = WatchlistManager(
        max_no_price=cap_for_raw(0.70, 600, 200),
        min_notional=50.0,
        fee_bps=600, slip_bps=200,
        dust_price=0.02, dust_min_notional=20.0,
        poll_every=3, backoff_first=3, backoff_base=6,
        backoff_max=60, jitter=3
    )
    mgr.seed_from_gamma(markets)
    probe_under_cap_sample(mgr, n=30)
    fee_mult = 1.0 + mgr.fee + mgr.slip
    print(f"[CAP] fee_mult={fee_mult:.4f} raw_cap=0.65 -> max_no_price={mgr.max_no_price:.4f} (=> p <= {mgr.max_no_price / fee_mult:.4f} pre-fee)")

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
        no_token_id = get_no_token_id(market)


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

    REFRESH_SEED_EVERY = 90  # re-fetch markets every 3 minutes
    last_seed = 0

    try:
        while True:
            try:
                now_ts = int(time.time())

                # periodic reseed to catch new markets
                if now_ts - last_seed >= REFRESH_SEED_EVERY:
                    try:
                        fresh = fetch_open_yesno_fast(limit=250, max_pages=3, days_back=2, verbose=True)
                        tradable = [m for m in fresh if is_actively_tradable(m)]
                        mgr.seed_from_gamma(tradable)
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
                    max_checks_per_tick=200,     # was 200
                    min_probe_when_idle=100,      # was 100
                    probe_strategy="newest",
                )

                log_run_snapshot(bank, total_trades_taken)

                # --- every ~5 minutes, log network usage stats
                if now_ts % 300 < mgr.poll_every:   # within a small window so it fires once
                    log_net_usage()
                if now_ts % 600 < mgr.poll_every:   # ~every 10 minutes
                    compress_and_prune_logs()

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

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def maybe_log_decision(*args, **kwargs):
    import random
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

def compress_and_prune_logs(log_dir="logs",
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

if __name__ == "__main__":
    main()
