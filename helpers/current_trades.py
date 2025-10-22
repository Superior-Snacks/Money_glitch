#!/usr/bin/env python3
"""
Trades-Only Market Scanner (slow + single-file state)

What it does
------------
- Prompts for a start time (ISO like '2025-10-01T00:00:00Z' OR relative like 'hours=6', 'days=2').
- Fetches all open Yes/No Polymarket markets.
- For each market, SLOWLY pages trade history back to the start time.
- Computes per-market stats from *trades only*:
    * min_trade_px_raw (lowest observed raw price; NO-only if available, else all prints)
    * min_trade_px_inc (with fee/slip multiplier)
    * per budget: the *tightest price interval (spread)* whose dollar liquidity >= budget
      + VWAP for that interval (raw & inc-fee)
- Maintains a single JSON file on disk (`trades_only_state.json`) with a dict:
    { market_id: { question, slug, updated_at, min_trade_px_raw, min_trade_px_inc, budgets{...}, under_cap: bool } }
- After completing a full pass over markets, prints a report and sleeps before next pass.

Notes
-----
- “Under cap” is evaluated as (min_trade_px_inc <= CAP_INC_FEE).
- This is a lower-bound view of achievable prices (from actual prints), not current book liquidity.
- API paging:
    * Primary: `starting_before` (older-than timestamp in seconds) if supported.
    * Fallback: `offset` paging.
- Tuned for low RPS. Increase delays if you want to be gentler.
"""

import json, time, os, math, random, re, gzip, glob, sys
from datetime import datetime, timezone, timedelta
from collections import deque
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------- Config -----------------
BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"

name_log = input("name log: ")
if name_log:
    STATE_PATH = os.path.join("logs", name_log)
else:
    STATE_PATH = os.path.join("logs", f"trades_only_state.json")

# Fee/slip as FRACTIONS (not bps)
FEE  = 0.00
SLIP = 0.00
FEE_MULT = 1.0 + FEE + SLIP

# Inc-fee CAP used to mark markets "under_cap"
CAP_INC_FEE = 0.40

# Budgets to evaluate (USD notional, using raw trade prices in the window)
TARGET_BUDGETS = [5, 10, 50, 100, 200]

# Pull cadence
MARKET_REFRESH_SEC   = 15 * 60    # refresh market list every 15 min
SLEEP_BETWEEN_PAGES  = 2.0        # seconds between trade pages (be gentle)
SLEEP_BETWEEN_MARKETS= 1.0        # seconds between markets
SLEEP_AFTER_FULL_PASS= 120.0      # rest between passes

# HTTP + RPS limiter
RPS_TARGET = 4.0  # nice and slow

# ----------------- HTTP session -----------------
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429,500,502,503,504),
        allowed_methods=("GET","POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=40, pool_maxsize=40)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pm-trades-only-slow/1.0"})
    return s

SESSION = make_session()
_last_tokens_ts = time.monotonic()
_bucket = RPS_TARGET
def _rate_limit():
    global _last_tokens_ts, _bucket
    now = time.monotonic()
    _bucket = min(RPS_TARGET, _bucket + (now - _last_tokens_ts) * RPS_TARGET)
    _last_tokens_ts = now
    if _bucket < 1.0:
        need = (1.0 - _bucket) / RPS_TARGET
        time.sleep(need)
        now2 = time.monotonic()
        _bucket = min(RPS_TARGET, _bucket + (now2 - _last_tokens_ts) * RPS_TARGET)
        _last_tokens_ts = now2
    _bucket -= 1.0

# ----------------- Time parsing -----------------
def parse_start_input(user_input: str) -> int:
    """
    Accepts:
      - ISO 8601 e.g. '2025-10-01T00:00:00Z' or '2025-10-01 00:00:00'
      - Relative like 'hours=6', 'days=2', 'minutes=30'
    Returns epoch seconds UTC.
    """
    s = user_input.strip()
    # relative?
    if "=" in s and all(k in s for k in ["="]):
        parts = dict(
            (k.strip(), float(v))
            for k,v in (p.split("=",1) for p in s.split(","))
        )
        delta = timedelta(
            days=parts.get("days", 0.0),
            hours=parts.get("hours", 0.0),
            minutes=parts.get("minutes", 0.0)
        )
        t = datetime.now(timezone.utc) - delta
        return int(t.timestamp())

    # ISO variants
    s2 = s.replace("Z","+00:00")
    try:
        t = datetime.fromisoformat(s2).astimezone(timezone.utc)
        return int(t.timestamp())
    except Exception:
        print("Could not parse time. Try ISO like 2025-10-01T00:00:00Z or relative like hours=6")
        sys.exit(1)

def dt_iso():
    return datetime.now(timezone.utc).isoformat()

# ----------------- Market + Trades fetch -----------------
def fetch_open_yesno_fast(limit=250, max_pages=4):
    params = {
        "limit": limit, "order": "startDate", "ascending": False,
        "closed": False, "enableOrderBook": True
    }
    seen, out = set(), []
    offset = 0
    for _ in range(max_pages):
        q = dict(params, offset=offset)
        _rate_limit()
        r = SESSION.get(BASE_GAMMA, params=q, timeout=20); r.raise_for_status()
        data = r.json() or []
        if not data: break
        for m in data:
            outs = m.get("outcomes")
            if isinstance(outs, str):
                try: outs = json.loads(outs)
                except: outs = None
            if isinstance(outs, list) and len(outs) == 2:
                names = {str(x).strip().lower() for x in outs}
                if names == {"yes","no"}:
                    mid = m.get("conditionId") or m.get("id")
                    if mid and mid not in seen:
                        out.append(m); seen.add(mid)
        if len(data) < limit: break
        offset += limit
    return out

def fetch_trades_page(cid: str, limit=250, starting_before=None, offset=None):
    """
    Try timestamp-paging first (preferred), fallback to offset paging.
    - starting_before: epoch seconds; returns trades strictly older than this timestamp.
    - offset: integer offset for pagination, if timestamp paging not available/working.
    """
    _rate_limit()
    params = {"market": cid, "sort": "desc", "limit": limit}
    if starting_before is not None:
        params["starting_before"] = int(starting_before)
    if offset is not None:
        params["offset"] = int(offset)
    r = SESSION.get(DATA_TRADES, params=params, timeout=20)
    r.raise_for_status()
    return r.json() or []

def fetch_all_trades_since(cid: str, since_epoch: int, page_limit=250, slow_delay=SLEEP_BETWEEN_PAGES):
    """
    Slowly page backward in time to since_epoch.
    Use starting_before when possible; if we see no progress, try offset paging.
    Returns list of trade dicts.
    """
    out = []
    starting_before = None
    last_len = None
    # Timestamp-paging attempt
    for _ in range(10_000):  # practically unbounded; rely on time cutoff
        data = fetch_trades_page(cid, limit=page_limit, starting_before=starting_before)
        if not data:
            break
        out.extend(data)
        oldest_ts = min(_to_epoch(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in data)
        if oldest_ts <= since_epoch:
            break
        # prepare next page
        starting_before = oldest_ts
        if slow_delay: time.sleep(slow_delay)

        # detect no progress
        if last_len == len(data):
            break
        last_len = len(data)

    # If we didn’t reach since_epoch and got very few pages, try offset paging as fallback
    if (not out) or (min((_to_epoch(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0) for t in out) > since_epoch):
        offset = 0
        for _ in range(2000):
            data = fetch_trades_page(cid, limit=page_limit, offset=offset)
            if not data:
                break
            out.extend(data)
            oldest_ts = min(_to_epoch(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in data)
            if oldest_ts <= since_epoch:
                break
            offset += len(data)
            if slow_delay: time.sleep(slow_delay)

    # Deduplicate by (id or (ts, price, size, side, outcome))
    seen = set()
    uniq = []
    for t in out:
        key = t.get("id") or (t.get("timestamp"), t.get("price"), t.get("size"), t.get("side"), t.get("outcome"))
        if key in seen: continue
        seen.add(key); uniq.append(t)
    return uniq

# ----------------- Analytics -----------------
def _to_epoch(x):
    try:
        f = float(x)
        return int(f/1000) if f > 1e12 else int(f)
    except Exception:
        return None

def tighten_intervals_min_spread_for_budget(trades, budget_usd):
    """
    trades: list of (price, size) RAW (no fee)
    Return (spread, lo, hi, dollars, shares, vwap_raw) for the *narrowest* price interval
    whose Σ(price*size) >= budget_usd. Two-pointer on price-sorted trades.
    """
    pts = sorted(((float(p), float(s)) for p,s in trades if p>0 and s>0), key=lambda x: x[0])
    if not pts:
        return None
    n = len(pts)
    best = None
    j = 0
    dollars = 0.0
    shares  = 0.0
    for i in range(n):
        while j < n and dollars < budget_usd:
            p,s = pts[j]; dollars += p*s; shares += s; j += 1
        if dollars >= budget_usd:
            lo, _ = pts[i]; hi, _ = pts[j-1]
            spread = hi - lo
            if (best is None) or (spread < best[0]):
                best = (spread, i, j-1, dollars, shares)
        # remove i from window
        p,s = pts[i]; dollars -= p*s; shares -= s
    if not best:
        return None
    spread, i0, j0, dollars, shares = best
    lo = pts[i0][0]; hi = pts[j0][0]
    vwap_raw = (sum(p*s for p,s in pts[i0:j0+1]) /
                max(1e-12, sum(s for _,s in pts[i0:j0+1])))
    return (spread, lo, hi, dollars, shares, vwap_raw)

def compute_stats_from_trades(trades):
    """
    trades: raw trade dicts. Prefer NO-only; fallback to all.
    Returns dict with min prices and per-budget interval metrics.
    """
    tuples_no = [(float(t.get("price",0)), float(t.get("size",0)))
                 for t in trades
                 if str(t.get("outcome","")).lower()=="no" and float(t.get("price",0))>0 and float(t.get("size",0))>0]
    tuples_all = [(float(t.get("price",0)), float(t.get("size",0)))
                  for t in trades
                  if float(t.get("price",0))>0 and float(t.get("size",0))>0]
    tuples = tuples_no if tuples_no else tuples_all
    if not tuples:
        return None

    min_px = min(p for p,_ in tuples)
    res = {
        "min_trade_px_raw": round(min_px, 6),
        "min_trade_px_inc": round(min_px * FEE_MULT, 6),
        "budgets": {}
    }
    for B in TARGET_BUDGETS:
        win = tighten_intervals_min_spread_for_budget(tuples, B)
        if win is None:
            res["budgets"][str(B)] = None
        else:
            spread, lo, hi, dollars, shares, vwap_raw = win
            res["budgets"][str(B)] = {
                "min_spread_raw": round(spread, 6),
                "lo": round(lo, 6),
                "hi": round(hi, 6),
                "vwap_raw": round(vwap_raw, 6),
                "vwap_inc": round(vwap_raw * FEE_MULT, 6),
                "covered_dollars": round(dollars, 2),
                "covered_shares": round(shares, 6),
            }
    return res

# ----------------- State I/O -----------------
def load_state(path=STATE_PATH):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return {}

def save_state(state, path=STATE_PATH):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)

# ----------------- Main loop -----------------
def main():
    # ask when to start
    user = input("Start time (ISO like 2025-10-01T00:00:00Z or relative like hours=6): ").strip()
    since_epoch = parse_start_input(user)
    print(f"→ scanning trades since {datetime.fromtimestamp(since_epoch, tz=timezone.utc).isoformat()}")

    state = load_state()
    markets = []
    last_market_pull = 0

    while True:
        now_s = int(time.time())
        # refresh markets list periodically
        if (now_s - last_market_pull >= MARKET_REFRESH_SEC) or not markets:
            try:
                markets = fetch_open_yesno_fast()
                last_market_pull = now_s
                print(f"[MKT] loaded {len(markets)} markets @ {dt_iso()}")
            except Exception as e:
                print(f"[WARN markets] {e}; keeping previous list")
                time.sleep(5)

        total = 0
        under = 0
        over  = 0

        # one full pass
        for m in markets:
            cid = m.get("conditionId") or m.get("id")
            question = m.get("question")
            slug = m.get("slug") or m.get("market_slug")

            try:
                trades = fetch_all_trades_since(cid, since_epoch)
                stats = compute_stats_from_trades(trades)
                if stats:
                    entry = {
                        "market_id": cid,
                        "question": question,
                        "slug": slug,
                        "updated_at": dt_iso(),
                        "min_trade_px_raw": stats["min_trade_px_raw"],
                        "min_trade_px_inc": stats["min_trade_px_inc"],
                        "budgets": stats["budgets"],
                        "under_cap": (stats["min_trade_px_inc"] <= CAP_INC_FEE)
                    }
                    state[cid] = entry
                    save_state(state)
                    total += 1
                    under += 1 if entry["under_cap"] else 0
                    over  += 1 if not entry["under_cap"] else 0
                else:
                    # no trades found; keep a minimal placeholder
                    state[cid] = {
                        "market_id": cid,
                        "question": question,
                        "slug": slug,
                        "updated_at": dt_iso(),
                        "no_trades": True
                    }
                    save_state(state)
                # be gentle between markets
                time.sleep(SLEEP_BETWEEN_MARKETS)

            except Exception as e:
                print(f"[WARN {cid[:8]}] {e}")
                time.sleep(2)

        # -------- Report after full pass --------
        print("\n===== PASS REPORT =====")
        print(f"Time:           {dt_iso()}")
        print(f"Markets seen:   {len(markets)}")
        print(f"Markets updated:{total}")
        print(f"Under cap (≤{CAP_INC_FEE:.2f} inc): {under}")
        print(f"Over cap:       {over}")
        print("=======================\n")

        # rest before starting again
        time.sleep(SLEEP_AFTER_FULL_PASS)

# ------------- utils -------------
def _to_epoch_any(ts):
    try:
        x = float(ts); return int(x/1000) if x>1e12 else int(x)
    except Exception: return None

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nbye.")