#!/usr/bin/env python3
import json, time, os, math, random, re, gzip, glob
from datetime import datetime, timezone, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import defaultdict, deque

# ----------------- Config -----------------
BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"
LOG_OUT      = "trades_only_stats.jsonl"

POLL_MARKETS_EVERY_S = 300          # refresh market list
POLL_TRADES_EVERY_S  = 15           # poll trade pages
TRADES_LOOKBACK_S    = 6*3600       # keep last 6h of prints per market
TRADES_PAGE_LIMIT    = 200          # per request
MAX_MARKET_PAGES     = 2

# fee/slip as FRACTIONS (not bps)
FEE  = 0.00
SLIP = 0.00
FEE_MULT = 1.0 + FEE + SLIP

# budgets to evaluate (USD notional, using raw prices in the interval)
TARGET_BUDGETS = [25, 50, 100, 250, 500]

# RPS limiter
RPS_TARGET = 8.0

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
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pm-trades-only/1.0"})
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

# ----------------- Helpers -----------------
def _dt_iso_utc():
    return datetime.now(timezone.utc).isoformat()

def _to_epoch(ts_any):
    """accept ms or s epoch"""
    try:
        x = float(ts_any)
        return int(x/1000) if x > 1e12 else int(x)
    except Exception:
        return None

def fetch_open_yesno_fast(limit=250, max_pages=2):
    params = {"limit": limit, "order": "startDate", "ascending": False, "closed": False, "enableOrderBook": True}
    seen, out = set(), []
    offset = 0
    for page in range(max_pages):
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
                s = {str(x).strip().lower() for x in outs}
                if s == {"yes","no"}:
                    mid = m.get("conditionId") or m.get("id")
                    if mid and mid not in seen:
                        out.append(m); seen.add(mid)
        if len(data) < limit: break
        offset += limit
    return out

def fetch_trades_recent(cid: str, limit=TRADES_PAGE_LIMIT):
    _rate_limit()
    r = SESSION.get(DATA_TRADES, params={"market": cid, "sort": "desc", "limit": limit}, timeout=15)
    r.raise_for_status()
    return r.json() or []

def append_jsonl(path, record):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

# ----------------- Core analytics -----------------
def tighten_intervals_min_spread_for_budget(trades, budget_usd):
    """
    trades: list of (price, size) using RAW trade price & size
    returns best tuple: (spread, p_low, p_high, dollars_in_interval, shares, vwap_raw)
    We find minimal width interval by price whose sum(price*size) >= budget_usd.
    """
    if not trades:
        return None
    # sort by price asc
    pts = sorted(((float(p), float(s)) for p,s in trades if p>0 and s>0), key=lambda x: x[0])
    n = len(pts)
    i = 0
    total_dollars = 0.0
    total_shares  = 0.0
    best = None  # (spread, i, j, dollars, shares)

    j = 0
    for i in range(n):
        # expand j until we cover budget
        while j < n and total_dollars < budget_usd:
            p,s = pts[j]
            total_dollars += p*s
            total_shares  += s
            j += 1
        if total_dollars >= budget_usd:
            p_low, _ = pts[i]
            p_high,_ = pts[j-1]
            spread = p_high - p_low
            cand = (spread, i, j-1, total_dollars, total_shares)
            if (best is None) or (spread < best[0]):
                best = cand
        # shrink from left
        p,s = pts[i]
        total_dollars -= p*s
        total_shares  -= s
        # window moves on; j stays where it is (classic two-pointer)

    if not best:
        return None
    spread, i0, j0, dollars, shares = best
    p_low = pts[i0][0]
    p_high= pts[j0][0]
    vwap_raw = (sum(p*s for p,s in pts[i0:j0+1]) /
                max(1e-12, sum(s for _,s in pts[i0:j0+1])))
    return (spread, p_low, p_high, dollars, shares, vwap_raw)

class TradesOnlyTracker:
    def __init__(self, budgets):
        self.budgets = list(budgets)
        self.by_market = {}              # cid -> deque of trades dicts (bounded by time)
        self.seen_trade_keys = set()     # dedupe
        self.market_meta = {}            # cid -> {"question":..., "slug":...}

    def ingest_trades(self, cid, trades):
        dq = self.by_market.setdefault(cid, deque())
        now_s = int(time.time())
        for t in trades:
            # create a dedupe key that survives missing IDs
            key = (cid, t.get("id") or t.get("transactionId") or (t.get("timestamp"), t.get("price"), t.get("size"), t.get("side")))
            if key in self.seen_trade_keys:
                continue
            self.seen_trade_keys.add(key)

            px = float(t.get("price", 0) or 0)
            sz = float(t.get("size", 0) or 0)
            side = str(t.get("side","")).lower()  # "buy"/"sell" from the aggressor perspective
            outcome = str(t.get("outcome","")).lower()  # "yes"/"no"
            ts = _to_epoch(t.get("timestamp") or t.get("time") or t.get("ts") or now_s)
            if px <= 0 or sz <= 0 or ts is None:
                continue
            dq.append({"ts": ts, "price": px, "size": sz, "side": side, "outcome": outcome})
        # time prune
        cutoff = int(time.time()) - TRADES_LOOKBACK_S
        while dq and dq[0]["ts"] < cutoff:
            dq.popleft()

    def snapshot_stats(self, cid):
        dq = self.by_market.get(cid)
        if not dq:
            return None
        # only NO side if you want NO-specific; otherwise all trades as liquidity
        tuples = [(t["price"], t["size"]) for t in dq if t["outcome"] == "no"]
        if not tuples:
            tuples = [(t["price"], t["size"]) for t in dq]  # fallback: any side
        if not tuples:
            return None

        min_px = min(p for p,_ in tuples)
        min_px_inc = min_px * FEE_MULT

        per_budget = {}
        for B in self.budgets:
            win = tighten_intervals_min_spread_for_budget(tuples, B)
            if win is None:
                per_budget[str(B)] = None
            else:
                spread, lo, hi, dollars, shares, vwap_raw = win
                per_budget[str(B)] = {
                    "min_spread_raw": round(spread, 6),
                    "lo": round(lo,6),
                    "hi": round(hi,6),
                    "vwap_raw": round(vwap_raw, 6),
                    "vwap_inc": round(vwap_raw*FEE_MULT, 6),
                    "covered_dollars": round(dollars, 2),
                    "covered_shares": round(shares, 6),
                }

        return {
            "min_trade_px_raw": round(min_px, 6),
            "min_trade_px_inc": round(min_px_inc, 6),
            "budgets": per_budget
        }

def main():
    tracker = TradesOnlyTracker(TARGET_BUDGETS)
    last_markets = 0
    markets = []
    last_markets_pull = 0

    while True:
        now_s = int(time.time())

        # refresh markets list
        if now_s - last_markets_pull >= POLL_MARKETS_EVERY_S or not markets:
            try:
                markets = fetch_open_yesno_fast(limit=250, max_pages=MAX_MARKET_PAGES)
                last_markets = len(markets)
                last_markets_pull = now_s
                # cache meta
                for m in markets:
                    cid = m.get("conditionId") or m.get("id")
                    tracker.market_meta[cid] = {
                        "question": m.get("question"),
                        "slug": m.get("slug") or m.get("market_slug"),
                    }
                print(f"[MKT] loaded {last_markets} markets @ {datetime.now(timezone.utc).isoformat()}")
            except Exception as e:
                print(f"[WARN markets] {e}")

        # poll trades for each market (paged shallowly for freshness)
        for m in markets:
            cid = m.get("conditionId") or m.get("id")
            try:
                trades = fetch_trades_recent(cid, limit=TRADES_PAGE_LIMIT)
                tracker.ingest_trades(cid, trades)
                stats = tracker.snapshot_stats(cid)
                if stats:
                    rec = {
                        "ts": _dt_iso_utc(),
                        "market_id": cid,
                        "slug": tracker.market_meta.get(cid,{}).get("slug"),
                        "question": tracker.market_meta.get(cid,{}).get("question"),
                        "min_trade_px_raw": stats["min_trade_px_raw"],
                        "min_trade_px_inc": stats["min_trade_px_inc"],
                        "budgets": stats["budgets"],
                    }
                    append_jsonl(LOG_OUT, rec)

                    # quick console line (show first budget present)
                    firstB = next((b for b in TARGET_BUDGETS if rec["budgets"].get(str(b))), None)
                    if firstB:
                        b = rec["budgets"][str(firstB)]
                        print(f"[{cid[:8]}] min_px={rec['min_trade_px_raw']:.4f}  "
                              f"B={firstB}$  spread={b['min_spread_raw'] if b else None}  "
                              f"vwap_raw={b['vwap_raw'] if b else None}")
            except Exception as e:
                print(f"[WARN trades {cid[:8]}] {e}")

        time.sleep(POLL_TRADES_EVERY_S)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nbye.")