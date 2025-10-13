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
import os, glob, gzip, json, time
from datetime import datetime, timezone
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

#!/usr/bin/env python3
import os, re, json, gzip, time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Iterable, Tuple, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------------------
# CONFIG
# ----------------------------
LOG_DIR = "logs"
TRADE_LOG_GLOB = os.path.join(LOG_DIR, "trades_taken_*.jsonl*")   # picks .jsonl and .jsonl.gz
OUTPUT_ALL         = os.path.join(LOG_DIR, "pl_timeseries_all.jsonl")
OUTPUT_NO_CRYPTO   = os.path.join(LOG_DIR, "pl_timeseries_no_crypto.jsonl")
UPDATE_INTERVAL_S  = 3600   # run hourly
BOOK_DEPTH = 1              # only need top of book to MTM at bid
RPS_TARGET = 5.0            # gentle rate limit for /book

BASE_BOOK  = "https://clob.polymarket.com/book"

# same fees as your main watcher to keep apples-to-apples (MTM uses raw bid; P/L uses cost basis)
SETTLE_FEE = 0.01

# crude crypto detector (expand as you like)
CRYPTO_WORDS = {
    "crypto","coin","token","blockchain","defi","stablecoin",
    "bitcoin","btc","ethereum","eth","solana","sol","xrp","ripple","doge","dogecoin",
    "ada","cardano","bnb","ton","shib","litecoin","ltc","avalanche","avax","tron","trx",
    "chainlink","link","polkadot","dot","near","aptos","apt","arbitrum","arb","base",
    "matic","polygon","pepe","sui","kaspa","kas","sei",
}

# ----------------------------
# small helpers
# ----------------------------
def _rate_limit_factory(rps: float):
    last_ts = time.monotonic()
    bucket = rps
    def _consume():
        nonlocal last_ts, bucket
        now = time.monotonic()
        bucket = min(rps, bucket + (now - last_ts) * rps)
        last_ts = now
        if bucket < 1.0:
            need = (1.0 - bucket)/rps
            time.sleep(need)
            now2 = time.monotonic()
            bucket = min(rps, bucket + (now2 - last_ts) * rps)
            last_ts = now2
        bucket -= 1.0
    return _consume

consume_token = _rate_limit_factory(RPS_TARGET)

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET","POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pl-timeseries-reporter/1.0"})
    return s

SESSION = make_session()

def parse_dt_iso(s: str) -> datetime:
    # tolerate no tz by assuming UTC; prefer ISO strings from your logs with +00:00
    try:
        return datetime.fromisoformat(s.replace("Z","+00:00"))
    except Exception:
        # last resort: treat as UTC naive
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)

def iter_trade_files() -> List[str]:
    import glob
    return sorted(glob.glob(TRADE_LOG_GLOB))

def open_maybe_gz(path: str, mode: str = "rt"):
    if path.endswith(".gz"):
        return gzip.open(path, mode)
    return open(path, mode, encoding="utf-8")

def iter_trade_records() -> Iterable[dict]:
    """Stream all trade lines across all dated parts (jsonl and jsonl.gz)."""
    for path in iter_trade_files():
        try:
            with open_maybe_gz(path, "rt") as f:
                for line in f:
                    line = line.strip()
                    if not line: continue
                    try:
                        rec = json.loads(line)
                        yield rec
                    except Exception:
                        continue
        except FileNotFoundError:
            continue

def earliest_trade_date_utc(trades: List[dict]) -> Optional[datetime]:
    dts = []
    for r in trades:
        ts = r.get("ts")
        if not ts: continue
        try:
            dts.append(parse_dt_iso(ts))
        except Exception:
            pass
    return min(dts).astimezone(timezone.utc) if dts else None

def is_crypto_market(rec: dict) -> bool:
    # check slug and question fields as your trade log provides both
    q = (rec.get("question") or "").lower()
    s = (rec.get("market_slug") or "").lower()
    text = f"{q} {s}"
    return any(w in text for w in CRYPTO_WORDS)

# ----------------------------
# positions + MTM
# ----------------------------
def build_positions_for_start(trades: List[dict], start_dt_utc: datetime, include_crypto: bool) -> Dict[str, dict]:
    """
    Build NO-only positions from trades occurring at/after start_dt_utc.
    Returns: market_id -> { 'shares': float, 'cost': float, 'side': 'NO', 'no_token_id': str }
    """
    pos: Dict[str, dict] = {}
    for rec in trades:
        if rec.get("side") != "NO":
            continue
        ts = rec.get("ts")
        if not ts: continue
        try:
            t_dt = parse_dt_iso(ts).astimezone(timezone.utc)
        except Exception:
            continue
        if t_dt < start_dt_utc:
            continue
        if not include_crypto and is_crypto_market(rec):
            continue

        mid = rec.get("market_id")
        shares = float(rec.get("shares", 0.0) or 0.0)
        cost   = float(rec.get("spent_after", 0.0) or 0.0)
        tok    = rec.get("token_id")  # NO token in your logs
        if not (mid and tok) or shares <= 0 or cost < 0:
            continue
        st = pos.setdefault(mid, {"shares": 0.0, "cost": 0.0, "side": "NO", "no_token_id": tok})
        st["shares"] += shares
        st["cost"]   += cost
    return pos

def fetch_best_bid_for_no(token_id: str) -> Optional[float]:
    """
    For NO token, MTM at bid (what you could immediately sell at).
    """
    consume_token()
    r = SESSION.get(BASE_BOOK, params={"token_id": token_id}, timeout=15)
    r.raise_for_status()
    data = r.json() or {}
    bids = data.get("bids") or []
    if not bids: 
        return 0.0
    try:
        # top-of-book highest bid
        best = max(float(b["price"]) for b in bids if "price" in b)
        return best
    except Exception:
        return 0.0

def mark_to_market(positions: Dict[str, dict]) -> Tuple[float, float, int]:
    """
    Returns (total_cost, total_value_mtm, markets_marked)
    Value MTM = sum(shares * best_bid_no).
    """
    total_cost = 0.0
    total_val  = 0.0
    marked = 0

    # de-dup book requests by token
    token_to_shares: Dict[str, float] = {}
    token_to_cost: Dict[str, float]   = {}
    for mid, st in positions.items():
        tok = st["no_token_id"]
        token_to_shares[tok] = token_to_shares.get(tok, 0.0) + float(st["shares"])
        token_to_cost[tok]   = token_to_cost.get(tok, 0.0) + float(st["cost"])

    for tok, sh in token_to_shares.items():
        bid = fetch_best_bid_for_no(tok) or 0.0
        total_val  += sh * bid
        total_cost += token_to_cost.get(tok, 0.0)
        marked += 1

    return total_cost, total_val, marked

# ----------------------------
# output
# ----------------------------
def append_jsonl(path: str, rec: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

# ----------------------------
# main loop
# ----------------------------
def compute_and_write_once(include_crypto: bool):
    # 1) load all trades into memory once (daily volumes are fine)
    trades = list(iter_trade_records())
    if not trades:
        now = datetime.now(timezone.utc).isoformat()
        print(f"[{now}] no trades found. nothing to do.")
        return

    earliest = earliest_trade_date_utc(trades)
    if earliest is None:
        now = datetime.now(timezone.utc).isoformat()
        print(f"[{now}] could not parse earliest trade date.")
        return

    now_dt = datetime.now(timezone.utc)
    # from earliest day to today, inclusive
    total_days = (now_dt.date() - earliest.date()).days
    start_dates = [datetime.combine(earliest.date() + timedelta(days=i), 
                                    datetime.min.time(), tzinfo=timezone.utc)
                   for i in range(total_days + 1)]

    variant = "all" if include_crypto else "no_crypto"
    out_path = OUTPUT_ALL if include_crypto else OUTPUT_NO_CRYPTO
    print(f"[RUN] {now_dt.isoformat()} variant={variant} days={len(start_dates)}")

    # cache of token->bid inside this run
    # (We already dedup in mark_to_market; keeping simple.)
    for sd in start_dates:
        positions = build_positions_for_start(trades, sd, include_crypto=include_crypto)
        total_cost, total_val, nmk = mark_to_market(positions)
        pl_mtm = total_val - total_cost

        rec = {
            "run_ts": now_dt.isoformat(),
            "as_of_ts": now_dt.isoformat(),
            "start_date": sd.date().isoformat(),
            "variant": variant,
            "positions_count": len(positions),
            "markets_marked": nmk,
            "total_cost": round(float(total_cost), 6),
            "mtm_value": round(float(total_val), 6),
            "pl_mtm": round(float(pl_mtm), 6),
        }
        append_jsonl(out_path, rec)
        # short print for visibility
        print(f"  {variant} | start={rec['start_date']} pos={rec['positions_count']} "
              f"cost={rec['total_cost']:.2f} mtm={rec['mtm_value']:.2f} P/L={rec['pl_mtm']:.2f}")

    print(f"[DONE] wrote to {out_path}")

def main():
    import argparse
    p = argparse.ArgumentParser(description="Continuous P/L MTM timeseries reporter")
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--include-crypto", action="store_true", help="Include crypto markets (default)")
    g.add_argument("--exclude-crypto", action="store_true", help="Exclude crypto markets")
    p.add_argument("--once", action="store_true", help="Run once and exit (no loop)")
    p.add_argument("--interval", type=int, default=UPDATE_INTERVAL_S, help="Seconds between updates (default hourly)")
    args = p.parse_args()

    include_crypto = not args.exclude_crypto

    if args.once:
        compute_and_write_once(include_crypto)
        return

    while True:
        try:
            compute_and_write_once(include_crypto)
        except Exception as e:
            print(f"[WARN] compute error: {e}")
        time.sleep(max(60, args.interval))

if __name__ == "__main__":
    main()