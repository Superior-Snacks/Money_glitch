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

#!/usr/bin/env python3
import os, re, json, gzip, time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Iterable, Tuple, Optional

# ----------------------------
# CONFIG
# ----------------------------
BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_BOOK  = "https://clob.polymarket.com/book"
BASE_PHIST = "https://clob.polymarket.com/prices-history"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "pl-bot/1.0"})
LOG_DIR = input("name log to check:")
TRADE_LOG_GLOB = os.path.join(LOG_DIR, "trades_taken_*.jsonl*")   # picks .jsonl and .jsonl.gz
OUTPUT_ALL         = os.path.join(LOG_DIR, "pl_timeseries_all.jsonl")
OUTPUT_NO_CRYPTO   = os.path.join(LOG_DIR, "pl_timeseries_no_crypto.jsonl")
UPDATE_INTERVAL_S  = 3600   # run hourly
BOOK_DEPTH = 1              # only need top of book to MTM at bid
RPS_TARGET = 5.0            # gentle rate limit for /book
CLOSED_NO = os.path.join(LOG_DIR, "closed_no.jsonl")
CLOSED_YES = os.path.join(LOG_DIR, "closed_yes.jsonl")

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

# --- simple per-run caches ---
_market_meta_cache: dict[str, dict] = {}
_best_bid_cache: dict[str, float | None] = {}
_last_price_cache: dict[str, float | None] = {}

def fetch_market_meta_cached(condition_id: str) -> dict | None:
    m = _market_meta_cache.get(condition_id)
    if m is not None:
        return m
    m = fetch_market_meta(condition_id)
    _market_meta_cache[condition_id] = m
    return m


def read_last_snapshot_for(outfile: str, variant: str, start_date: str) -> dict | None:
    """
    Read the most recent line in `outfile` for the given (variant, start_date).
    Returns that JSON record or None if not found.
    """
    if not os.path.exists(outfile):
        return None
    last = None
    with open(outfile, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("variant") == variant and rec.get("start_date") == start_date:
                last = rec
    return last


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

def parse_dt_any(v):
    """Best-effort parse: ISO (with/without Z) or epoch seconds/millis → aware UTC datetime."""
    if not v:
        return None
    # numeric epoch?
    try:
        if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().isdigit()):
            ts = float(v)
            if ts > 1e12:  # millis → seconds
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        pass
    # ISO-ish
    try:
        s = str(v).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

# ----------------------------
# output
# ----------------------------
def append_jsonl(path: str, rec: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

def open_jsonl():
    ...

# ----------------------------
# main loop
# ----------------------------
def make_bets():
    ...

def main():
    while True:
        try:
            make_bets()
        except Exception as e:
            print(f"[WARN] compute error: {e}")
        time.sleep(60*60)

if __name__ == "__main__":
    main()