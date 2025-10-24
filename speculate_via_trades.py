"""
hlutir til að fylgjast með fyrir hvern markað,
all time lægsta, nr. hvaða trade það var, fyrsta undir cap, nr hvað það var, magn trades, magn af shares fyrir allt, 
kaup (ef magn stocks og price nudir cap),
magn under (open), magn over (opem), magn no trades (open)
magn under (closed), magn over (closed), magn no trades (closed)
"""
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
RETAIN_DAYS = 79           # delete logs older than this
COMPRESS_AFTER_DAYS = 91   # gzip logs older than this (but not today's)
_created_cutoff = None

GLOBAL_FEE = 0.0   # 600 for 6.00%
GLOBAL_SLIP = 0.0  # 200 for 2.00%

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