"""
hlutir til að fylgjast með fyrir hvern markað,
all time lægsta, nr. hvaða trade það var, fyrsta undir cap, nr hvað það var, magn trades, magn af shares fyrir allt, 
kaup (ef magn stocks og price nudir cap),
magn under (open), magn over (opem), magn no trades (open)
magn under (closed), magn over (closed), magn no trades (closed)
magn over x, y, z
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
import os, json, time
from datetime import datetime, timedelta, timezone
import gzip
import glob
import time, traceback, sys
import os, sys, time, json, requests, traceback
from datetime import datetime, timezone, timedelta
import argparse

bet = input("bet: ")
if not bet:
    bet = 100.0
else:
    bet = float(bet)
cap = input("cap: ")
if not cap:
    cap = 0.5
else:
    cap = float(cap)
name_log = input("name log: ")
if name_log:
    LOG_DIR = os.path.join("logs", name_log)
else:
    LOG_DIR = os.path.join("logs", f"logs_run_{bet}_{int(time.time())}")

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
get_days_back = input("days=, hours=, minutes= : ")
if not get_days_back:
    days_back = parse_start_input("days=10")
else:
    days_back = parse_start_input(get_days_back)

def fetch_open_yesno_fast(limit=250, max_pages=100, days_back=360, offset=0,
                          require_clob=False, min_liquidity=None, min_volume=None,
                          session=SESSION, verbose=True):
    params = {
        "limit": limit,
        "order": "startDate",
        "ascending": False,
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
    pages = 0

    while pages < max_pages:
        time.sleep(1)
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
    all_rows.sort(key=lambda m: (m.get("startDate") or m.get("createdAt") or ""), reverse=False)
    if verbose:
        print(f"✅ Total open Yes/No markets: {len(all_rows)}")
    return all_rows

def fetch_trades(market_dict, session=SESSION, limit=100, max_pages=250):
    """Pull full trade history with retries+timeouts and a hard time budget."""
    cid = market_dict["conditionId"]
    offset = 0
    all_trades = []
    while True:
        try:
            params={"market": cid, "sort": "asc", "limit": limit, "offset": offset,}
            r = session.get(DATA_TRADES, params=params, timeout=20)
            r.raise_for_status()
            payload = r.json()
            
            if not payload or len(payload) == 0:
                break
            all_trades.extend(payload)
            offset += limit
            if offset // limit >= max_pages:
                print(f"[WARN] Hit max_pages ({max_pages}), stopping early.")
                break
            time.sleep(0.3)
        except:
            return all_trades
    return all_trades
#current
def is_actively_tradable(m):
    if not m.get("enableOrderBook"): return False
    toks = m.get("clobTokenIds"); 
    if isinstance(toks, str):
        try: toks=json.loads(toks)
        except: toks=[]
    q = (m.get("question") or "").lower()
    # Skip range/between/greater-than style if you want simpler binarys:
   #if any(w in q for w in ["between", "range", "greater than", "less than"]):
    #    return False
    return isinstance(toks, list) and len(toks) == 2

def decode_trades(trades, market, cap=0.5, bet=100):
    # Initialize metrics
    smallest_ever = 0.99
    amount_under_cap = 0
    notional_under_cap = 0
    trades_till_fill = 0
    count = 0
    first_under = None
    first = False

    # Buckets by dollar amount
    spread = {
        "5": [0, 0], "10": [0, 0], "25": [0, 0], "50": [0, 0],
        "75": [0, 0], "100": [0, 0], "150": [0, 0], "200": [0, 0],
        "300": [0, 0], "400": [0, 0], "500": [0, 0],
        "750": [0, 0], "1000": [0, 0],
    }

    if not trades:
        print(f"NO TRADES FOR | {market['question']}")
        return None

    # Sort by price ascending if needed
    trades = sorted(trades, key=lambda x: x["price"])

    for tr in trades:
        count += 1
        pr = tr["price"]
        sz = tr["size"]
        notional = pr * sz

        if not first and (pr <= cap):
            first_under = (pr , sz)

        if count > 9999:
            break

        # Track the smallest trade price seen
        if pr < smallest_ever:
            smallest_ever = pr

        # Track under cap stats
        if pr <= cap:
            amount_under_cap += sz
            notional_under_cap += notional
            trades_till_fill += 1

        # Update each bucket — how much notional is available up to that size
        cumulative_notional = 0
        for key in spread:
            bucket = int(key)
            if bucket >= notional:
                spread[key][0] += sz       # total size
                spread[key][1] += notional # total notional
                break
            cumulative_notional += notional

    return {
        "market": market["question"],
        "smallest_price": smallest_ever,
        "first_under": first_under,
        "amount_under_cap": amount_under_cap,
        "notional_under_cap": notional_under_cap,
        "trades_till_fill": trades_till_fill,
        "spread": spread
    }

def wl_markets_under_cap(market):
    if market.get("closed") == False:
        return "TBD"
    outcome_raw = market.get("outcomePrices", ["0", "0"])
    outcome = json.loads(outcome_raw) if isinstance(outcome_raw, str) else outcome_raw
    yes_p, no_p = float(outcome[0]), float(outcome[1])
    if 0.02 < yes_p < 0.98 and 0.02 < no_p < 0.98:
        return "TBD"
    if no_p > yes_p:
        return "NO"
    elif no_p < yes_p:
        return "YES"
    else:
        print("how?")
        return "TBD"


def run_historic(days_back, bet, cap):
    finished = False
    offset = 0
    under = 0
    over = 0
    no_trades = 0
    wl = 0
    tbd = 0
    wl_notional = 0.0
    while not finished:
        days_ago = (datetime.now(timezone.utc) - datetime.fromtimestamp(days_back, tz=timezone.utc)).days
        markets = fetch_open_yesno_fast(offset=offset, days_back=days_ago)
        offset += 100 * 250
        for market in markets:
            raw_trades = fetch_trades(market)
            if raw_trades:
                dec = decode_trades(raw_trades, market, cap=cap, bet=bet)
                res = wl_markets_under_cap(market)
                if dec["amount_under_cap"] > bet:
                    under += 1
                else:
                    over += 1
                if res == "YES":
                    wl -= 1
                    wl_notional -= bet
                elif res == "TBD":
                    tbd += 1
                elif (res == "NO") and (dec["amount_under_cap"] > bet):
                    wl += 1
                    wl_notional += (bet/cap) - bet
                else:
                    print("ERROR SOMETHING HORRIBLE WENT WRONG")
                print(f"wl:{wl},{wl_notional} tbd:{tbd} | ratio:{under}/{over} | ${dec["amount_under_cap"]} | time:{dec["trades_till_fill"]} | {dec["market"][:40]}")
            else:
                no_trades += 1
                print(f"NO TRADES | {no_trades} | {market["question"]}")
        log_view(wl, wl_notional, no_trades, tbd, under, over)
            
def fetch_yesno_fast(limit=250, max_pages=100, days_back=360, offset=0,
                          require_clob=False, min_liquidity=None, min_volume=None,
                          session=SESSION, verbose=True,
                          now=(datetime.now(timezone.utc) - timedelta(minutes=3)).isoformat()):
    params = {
        "limit": limit,
        "order": "startDate",
        "start_date_min": now,
        "ascending": False,
    }
    if require_clob:
        params["enableOrderBook"] = True
    if min_liquidity is not None:
        params["liquidity_num_min"] = float(min_liquidity)
    if min_volume is not None:
        params["volume_num_min"] = float(min_volume)

    all_rows, seen_ids = [], set()
    pages = 0

    while pages < max_pages:
        time.sleep(1)
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
    all_rows.sort(key=lambda m: (m.get("startDate") or m.get("createdAt") or ""), reverse=False)
    if verbose:
        print(f"✅ Total open Yes/No markets: {len(all_rows)}")
    return all_rows

old_markets = []
def run_active():
    #fetch marketr from x date real time right away, search for createdAt (should return only a few markets)
    #load saved trades, compare if new are in old, if not create bet at price
    #save when "bet placed" make sure trades check only "takes if param happens after place time"
    #check trades filterd by startDate
    #periodically check if trades are finnished, maybe another script
    now = (datetime.now(timezone.utc) - timedelta(minutes=3)).isoformat()
    new_markets = fetch_yesno_fast(now=now)
    for market in new_markets:
        if market in old_markets:
            continue
        if len(old_markets) > 100:
            old_markets = []
        print(f"{market["startDate"]} | {market["question"]}")
        save_market(market, now)



def main():
    p = argparse.ArgumentParser(description="trades taken under cap simulator")
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--active", action="store_true", help="simulate looking at new markets store-ing all including not taken")
    g.add_argument("--historic", action="store_true", help="simulating historic to compare with active")
    args = p.parse_args()

    if args.historic:
        run_historic(days_back, bet, cap)
    else:
        while True:
            try:
                run_active()
            except Exception as e:
                print(f"[WARN] compute error: {e}")
            time.sleep(10)

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

def log_view(wl, wl_notional, no_trades, tbd, under, over):
    rec = {
        "trades_taken":under,
        "skipped":over,
        "p/l":wl_notional,
        "w/l":wl,
        "tbd":tbd,
        "no_trades":no_trades
    }
    append_jsonl(RUN_SNAP_BASE, rec)

def save_market(market, time):
    rec = {
    "time_found": time,
    "id": market["id"],
    "question": market["question"]
    }
    append_jsonl(RUN_SNAP_BASE, rec)



def check_old_markets():
    ...



def now_iso():
    return datetime.now(timezone.utc).isoformat()

if __name__ == "__main__":
    main()
    """
    active mode, only track closed false open markets, follow up report wl --slow
    history mode, from x days back give report svo end -- fast ish
    """

    """
    def main():
    bet = 100
    under = 0
    over = 0
    no_trades = 0
    wl = 0
    tbd = 0
    wl_notional = 0.0
    m = fetch_open_yesno_fast(days_back=100)
    for i in m:
        trades = fetch_trades(i)
        if trades:
            dec = decode_trades(trades, i, bet=bet)
            if dec["amount_under_cap"] > 5:
                under += 1
            else:
                over += 1
            finnished = wl_markets_under_cap(i)
            if finnished == "NO":
                wl += 1
                wl_notional += 4
            elif finnished == "YES":
                wl -= 1
                wl_notional -= 5
            elif finnished == "TBD":
                tbd += 1
            print(f"wl:{wl},{wl_notional} tbd:{tbd} | ratio:{under}/{over} | ${dec["amount_under_cap"]} | time:{dec["trades_till_fill"]} | {dec["market"][:40]}")

        else:
            no_trades += 1
            print(f"NO TRADES | {no_trades} | {i["question"]}")
    """