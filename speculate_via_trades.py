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



def fetch_open_yesno_fast(limit=250, max_pages=1000, days_back=360,
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
    offset, pages = 0, 0

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
    all_rows.sort(key=lambda m: (m.get("startDate") or m.get("createdAt") or ""), reverse=True)
    if verbose:
        print(f"✅ Total open Yes/No markets: {len(all_rows)}")
    return all_rows

def fetch_trades(market_dict, session=SESSION, limit=100, max_pages=10000):
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
        sz = tr.get("size", tr.get("amount", 0))
        notional = pr * sz

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
        "amount_under_cap": amount_under_cap,
        "notional_under_cap": notional_under_cap,
        "trades_till_fill": trades_till_fill,
        "spread": spread
    }


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

def main():
    bet = 100
    under = 0
    over = 0
    no_trades = 0
    trades_till = None
    smallest_ever =None
    m = fetch_open_yesno_fast(days_back=10)
    for i in m:
        trades = fetch_trades(i)
        if trades:
            dec = decode_trades(trades, i)
            if dec["amount_under_cap"] > 5:
                under += 1
            else:
                over += 1
            print(f"ratio{under}/{over}${dec["amount_under_cap"]} | time:{dec["trades_till_fill"]} | {dec["market"]}")

        else:
            print(f"NO TRADES | {i["question"]}")

if __name__ == "__main__":
    main()