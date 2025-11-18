# ============================================================
#   Polymarket Historical Cap Spread Builder (Parallel A)
# ============================================================
# Features:
#   --days N         → look back N days
#   --parallel N     → number of trade fetch workers (0 = serial)
#   retry on trade fetch failure
#   skipped_trades count logged
# ============================================================

import os, sys, json, random, time
import argparse
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock  # <--- ADDED FOR SAFETY

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ======================= Config / Constants =======================
BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"
LOGS_DIR     = "logs"

TRADES_PAGE_LIMIT = 250
MAX_TRADE_RETRIES = 5   # per-market trade fetch retry attempts

# Adaptive RPS bucket
RPS_TARGET           = 1.0
_RPS_SCALE           = 1.0
_RPS_MIN             = 0.3
_RPS_RECOVER_PER_SEC = 0.03
_last_tokens_ts      = time.monotonic()
_bucket              = RPS_TARGET

# GLOBAL LOCKS
_RATE_LOCK  = Lock()
_CACHE_LOCK = Lock()

# Cap spread buckets
CAPS = [
    0.001, 0.05, 0.10, 0.15, 0.20,
    0.25, 0.30, 0.35, 0.40, 0.45,
    0.50, 0.55, 0.60, 0.65, 0.70,
    0.75, 0.80, 0.85, 0.90, 0.95, 1.0
]

# ============================================================
#                       HTTP + RATE LIMIT
# ============================================================
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=40, pool_maxsize=40)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pm-maker-cap-spread-historical/parallel-A"} )
    return s

SESSION = make_session()
_num_429 = 0


def _rate_limit():
    global _last_tokens_ts, _bucket, _RPS_SCALE

    # <--- LOCKING for Thread Safety
    with _RATE_LOCK:
        now = time.monotonic()
        rps_now = max(RPS_TARGET * _RPS_SCALE, 0.1)
        _bucket = min(rps_now, _bucket + (now - _last_tokens_ts) * rps_now)
        _last_tokens_ts = now

        if _bucket < 1.0:
            need = (1.0 - _bucket) / rps_now
            # We hold the lock while sleeping to ensure strict serial rate limiting
            # across threads. This prevents 10 threads from seeing "bucket ok" 
            # simultaneously and hammering the API.
            time.sleep(need)
            
            now2 = time.monotonic()
            _bucket = min(rps_now, _bucket + (now2 - _last_tokens_ts) * rps_now)
            _last_tokens_ts = now2

        _bucket -= 1.0


def _rps_on_429():
    global _RPS_SCALE, _num_429
    with _RATE_LOCK:
        _num_429 += 1
        _RPS_SCALE = max(_RPS_MIN, _RPS_SCALE * 0.7)


def _rps_recover(dt_sec: float):
    global _RPS_SCALE
    with _RATE_LOCK:
        _RPS_SCALE = min(1.0, _RPS_SCALE + _RPS_RECOVER_PER_SEC * dt_sec)


def _retry_after_seconds(resp) -> float:
    ra = resp.headers.get("Retry-After")
    if not ra:
        return 0.0
    try:
        return max(0.0, float(ra))
    except:
        return 0.0


def http_get_with_backoff(url, *, params=None, timeout=20, max_tries=8):
    back = 0.5
    tries = 0
    last_t = time.monotonic()

    while True:
        _rate_limit()

        try:
            r = SESSION.get(url, params=params or {}, timeout=timeout)
        except requests.RequestException as e:
            print(f"      [HTTP EXC] {e} → sleep {back:.1f}")
            time.sleep(back)
            back = min(back * 1.7, 20.0)
            tries += 1
            if tries >= max_tries:
                raise
            continue

        if r.status_code < 400:
            now = time.monotonic()
            _rps_recover(now - last_t)
            return r

        if r.status_code == 429:
            _rps_on_429()
            ra = _retry_after_seconds(r)
            sleep_s = max(ra, back)
            print(f"      [429] sleep {sleep_s:.1f}")
            time.sleep(sleep_s)
            back = min(back * 1.8, 30)
            tries += 1
            if tries >= max_tries:
                r.raise_for_status()
            continue

        if 500 <= r.status_code < 600:
            print(f"      [5xx] sleep {back:.1f}")
            time.sleep(back)
            back = min(back * 1.7, 20)
            tries += 1
            if tries >= max_tries:
                r.raise_for_status()
            continue

        print(f"      [HTTP {r.status_code}] fatal")
        r.raise_for_status()

# ============================================================
#                     SIMPLE UTILITIES
# ============================================================
def dt_iso():
    return datetime.now(timezone.utc).isoformat()


def _to_epoch_any(x):
    try:
        f = float(x)
        return int(f / 1000) if f > 1e12 else int(f)
    except:
        pass

    if isinstance(x, str):
        s = x.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo:
                dt = dt.astimezone(timezone.utc)
            else:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except:
            return None

    return None


def ensure_dir(p):
    os.makedirs(p, exist_ok=True)


def atomic_write_text(path: str, text: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)

# ============================================================
#                     MARKET RESOLUTION LOGIC
# ============================================================
MARKET_META_CACHE = {}

HINT_SPREAD = 0.98
FINAL_GRACE = timedelta(days=2)


def _parse_dt_any(v):
    if not v:
        return None
    try:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(float(v), tz=timezone.utc)
        if isinstance(v, str) and v.strip().isdigit():
            return datetime.fromtimestamp(float(v), tz=timezone.utc)
    except:
        pass

    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except:
        return None


def resolve_status(m: dict):
    uma = (m.get("umaResolutionStatus") or "").lower()
    if uma in ("yes", "no"):
        return True, uma.upper(), "umaResolutionStatus"
    if uma.startswith("resolved_"):
        w = uma.split("_", 1)[1].upper()
        if w in ("YES", "NO"):
            return True, w, "umaResolutionStatus"

    w = (m.get("winningOutcome") or m.get("winner") or "").upper()
    if w in ("YES", "NO"):
        return True, w, "winningOutcome"

    if m.get("closed"):
        end_dt = (
            _parse_dt_any(m.get("closedTime"))
            or _parse_dt_any(m.get("umaEndDate"))
            or _parse_dt_any(m.get("endDate"))
            or _parse_dt_any(m.get("updatedAt"))
        )
        age_ok = True if end_dt is None else (datetime.now(timezone.utc) - end_dt) >= FINAL_GRACE

        raw = m.get("outcomePrices", ["0", "0"])
        prices = json.loads(raw) if isinstance(raw, str) else raw
        try:
            y, n = float(prices[0]), float(prices[1])
        except:
            y = n = None

        if age_ok and y is not None and n is not None:
            if y >= HINT_SPREAD and n <= 1 - HINT_SPREAD:
                return True, "YES", "terminal_prices"
            if n >= HINT_SPREAD and y <= 1 - HINT_SPREAD:
                return True, "NO", "terminal_prices"

        if y is not None and n is not None:
            if y >= 0.90 and n <= 0.10:
                return True, "YES", "closed_hint_yes"
            if n >= 0.90 and y <= 0.10:
                return True, "NO", "closed_hint_no"

    return False, None, "unresolved"


def current_status(m: dict) -> str:
    resolved, winner, _ = resolve_status(m)
    if resolved:
        return winner
    return "TBD"

# ============================================================
#                 TRADE FETCHING (OFFSET)
# ============================================================
def trade_ts(trade: dict) -> int:
    for key in ("match_time", "timestamp", "time", "ts", "last_update"):
        v = trade.get(key)
        ts = _to_epoch_any(v)
        if ts:
            return ts
    return 0


def fetch_trades_page(cid: str, limit: int, offset: int):
    params = {
        "market": cid,
        "limit": limit,
        "offset": offset,
        "sort": "asc",
    }
    r = http_get_with_backoff(DATA_TRADES, params=params, timeout=20)
    return r.json() or []


def fetch_all_trades_since(cid: str, baseline: int):
    """
    SERIAL trade fetch for one market (but called in parallel externally)
    """
    offset = 0
    seen = set()
    uniq = []
    seen_since = False
    
    # <--- PRINT START
    print(f"   -> [FETCH START] cid={cid[:10]} baseline={baseline}")

    for page_num in range(5000):
        page = fetch_trades_page(cid, TRADES_PAGE_LIMIT, offset)
        
        if not page:
            # <--- PRINT EMPTY
            print(f"      [Page {page_num}] Empty response, stopping. cid={cid[:10]}")
            break

        added = 0
        for t in page:
            tid = t.get("id") or f"{t.get('price')}-{t.get('size')}-{t.get('match_time')}"
            if tid in seen:
                continue
            
            ts = trade_ts(t)
            if ts < baseline:
                continue
                
            seen_since = True
            seen.add(tid)
            uniq.append(t)
            added += 1

        # <--- PRINT PROGRESS
        print(f"      [Page {page_num}] offset={offset} fetched={len(page)} new_added={added} (Total: {len(uniq)}) cid={cid[:10]}")

        if seen_since and added == 0:
            print(f"      [STOP] No new trades found in this page. cid={cid[:10]}")
            break
        if len(page) < TRADES_PAGE_LIMIT:
            print(f"      [STOP] Page size {len(page)} < {TRADES_PAGE_LIMIT}. End of stream. cid={cid[:10]}")
            break

        offset += TRADES_PAGE_LIMIT

    uniq.sort(key=trade_ts)
    return uniq

# ============================================================
#                 PARALLEL TRADE FETCH WRAPPER
# ============================================================
def fetch_trades_parallel_task(cid: str, baseline: int):
    """
    Safe wrapper that retries, returns:
    { "cid": cid, "trades": [...], "ok": bool }
    """
    for attempt in range(1, MAX_TRADE_RETRIES + 1):
        try:
            trades = fetch_all_trades_since(cid, baseline)
            return {"cid": cid, "trades": trades, "ok": True}
        except Exception as e:
            # <--- PRINT ERROR
            print(f"[WARN] trade fetch failed cid={cid[:12]} attempt={attempt} err={e}")
            time.sleep(1.5 * attempt)

    print(f"[SKIP] trade fetch failed after retries cid={cid[:12]}")
    return {"cid": cid, "trades": [], "ok": False}

# ============================================================
#                   CAP SPREAD COLLECTION
# ============================================================
def collect_cap_spread(trades: List[dict], caps: List[float], since_epoch: int):
    caps = sorted({round(c, 3) for c in caps})

    spread = {
        "last_trade_ts": since_epoch,
        "caps": {c: {"shares": 0.0, "dollars": 0.0, "trades": 0} for c in caps}
    }

    for t in trades:
        ts = trade_ts(t)
        if ts < since_epoch:
            continue

        if ts > spread["last_trade_ts"]:
            spread["last_trade_ts"] = ts

        if (t.get("outcome") or "").lower() != "no":
            continue

        try:
            p = float(t.get("price"))
            s = float(t.get("size"))
        except:
            continue

        if p <= 0 or s <= 0:
            continue

        notional = p * s
        for c in caps:
            if c >= p:
                st = spread["caps"][c]
                st["shares"] += s
                st["dollars"] += notional
                st["trades"] += 1

    json_caps = {f"{c:.3f}": {
        "shares": round(st["shares"], 6),
        "dollars": round(st["dollars"], 2),
        "trades": st["trades"]
    } for c, st in spread["caps"].items()}

    return {"last_trade_ts": spread["last_trade_ts"], "caps": json_caps}

# ============================================================
#                 FETCH ALL MARKETS FROM GAMMA
# ============================================================
def fetch_markets_since(start_epoch: int):
    out = {}
    offset = 0
    limit = 500

    print(f"{dt_iso()} Fetching markets since {start_epoch}")

    while True:
        # Added print to show progress
        print(f"   [Market Discovery] Fetching offset {offset}... (Found {len(out)} relevant so far)")
        
        r = http_get_with_backoff(BASE_GAMMA, params={"limit": limit, "offset": offset}, timeout=30)
        rows = r.json() or []
        
        if not rows:
            print("   [Market Discovery] No more markets returned by API.")
            break

        for m in rows:
            cid = m.get("conditionId")
            if not cid:
                continue
            created_raw = m.get("createdAt") or m.get("updatedAt")
            created_ts = _to_epoch_any(created_raw)
            
            # Filter: Only keep markets created after start_epoch
            if created_ts is None or created_ts < start_epoch:
                continue
                
            out[cid] = {
                "conditionId": cid,
                "question": m.get("question"),
                "created_epoch": created_ts,
                "createdAt": created_raw,
            }

        offset += len(rows)
        
        # Optional: Stop if the API returns fewer than limit (end of list), 
        # though usually empty list catches this.
        if len(rows) < limit:
            print("   [Market Discovery] Reached end of market list.")
            break

    return out

# ============================================================
#                             MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, required=True, help="Days back to fetch markets")
    parser.add_argument("--parallel", type=int, default=0,
                        help="Parallel trade workers (0 = serial)")
    args = parser.parse_args()

    days_back = args.days
    workers   = args.parallel

    start_epoch = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp())

    out_dir = os.path.join(LOGS_DIR, f"daysback_{days_back}")
    ensure_dir(out_dir)
    open_path   = os.path.join(out_dir, "open.jsonl")
    closed_path = os.path.join(out_dir, "closed.jsonl")

    print(f"{dt_iso()} Starting historical build… parallel={workers}")

    markets = fetch_markets_since(start_epoch)
    total = len(markets)
    print(f"Markets found: {total}")

    open_rows   = []
    closed_rows = []

    skipped_trades = 0

    # ---------------------- Parallel processing -----------------------
    if workers > 0:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = []
            for cid, meta in markets.items():
                baseline = meta["created_epoch"] + 5 * 60
                futures.append(pool.submit(fetch_trades_parallel_task, cid, baseline))

            for i, fut in enumerate(as_completed(futures), 1):
                res = fut.result()
                cid = res["cid"]
                meta = markets[cid]
                q = meta["question"]

                print(f"\n[{i}/{total}] Processing: {q} (cid={cid[:10]})")

                # fetch meta / status
                m = fetch_market_meta(cid)
                status = current_status(m)

                if not res["ok"]:
                    skipped_trades += 1
                    print(f"    [SKIPPED TRADES] for {cid}")
                    trades = []
                else:
                    trades = res["trades"]
                    print(f"    [OK] Processed {len(trades)} trades for {cid[:10]}")

                cap_spread = collect_cap_spread(trades, CAPS, meta["created_epoch"]+5*60)

                row = {
                    "ts": dt_iso(),
                    "status": status,
                    "conditionId": cid,
                    "question": q,
                    "createdAt": meta["createdAt"],
                    "created_epoch": meta["created_epoch"],
                    "cap_spread": cap_spread,
                }

                if status in ("YES","NO"):
                    closed_rows.append(row)
                else:
                    open_rows.append(row)

    # ------------------------- SERIAL -----------------------------
    else:
        for i, (cid, meta) in enumerate(markets.items(), 1):
            q = meta["question"]
            print(f"\n[{i}/{total}] {q} (cid={cid[:10]})")

            m = fetch_market_meta(cid)
            status = current_status(m)

            baseline = meta["created_epoch"] + 5 * 60

            ok = False
            for attempt in range(1, MAX_TRADE_RETRIES+1):
                try:
                    trades = fetch_all_trades_since(cid, baseline)
                    ok = True
                    break
                except Exception:
                    time.sleep(1.5 * attempt)

            if not ok:
                skipped_trades += 1
                trades = []
            
            print(f"    -> Trades fetched: {len(trades)}")

            cap_spread = collect_cap_spread(trades, CAPS, baseline)

            row = {
                "ts": dt_iso(),
                "status": status,
                "conditionId": cid,
                "question": q,
                "createdAt": meta["createdAt"],
                "created_epoch": meta["created_epoch"],
                "cap_spread": cap_spread,
            }

            if status in ("YES","NO"):
                closed_rows.append(row)
            else:
                open_rows.append(row)

    # --------------------------- SAVE --------------------------
    atomic_write_text(open_path,   "\n".join(json.dumps(r) for r in open_rows)   + "\n")
    atomic_write_text(closed_path, "\n".join(json.dumps(r) for r in closed_rows) + "\n")

    print("\n===== SUMMARY =====")
    print(f"Open markets saved:     {len(open_rows)}")
    print(f"Closed markets saved:   {len(closed_rows)}")
    print(f"Skipped trade fetches:  {skipped_trades}")

# Small helper to fetch meta inside parallel loop
def fetch_market_meta(cid: str):
    # <--- LOCKING for Thread Safety
    with _CACHE_LOCK:
        if cid in MARKET_META_CACHE:
            return MARKET_META_CACHE[cid]
    
    # Perform network request OUTSIDE the lock to avoid blocking other threads
    r = http_get_with_backoff(BASE_GAMMA, params={"condition_ids": cid, "limit": 1}, timeout=15)
    rows = r.json() or []
    
    res = {}
    for m in rows:
        if m.get("conditionId") == cid:
            res = m
            break
            
    # Re-acquire lock to write
    with _CACHE_LOCK:
        MARKET_META_CACHE[cid] = res
        
    return res

# ============================================================
if __name__ == "__main__":
    main()