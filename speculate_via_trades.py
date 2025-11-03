import os, sys, json, time, math, random, glob
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Tuple, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==============================
# Config
# ==============================
LOGS_ROOT      = "logs"
MARKET_SUBDIR  = "marketfiles"
SNAPSHOT_NAME  = "no_bet_snapshots.jsonl"  # appended in logs/<folder>/
BASE_GAMMA     = "https://gamma-api.polymarket.com"
GAMMA_MARKETS  = f"{BASE_GAMMA}/markets"
DATA_TRADES    = "https://data-api.polymarket.com/trades"

RPS_TARGET     = 3.0      # adaptive RPS target
_RPS_SCALE     = 1.0
_RPS_MIN       = 0.3
_RPS_RECOVER_PER_SEC = 0.03

# ==============================
# Session + rate limiter + HTTP
# ==============================
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=40, pool_maxsize=40)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pm-no-bet-backtest/1.0"})
    return s

SESSION = make_session()
_last_tokens_ts = time.monotonic()
_bucket = RPS_TARGET

def _rate_limit():
    global _last_tokens_ts, _bucket, _RPS_SCALE
    now = time.monotonic()
    rps_now = max(RPS_TARGET * _RPS_SCALE, 0.1)
    _bucket = min(rps_now, _bucket + (now - _last_tokens_ts) * rps_now)
    _last_tokens_ts = now
    if _bucket < 1.0:
        need = (1.0 - _bucket) / rps_now
        time.sleep(need)
        now2 = time.monotonic()
        _bucket = min(rps_now, _bucket + (now2 - _last_tokens_ts) * rps_now)
        _last_tokens_ts = now2
    _bucket -= 1.0

def _rps_on_429():
    global _RPS_SCALE
    _RPS_SCALE = max(_RPS_MIN, _RPS_SCALE * 0.7)

def _rps_recover(dt_sec: float):
    global _RPS_SCALE
    _RPS_SCALE = min(1.0, _RPS_SCALE + _RPS_RECOVER_PER_SEC * dt_sec)

def _retry_after_seconds(resp) -> float:
    ra = resp.headers.get("Retry-After")
    if not ra:
        return 0.0
    try:
        return max(0.0, float(ra))
    except Exception:
        return 0.0

def http_get(url: str, *, params=None, timeout=20, max_tries=8) -> requests.Response:
    back = 0.5
    tries = 0
    last_t = time.monotonic()
    while True:
        _rate_limit()
        try:
            r = SESSION.get(url, params=params or {}, timeout=timeout)
        except requests.RequestException:
            time.sleep(back + random.random()*0.2)
            back = min(back*1.7, 20.0)
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
            sleep_s = max(ra, back) + random.random()*0.3
            time.sleep(sleep_s)
            back = min(back*1.8, 30.0)
            tries += 1
            if tries >= max_tries:
                r.raise_for_status()
            continue

        if 500 <= r.status_code < 600:
            time.sleep(back + random.random()*0.2)
            back = min(back*1.7, 20.0)
            tries += 1
            if tries >= max_tries:
                r.raise_for_status()
            continue

        r.raise_for_status()

# ==============================
# Utils
# ==============================
def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def to_epoch_seconds(x) -> Optional[int]:
    """Accept ms or s or iso. Returns seconds (int) or None."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        f = float(x)
        return int(f/1000) if f > 1e12 else int(f)
    s = str(x).strip()
    # ISO?
    try:
        t = datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
        return int(t.timestamp())
    except Exception:
        pass
    # numeric string?
    try:
        f = float(s)
        return int(f/1000) if f > 1e12 else int(f)
    except Exception:
        return None

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def list_market_log_files(folder: str) -> List[str]:
    base = os.path.join(LOGS_ROOT, folder, MARKET_SUBDIR)
    ensure_dir(base)
    # common names, change as needed
    pats = [
        os.path.join(base, "markets_*.jsonl"),
        os.path.join(base, "*.jsonl"),
    ]
    files = []
    for p in pats:
        files.extend(glob.glob(p))
    return sorted(set(files))

# ==============================
# Market id resolving
# ==============================
def resolve_condition_id_from_market_obj(m: Dict[str, Any]) -> Optional[str]:
    cid = m.get("conditionId")
    if isinstance(cid, str) and cid:
        return cid
    mid = m.get("id") or m.get("market_id")
    if not mid:
        return None
    # Try Gamma /markets?id=<mid>
    try:
        r = http_get(GAMMA_MARKETS, params={"id": mid, "limit": 1}, timeout=20)
        arr = r.json() or []
        if isinstance(arr, list) and arr:
            cid2 = arr[0].get("conditionId") or arr[0].get("id")
            if isinstance(cid2, str) and cid2:
                return cid2
    except Exception:
        return None
    return None

# ==============================
# Trade fetch (after a cutoff) - NO only
# ==============================
def fetch_trades_after_NO(cid: str, since_epoch: int, cap: float) -> Tuple[List[Dict[str,Any]], int, int, Tuple[Optional[float], float]]:
    """
    Returns:
      - trades_after (raw dicts, NO only, strictly after cutoff)
      - under_count (count of NO trades priced <= cap)
      - over_count  (count of NO trades priced >  cap)
      - lowest_seen: (min_price, total_notional_at_min_price) among NO trades (after cutoff)
    Also prints each trade with UNDER/OVER and keeps running 'lowest so far' + '$ under cap' logs.
    """
    out: List[Dict[str,Any]] = []
    under_count = 0
    over_count  = 0
    lowest_price: Optional[float] = None
    lowest_notional: float = 0.0
    cumulative_under_cap: float = 0.0

    limit = 250
    starting_before = int(time.time())

    printed_header = False
    while True:
        params = {
            "market": cid,
            "sort": "desc",
            "limit": limit,
            "starting_before": int(starting_before * 1000),  # ms
        }
        r = http_get(DATA_TRADES, params=params, timeout=20)
        data = r.json() or []
        if not data:
            break

        oldest = None
        # iterate newest→oldest on this page, but we’re only interested in AFTER cutoff
        for t in data:
            ts_s = to_epoch_seconds(t.get("timestamp") or t.get("time") or t.get("ts"))
            if ts_s is None:
                continue
            if oldest is None or ts_s < oldest:
                oldest = ts_s

            if ts_s <= since_epoch:
                # older than cutoff → ignore
                continue

            outcome = str(t.get("outcome", "")).upper()
            if outcome != "NO":
                continue

            price = float(t.get("price") or 0.0)
            size  = float(t.get("size")  or 0.0)
            if price <= 0 or size <= 0:
                continue

            # Update lowest ever (since cutoff) for NO
            if (lowest_price is None) or (price < lowest_price - 1e-12):
                lowest_price = price
                lowest_notional = price * size
            elif lowest_price is not None and abs(price - lowest_price) <= 1e-12:
                lowest_notional += price * size

            # Update under/over and cumulative under-cap notional
            if price <= cap + 1e-12:
                under_count += 1
                cumulative_under_cap += price * size
                flag = "UNDER"
            else:
                over_count += 1
                flag = "OVER"

            if not printed_header:
                print(f"\n--- NO trades AFTER {datetime.fromtimestamp(since_epoch, tz=timezone.utc).isoformat()} for market {cid} ---")
                printed_header = True

            print(f"[TRADE] {datetime.fromtimestamp(ts_s, tz=timezone.utc).isoformat()} "
                  f"NO price={price:.6f} size={size:.6f} → {flag} | "
                  f"lowest_so_far={('%.6f' % lowest_price) if lowest_price is not None else 'N/A'} "
                  f"@$={lowest_notional:.2f} | $under_cap={cumulative_under_cap:.2f}")

            out.append(t)

        if oldest is None or oldest <= since_epoch:
            break
        starting_before = oldest

    return out, under_count, over_count, (lowest_price, lowest_notional)

# ==============================
# Fill simulation (NO only)
# ==============================
def compute_no_fill(trades_after: List[Dict[str,Any]], cap: float, bet_size: float) -> Dict[str,Any]:
    """
    Use ONLY NO trades at price <= cap, oldest→newest, to see if we can spend bet_size dollars.
    Returns dict: filled, filled_dollars, filled_shares, vwap.
    """
    pts: List[Tuple[int,float,float]] = []
    for t in trades_after:
        outcome = str(t.get("outcome","")).upper()
        if outcome != "NO":
            continue
        p = float(t.get("price") or 0.0)
        s = float(t.get("size") or 0.0)
        if p <= 0 or s <= 0:
            continue
        if p <= cap + 1e-12:
            ts = to_epoch_seconds(t.get("timestamp") or t.get("time") or t.get("ts"))
            pts.append((ts if ts is not None else 0, p, s))

    pts.sort(key=lambda x: x[0])

    spent = 0.0
    shares = 0.0
    for _, p, s in pts:
        need = bet_size - spent
        if need <= 0:
            break
        avail_dollars = p * s
        take_dollars = min(need, avail_dollars)
        take_shares  = take_dollars / p
        spent += take_dollars
        shares += take_shares

    filled = spent >= bet_size - 1e-9
    vwap = (spent / shares) if shares > 0 else None

    return {
        "filled": filled,
        "filled_dollars": round(spent, 6),
        "filled_shares": round(shares, 6),
        "vwap": (round(vwap, 6) if vwap is not None else None),
    }

# ==============================
# Snapshot writing
# ==============================
def append_jsonl(path: str, rec: Dict[str,Any]):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

# ==============================
# Main scanning logic
# ==============================
def parse_markets_from_logs(folder: str) -> List[Dict[str,Any]]:
    """
    Reads every JSON line from logs/<folder>/marketfiles/* and yields
    {'conditionId','id','time_found','question',...} light records.
    Deduplicates by conditionId or id, keeping the earliest time_found.
    """
    files = list_market_log_files(folder)
    found: Dict[str, Dict[str,Any]] = {}
    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                # time_found key should exist in your logs; default if missing
                tf = row.get("time_found") or row.get("time_found_at") or row.get("found_at")
                if not tf:
                    continue
                try:
                    tf_s = to_epoch_seconds(tf)
                    if tf_s is None:
                        continue
                except Exception:
                    continue

                cid = row.get("conditionId")
                mid = row.get("id") or row.get("market_id")
                q   = row.get("question") or row.get("name") or row.get("title")

                key = (cid or mid)
                if not key:
                    continue

                prev = found.get(key)
                if prev is None or tf_s < prev["time_found_s"]:
                    found[key] = {
                        "conditionId": cid,
                        "id": mid,
                        "question": q,
                        "time_found_s": tf_s,
                        "raw": row,
                    }
    return list(found.values())

def scan_once(folder: str, bet_size: float, cap: float):
    markets = parse_markets_from_logs(folder)
    out_dir = os.path.join(LOGS_ROOT, folder)
    ensure_dir(out_dir)
    snap_path = os.path.join(out_dir, SNAPSHOT_NAME)

    print(f"\n=== NO-bet backtest: folder='{folder}' bet_size=${bet_size:.2f} cap={cap:.4f} ===")
    print(f"Found {len(markets)} unique markets from logs/{folder}/{MARKET_SUBDIR}/")

    for i, m in enumerate(markets, 1):
        cid = m.get("conditionId")
        mid = m.get("id")
        q   = m.get("question")
        tf_s = m["time_found_s"]

        # Resolve conditionId if missing / numeric id
        if not cid or not (isinstance(cid, str) and cid.startswith("0x")):
            resolved = resolve_condition_id_from_market_obj(m["raw"])
            if resolved:
                cid = resolved
            else:
                # cannot fetch trades without a conditionId
                print(f"[{i}/{len(markets)}] SKIP (no conditionId) id={mid} q={q}")
                continue

        print(f"\n[{i}/{len(markets)}] Market {cid}  |  {q or '(no title)'}")
        # Pull trades after time_found, NO only; print trades and running stats
        trades_after, under_ct, over_ct, (min_px, min_notional) = fetch_trades_after_NO(cid, tf_s, cap)

        # Compute whether hypothetical NO bet fills (and VWAP) from these trades
        fill = compute_no_fill(trades_after, cap, bet_size)

        # Summary line in console
        print(f"[SUMMARY] under={under_ct}  over={over_ct}  "
              f"lowest_NO={'%.6f' % min_px if min_px is not None else 'N/A'} "
              f"@$={min_notional:.2f}  |  "
              f"filled={'YES' if fill['filled'] else 'NO'}  "
              f"spent=${fill['filled_dollars']:.2f}  shares={fill['filled_shares']:.4f}  "
              f"vwap={(('%.6f' % fill['vwap']) if fill['vwap'] is not None else 'N/A')}")

        # Append snapshot row
        row = {
            "time_logged": iso_now(),
            "folder": folder,
            "market_id": cid,
            "question": q,
            "time_found": datetime.fromtimestamp(tf_s, tz=timezone.utc).isoformat(),
            "bet_size": bet_size,
            "cap": cap,
            "trades_after_count": len(trades_after),
            "under_count": under_ct,
            "over_count": over_ct,
            "lowest_no_price": (round(min_px, 6) if min_px is not None else None),
            "lowest_no_notional": round(min_notional, 4),
            "filled": fill["filled"],
            "filled_dollars": fill["filled_dollars"],
            "filled_shares": fill["filled_shares"],
            "fill_vwap": fill["vwap"],
        }
        append_jsonl(snap_path, row)

    print(f"\nSaved snapshots → {snap_path}")

# ==============================
# CLI / loop
# ==============================
def ask_float(prompt: str, default: Optional[float]=None) -> float:
    while True:
        s = input(f"{prompt}{' ['+str(default)+']' if default is not None else ''}: ").strip()
        if not s and default is not None:
            return float(default)
        try:
            return float(s)
        except Exception:
            print("Enter a number.")

def main():
    folder = input("Custom folder name under logs/ (where 'marketfiles' lives): ").strip()
    if not folder:
        print("Need a folder name (e.g., 'customname').")
        sys.exit(1)

    bet_size = ask_float("Bet size $ (NO)", 100.0)
    cap      = ask_float("Price cap (<= this price)", 0.70)
    loop_s   = ask_float("LOOP_SECONDS (0 = single pass)", 0.0)

    try:
        if loop_s <= 0:
            scan_once(folder, bet_size, cap)
        else:
            while True:
                print("\n================= SCAN START =================")
                print(iso_now())
                print("================================================\n")
                scan_once(folder, bet_size, cap)
                print(f"\nSleeping {loop_s:.0f} sec … (Ctrl+C to stop)")
                time.sleep(loop_s)
    except KeyboardInterrupt:
        print("\nbye.")

if __name__ == "__main__":
    main()