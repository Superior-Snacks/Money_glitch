import argparse, json, os, glob, time, random, csv
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Iterable, List, Set
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"

# ----------------- Small time helpers -----------------
def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()

def parse_ts_any(v) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            s = float(v)
            if s > 1e12:
                s = s / 1000.0
            return datetime.fromtimestamp(s, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return None
    return None

def to_epoch_s(dt: Optional[datetime]) -> Optional[int]:
    if not dt: return None
    return int(dt.timestamp())

# ----------------- HTTP session w/ backoff -----------------
def make_session() -> requests.Session:
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
    s.headers.update({"User-Agent": "pm-hourly-closed-scan/1.1"})
    return s

SESSION = make_session()
RPS_TARGET = 3.0
_last = time.monotonic()
_bucket = RPS_TARGET

def rate_limit():
    global _last, _bucket
    now = time.monotonic()
    _bucket = min(RPS_TARGET, _bucket + (now - _last) * RPS_TARGET)
    _last = now
    if _bucket < 1.0:
        need = (1.0 - _bucket) / RPS_TARGET
        time.sleep(need)
        now2 = time.monotonic()
        _bucket = min(RPS_TARGET, _bucket + (now2 - _last) * RPS_TARGET)
        _last = now2
    _bucket -= 1.0

def http_get(url: str, params=None, timeout=20) -> requests.Response:
    back = 0.6
    for attempt in range(10):
        rate_limit()
        r = SESSION.get(url, params=params or {}, timeout=timeout)
        if r.status_code < 400:
            return r
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            extra = float(ra) if (ra and ra.isdigit()) else 0.0
            time.sleep(max(back, extra) + random.random()*0.3)
            back = min(back*1.7, 30.0)
            continue
        if 500 <= r.status_code < 600:
            time.sleep(back + random.random()*0.2)
            back = min(back*1.7, 30.0)
            continue
        r.raise_for_status()
    r.raise_for_status()
    return r

# ----------------- Market metadata helpers -----------------
def extract_winner(m: Dict[str, Any]) -> Optional[str]:
    for k in ("winningOutcome","resolvedOutcome","winner","resolveOutcome","resolution","outcome"):
        v = m.get(k)
        if isinstance(v, str) and v.strip().lower() in ("yes","no"):
            return v.strip().upper()
    res = m.get("result") or m.get("resolutionData") or {}
    if isinstance(res, dict):
        for k in ("winner","winningOutcome","outcome"):
            v = res.get(k)
            if isinstance(v, str) and v.strip().lower() in ("yes","no"):
                return v.strip().upper()
    return None

def is_closed(m: Dict[str,Any]) -> bool:
    if m.get("closed") is True:
        return True
    return extract_winner(m) is not None

def resolved_time(m: Dict[str,Any]) -> Optional[datetime]:
    for k in ("resolvedTime","resolveTime","resolutionTime","settledTime","closedTime","endDate","closeTime"):
        dt = parse_ts_any(m.get(k))
        if dt: return dt
    return None

def fetch_market_by_condition_id(cid: str) -> Optional[Dict[str,Any]]:
    try:
        r = http_get(BASE_GAMMA, params={"conditionIds": cid, "limit": 1})
        arr = r.json() or []
        if isinstance(arr, list) and arr:
            return arr[0]
    except Exception:
        pass
    return None

def fetch_market_by_id(mid: str) -> Optional[Dict[str,Any]]:
    try:
        r = http_get(BASE_GAMMA, params={"ids": mid, "limit": 1})
        arr = r.json() or []
        if isinstance(arr, list) and arr:
            return arr[0]
    except Exception:
        pass
    # small fallback scan
    try:
        for off in (0, 250, 500, 750):
            r = http_get(BASE_GAMMA, params={"limit": 250, "offset": off, "enableOrderBook": False})
            arr = r.json() or []
            for m in arr:
                if str(m.get("id")) == str(mid):
                    return m
            if len(arr) < 250:
                break
    except Exception:
        pass
    return None

# ----------------- Trades pulling -----------------
def _to_epoch_from_trade_ts(v) -> Optional[int]:
    try:
        if v is None: return None
        f = float(v)
        return int(f/1000) if f > 1e12 else int(f)
    except Exception:
        return None

def fetch_trades_after(cid: str, since_epoch_s: int, limit=250, max_pages=60) -> Iterable[Dict[str,Any]]:
    starting_before = None
    pages = 0
    while pages < max_pages:
        params = {"market": cid, "sort": "desc", "limit": int(limit)}
        if starting_before is not None:
            params["starting_before"] = int(starting_before * 1000)  # ms
        r = http_get(DATA_TRADES, params=params, timeout=20)
        arr = r.json() or []
        if not arr:
            break
        oldest_ts = None
        for t in arr:
            ts = _to_epoch_from_trade_ts(t.get("timestamp") or t.get("time") or t.get("ts"))
            if ts is None:
                continue
            if ts >= since_epoch_s:
                yield t
            oldest_ts = ts if (oldest_ts is None or ts < oldest_ts) else oldest_ts
        if oldest_ts is None or oldest_ts <= since_epoch_s:
            break
        starting_before = oldest_ts
        pages += 1

# ----------------- Core analysis -----------------
def analyze_market(row: Dict[str,Any], cap: float) -> Dict[str,Any]:
    time_found = parse_ts_any(row.get("time_found") or row.get("found_at") or row.get("created_at"))
    if not time_found:
        return {"status":"unknown","note":"missing_time_found"}

    cid = row.get("conditionId")
    meta = fetch_market_by_condition_id(cid) if cid else None
    if not meta:
        mid = str(row.get("id") or "")
        if mid:
            meta = fetch_market_by_id(mid)
    if not meta:
        return {"status":"unknown","note":"metadata_lookup_failed","question":row.get("question"),"conditionId":cid or None,"market_id":row.get("id")}

    question = meta.get("question") or row.get("question") or ""
    cond_id  = meta.get("conditionId") or cid
    status_closed = is_closed(meta)
    win = extract_winner(meta)  # "YES"|"NO"|None
    res_dt = resolved_time(meta)

    since_s = to_epoch_s(time_found)
    had_fill_candidate = False
    for t in fetch_trades_after(cond_id, since_s):
        try:
            price = float(t.get("price", 0))
            size  = float(t.get("size", 0))
            outcome = str(t.get("outcome","")).lower()
        except Exception:
            continue
        if price <= 0 or size <= 0:
            continue
        if outcome == "no" and price <= cap:
            had_fill_candidate = True
            break

    wl = None
    pl_per_share = None
    if had_fill_candidate and status_closed and win in ("YES","NO"):
        if win == "NO":
            wl = 1
            pl_per_share = 1.0 - cap
        else:
            wl = 0
            pl_per_share = -cap

    return {
        "market_id": str(meta.get("id") or row.get("id") or ""),
        "conditionId": cond_id,
        "question": question,
        "time_found": time_found.isoformat(),
        "status": "closed" if status_closed else "open",
        "winner": win,
        "resolved_at": res_dt.isoformat() if res_dt else None,
        "cap": cap,
        "had_no_trades_at_or_under_cap_after_time_found": bool(had_fill_candidate),
        "wl": wl,
        "pl_per_share": None if pl_per_share is None else round(pl_per_share, 4),
    }

# ----------------- Dedup (idempotency) -----------------
def load_processed_ids(closed_path: str) -> Set[str]:
    seen: Set[str] = set()
    if not os.path.exists(closed_path):
        return seen
    with open(closed_path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try:
                rec = json.loads(line)
                mid = str(rec.get("market_id") or "")
                if mid:
                    seen.add(mid)
            except Exception:
                continue
    return seen

# ----------------- Log discovery -----------------
def date_str_days_back(n: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=n)).strftime("%Y-%m-%d")

def discover_logs(log_dir: str, log_base: str, days_back: int) -> List[str]:
    name, ext = os.path.splitext(log_base)
    files: List[str] = []
    for back in range(days_back+1):
        day = date_str_days_back(back)
        pattern1 = os.path.join(log_dir, f"{name}_{day}{ext}")
        pattern2 = os.path.join(log_dir, f"{name}_{day}_part*{ext}")
        files.extend(glob.glob(pattern1))
        files.extend(glob.glob(pattern2))
    return sorted(set(files))

def iter_jsonl(paths: List[str]):
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if not line: continue
                try:
                    yield json.loads(line)
                except Exception:
                    continue

# ----------------- Timeseries writers -----------------
def append_locked_ts_json(jsonl_path: str, row: Dict[str,Any]):
    with open(jsonl_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

def append_locked_ts_csv(csv_path: str, row: Dict[str,Any]):
    header = ["ts","locked_dollars","bet_size","open_fill_count","closed_written","rows_scanned","logs_scanned"]
    file_exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not file_exists:
            w.writeheader()
        w.writerow({
            "ts": row["ts"],
            "locked_dollars": row["locked_dollars"],
            "bet_size": row["bet_size"],
            "open_fill_count": row["open_fill_count"],
            "closed_written": row["closed_written"],
            "rows_scanned": row["rows_scanned"],
            "logs_scanned": row["logs_scanned"],
        })

# ----------------- Main -----------------
def main():
    ap = argparse.ArgumentParser(description="Hourly scan: write closed markets & locked dollars timeseries.")
    ap.add_argument("--log-dir", required=True, help="Directory where rotated logs live.")
    ap.add_argument("--log-base", required=True, help="Base file name used by writer (e.g., markets.jsonl).")
    ap.add_argument("--days-back", type=int, default=1, help="How many days back of rotated logs to scan (default: 1).")
    ap.add_argument("--cap", required=True, type=float, help="Cap threshold for NO price (e.g., 0.70).")
    ap.add_argument("--bet-size", required=True, type=float, help="Per-market bet size to compute 'dollars in market'.")
    ap.add_argument("--out", default="closed_markets.jsonl", help="Output JSONL for closed market results.")
    ap.add_argument("--ts-json", default="locked_dollars_timeseries.jsonl", help="Timeseries JSONL output path.")
    ap.add_argument("--ts-csv",  default="locked_dollars_timeseries.csv",  help="Timeseries CSV output path.")
    args = ap.parse_args()

    log_paths = discover_logs(args.log_dir, args.log_base, max(0, int(args.days_back)))
    if not log_paths:
        print(f"No logs found in {args.log_dir} for base {args.log_base} (days_back={args.days_back}).")
        return

    processed_ids = load_processed_ids(args.out)
    wrote = 0
    total = 0
    open_markets_with_fill = 0
    closed_markets_scanned = 0
    open_markets_scanned = 0

    with open(args.out, "a", encoding="utf-8") as out:
        for row in iter_jsonl(log_paths):
            total += 1
            stable_key = str(row.get("conditionId") or row.get("id") or "")
            if not stable_key:
                continue

            res = analyze_market(row, float(args.cap))
            status = res.get("status")

            if status == "closed":
                closed_markets_scanned += 1
            elif status == "open":
                open_markets_scanned += 1

            if status == "open" and res.get("had_no_trades_at_or_under_cap_after_time_found"):
                open_markets_with_fill += 1

            if status == "closed":
                mid = str(res.get("market_id") or "")
                if mid and mid not in processed_ids:
                    out.write(json.dumps(res, ensure_ascii=False) + "\n")
                    processed_ids.add(mid)
                    wrote += 1

    locked_dollars = float(args.bet_size) * open_markets_with_fill

    # persist timeseries
    ts_row = {
        "ts": utcnow_iso(),
        "locked_dollars": round(locked_dollars, 2),
        "bet_size": float(args.bet_size),
        "open_fill_count": int(open_markets_with_fill),
        "closed_written": int(wrote),
        "rows_scanned": int(total),
        "logs_scanned": len(log_paths),
    }
    append_locked_ts_json(args.ts_json, ts_row)
    append_locked_ts_csv(args.ts_csv, ts_row)

    # console summary
    print("===== HOURLY CLOSED SCAN SUMMARY =====")
    print(f"When:                  {ts_row['ts']}")
    print(f"Scanned rows:          {total}")
    print(f"Logs scanned:          {len(log_paths)}")
    print(f"Closed markets seen:   {closed_markets_scanned}")
    print(f"Open markets seen:     {open_markets_scanned}")
    print(f"Wrote new closed rows: {wrote}  → {args.out}")
    print(f"Open markets w/fill:   {open_markets_with_fill}")
    print(f"Dollars in market:     ${locked_dollars:,.2f}   (bet_size=${float(args.bet_size):.2f} × {open_markets_with_fill})")
    print(f"Timeseries → JSONL:    {args.ts_json}")
    print(f"Timeseries → CSV:      {args.ts_csv}")

if __name__ == "__main__":
    main()