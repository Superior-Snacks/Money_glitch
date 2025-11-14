import time
import json
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DATA_TRADES = "https://data-api.polymarket.com/trades"
TRADES_PAGE_LIMIT = 250

# ---------- HTTP session with gentle backoff ----------
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pm-trades-debug/1.0"})
    return s

SESSION = make_session()

def http_get(url, params=None, timeout=20):
    back = 0.5
    while True:
        try:
            r = SESSION.get(url, params=params or {}, timeout=timeout)
        except requests.RequestException as e:
            print(f"[HTTP EXC] {e} → sleeping {back:.1f}s")
            time.sleep(back)
            back = min(back * 1.7, 20.0)
            continue

        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            try:
                wait = float(ra) if ra else back
            except Exception:
                wait = back
            print(f"[429] rate limited → sleeping {wait:.1f}s")
            time.sleep(wait)
            back = min(back * 1.8, 30.0)
            continue

        if 500 <= r.status_code < 600:
            print(f"[{r.status_code}] server error → sleeping {back:.1f}s")
            time.sleep(back)
            back = min(back * 1.7, 20.0)
            continue

        r.raise_for_status()
        return r

def to_epoch(v):
    if v is None:
        return None
    try:
        f = float(v)
        if f > 1e12:  # ms -> s
            f /= 1000.0
        return int(f)
    except Exception:
        try:
            s = str(v).strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return None

def ts_from_trade(t):
    for key in ("match_time", "last_update", "timestamp", "time", "ts"):
        if key in t and t[key] is not None:
            ts = to_epoch(t[key])
            if ts is not None:
                return ts
    return None

def iso(ts):
    if ts is None:
        return "None"
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

def main():
    cid = input("Market (conditionId): ").strip()
    if not cid:
        print("No market id given, exiting.")
        return

    print(f"\n=== FULL TRADE DUMP (offset paging) for {cid} ===")

    offset = 0
    page_no = 0
    total_trades = 0

    earliest = None
    latest = None

    start_time = time.time()

    while True:
        page_no += 1
        params = {
            "market": cid,
            "limit": TRADES_PAGE_LIMIT,
            "offset": offset,
        }
        print(f"\n[PAGE {page_no}] offset={offset}")
        r = http_get(DATA_TRADES, params=params, timeout=30)
        data = r.json() or []

        n = len(data)
        print(f"  → got {n} trades")

        if not data:
            print("  → empty page, stopping.")
            break

        # timestamp stats for this page
        page_ts = []
        for t in data:
            ts = ts_from_trade(t)
            if ts is not None:
                page_ts.append(ts)
                if earliest is None or ts < earliest:
                    earliest = ts
                if latest is None or ts > latest:
                    latest = ts

        if page_ts:
            print(f"  → page ts range: {iso(min(page_ts))} .. {iso(max(page_ts))}")
        else:
            print("  → page ts range: (no valid timestamps)")

        total_trades += n
        offset += n

        if n < TRADES_PAGE_LIMIT:
            print("  → short page, likely end of history.")
            break

    elapsed = time.time() - start_time
    print("\n===== SUMMARY =====")
    print(f"Pages fetched:   {page_no}")
    print(f"Total trades:    {total_trades}")
    print(f"Earliest trade:  {iso(earliest)}")
    print(f"Latest trade:    {iso(latest)}")
    print(f"Elapsed time:    {elapsed:.1f}s")

if __name__ == "__main__":
    main()