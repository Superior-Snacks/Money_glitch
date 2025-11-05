import os, sys, json, time, random, glob, math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------- Constants -----------------
BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"
LOGS_DIR     = "logs"

TRADES_PAGE_LIMIT = 250
LOOP_DEFAULT_SEC  = 3600  # 0 = single pass

# Rate limiting with adaptive scale
RPS_TARGET            = 3.5
_RPS_SCALE            = 1.0
_RPS_MIN              = 0.3
_RPS_RECOVER_PER_SEC  = 0.03

# ----------------- Session + pacing -----------------
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET","POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=40, pool_maxsize=40)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pm-no-bet-scan/2.0"})
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
    if not ra: return 0.0
    try:
        return max(0.0, float(ra))
    except Exception:
        return 0.0

def http_get_with_backoff(url, *, params=None, timeout=20, max_tries=8):
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

# ----------------- Time utils -----------------
def dt_iso():
    return datetime.now(timezone.utc).isoformat()

def _parse_iso_to_epoch(s: str) -> Optional[int]:
    try:
        return int(datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc).timestamp())
    except Exception:
        return None

def _to_epoch_any(x):
    try:
        f = float(x)
        return int(f/1000) if f > 1e12 else int(f)
    except Exception:
        return None

# ----------------- FS helpers -----------------
def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def atomic_write_text(path: str, text: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)

# ----------------- Read rolling market logs -----------------
def read_unique_markets(folder_name: str) -> Dict[str, dict]:
    """
    Reads all logs/<folder_name>/markets_*.jsonl files,
    returns {conditionId: {"question":..., "time_found":..., "conditionId":...}}
    keeping the EARLIEST time_found per cid.
    """
    out_dir = os.path.join(LOGS_DIR, folder_name)
    paths = sorted(glob.glob(os.path.join(out_dir, "markets_*.jsonl")))
    uniq = {}
    bad = 0
    total = 0
    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    total += 1
                    try:
                        rec = json.loads(line)
                    except Exception:
                        bad += 1
                        continue
                    cid = rec.get("conditionId")
                    q   = rec.get("question")
                    tf  = rec.get("time_found")
                    if not cid or not q or not tf:
                        continue
                    if (cid not in uniq) or (_parse_iso_to_epoch(tf) or 10**18) < (_parse_iso_to_epoch(uniq[cid]["time_found"]) or 10**18):
                        uniq[cid] = {"conditionId": cid, "question": q, "time_found": tf}
        except FileNotFoundError:
            continue
    print(f"[READ] files={len(paths)} rows≈{total} unique_by_cid={len(uniq)} bad_lines={bad}")
    return uniq

# ----------------- Category fetch + filter -----------------
MARKET_META_CACHE: Dict[str, dict] = {}

def fetch_market_meta_by_cid(cid: str) -> dict:
    if not cid:
        return {}
    if cid in MARKET_META_CACHE:
        return MARKET_META_CACHE[cid]
    try:
        r = http_get_with_backoff(BASE_GAMMA, params={"conditionId": cid, "limit": 1}, timeout=15)
        arr = r.json() or []
        if not arr:
            MARKET_META_CACHE[cid] = {}
            return {}
        m = arr[0]
        meta = {
            "category": (m.get("category") or "").strip().lower(),
            "subcategory": (m.get("subcategory") or "").strip().lower(),
            "tags": [str(t).strip().lower() for t in (m.get("tags") or [])],
        }
        MARKET_META_CACHE[cid] = meta
        return meta
    except Exception:
        MARKET_META_CACHE[cid] = {}
        return {}

def should_exclude_market(question: str, cid: str, excluded: List[str]) -> bool:
    if not excluded:
        return False
    tokens = [e.strip().lower() for e in excluded if e.strip()]
    q_low = (question or "").lower()
    if any(tok in q_low for tok in tokens):
        return True
    meta = fetch_market_meta_by_cid(cid)
    fields = [meta.get("category",""), meta.get("subcategory","")] + meta.get("tags", [])
    for tok in tokens:
        if any(tok in f for f in fields):
            return True
    return False

# ----------------- Trades pulling -----------------
def fetch_trades_page_ms(cid: str, limit=TRADES_PAGE_LIMIT, starting_before_s=None, offset=None):
    params = {"market": cid, "sort": "desc", "limit": int(limit)}
    if starting_before_s is not None:
        params["starting_before"] = int(starting_before_s * 1000)  # ms
    if offset is not None:
        params["offset"] = int(offset)
    r = http_get_with_backoff(DATA_TRADES, params=params, timeout=20)
    return r.json() or []

def fetch_all_trades_since(cid: str, since_epoch_s: int, page_limit=TRADES_PAGE_LIMIT):
    out = []
    starting_before = None
    last_len = None

    # timestamp paging first
    for _ in range(10000):
        data = fetch_trades_page_ms(cid, limit=page_limit, starting_before_s=starting_before)
        if not data:
            break
        out.extend(data)
        oldest_ts = min(_to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in data)
        if oldest_ts <= since_epoch_s:
            break
        starting_before = oldest_ts
        if last_len == len(data):
            break
        last_len = len(data)

    # offset fallback
    if (not out) or (min((_to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0) for t in out) > since_epoch_s):
        offset = 0
        for _ in range(2000):
            data = fetch_trades_page_ms(cid, limit=page_limit, offset=offset)
            if not data:
                break
            out.extend(data)
            oldest_ts = min(_to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in data)
            if oldest_ts <= since_epoch_s:
                break
            offset += len(data)

    # de-dup and cut since
    seen, uniq = set(), []
    for t in out:
        key = t.get("id") or (t.get("timestamp"), t.get("price"), t.get("size"), t.get("side"), t.get("outcome"))
        if key in seen: 
            continue
        seen.add(key); uniq.append(t)

    cut = []
    for t in uniq:
        ts = _to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0
        if ts >= since_epoch_s:
            cut.append(t)
    return cut

# ----------------- NO-only accounting -----------------
def analyze_no_trades(trades: List[dict], cap: float, bet_size_dollars: float) -> dict:
    no_trades = []
    for t in trades:
        if str(t.get("outcome","")).lower() != "no":
            continue
        try:
            p = float(t.get("price", 0) or 0.0)
            s = float(t.get("size", 0) or 0.0)
        except Exception:
            continue
        if p <= 0 or s <= 0:
            continue
        no_trades.append((p, s))

    if not no_trades:
        return {
            "no_trades": 0,
            "lowest_no_px": None,
            "lowest_no_dollars": 0.0,
            "lowest_no_shares": 0.0,
            "under_cap_dollars": 0.0,
            "under_cap_shares": 0.0,
            "under_cap_trades": 0,
            "over_cap_trades": 0,
            "success_fill": False
        }

    lowest_no_px = min(p for p,_ in no_trades)
    low_dollars = sum(p*s for p,s in no_trades if abs(p - lowest_no_px) < 1e-12)
    low_shares  = sum(s    for p,s in no_trades if abs(p - lowest_no_px) < 1e-12)

    under_cap = [(p,s) for (p,s) in no_trades if p <= cap + 1e-12]
    over_cap  = [(p,s) for (p,s) in no_trades if p  > cap + 1e-12]

    under_cap_dollars = sum(p*s for p,s in under_cap)
    under_cap_shares  = sum(s    for p,s in under_cap)

    return {
        "no_trades": len(no_trades),
        "lowest_no_px": round(lowest_no_px, 6),
        "lowest_no_dollars": round(low_dollars, 2),
        "lowest_no_shares": round(low_shares, 6),
        "under_cap_dollars": round(under_cap_dollars, 2),
        "under_cap_shares": round(under_cap_shares, 6),
        "under_cap_trades": len(under_cap),
        "over_cap_trades": len(over_cap),
        "success_fill": (under_cap_dollars >= bet_size_dollars - 1e-9),
    }

# ----------------- Overview helpers -----------------
def mean(xs: List[float]) -> float:
    return sum(xs)/len(xs) if xs else 0.0

def pct(n: int, d: int) -> str:
    return f"{(100.0*n/d):.2f}%" if d else "0.00%"

def print_overview(snapshots: List[dict], bet_size: float, cap: float, skipped: int, errors: int):
    n = len(snapshots)
    succ = sum(1 for s in snapshots if s.get("success_fill"))
    no_no_trades = sum(1 for s in snapshots if s.get("no_trades",0)==0)

    lows = [s["lowest_no_px"] for s in snapshots if s.get("lowest_no_px") is not None]
    under_dollars = [s.get("under_cap_dollars",0.0) for s in snapshots]
    under_shares  = [s.get("under_cap_shares",0.0) for s in snapshots]

    print("\n===== PASS OVERVIEW =====")
    print(f"Markets scanned:      {n}")
    print(f"Skipped (filters):    {skipped}")
    print(f"Errors (fetch/etc):   {errors}")
    print(f"No trades on market:  {no_no_trades}")
    print(f"open markets:         {...}")
    print(f"closed markets:       {...}")
    print(f"Cap:                  {cap}   Bet size: ${bet_size:.2f}")
    print(f"p/l:                  {...}")
    print(f"Success fills:        {succ}  ({pct(succ, n)})")
    print(f"Avg under-cap $:      ${mean(under_dollars):.2f}")
    print(f"Avg under-cap shares: {mean(under_shares):.4f}")
    if lows:
        print(f"Lowest(ever) NO px:   min={min(lows):.4f}  mean={mean(lows):.4f}  max={max(lows):.4f}")
    else:
        print(f"Lowest(ever) NO px:   (no data)")

    # Top 10 by under-cap dollars
    topN = sorted(snapshots, key=lambda s: s.get("under_cap_dollars",0.0), reverse=True)[:10]
    if topN:
        print("\nTop 10 by under-cap dollars:")
        for i, s in enumerate(topN, 1):
            print(f"  {i:>2}. ${s.get('under_cap_dollars',0.0):>10.2f} | {s.get('question','')[:80]}")

# ----------------- Main -----------------
def main():
    folder = input("Folder name under logs/: ").strip()
    try:
        bet_size = float(input("Bet size $ [10.0]: ").strip() or "10.0")
    except:
        bet_size = 10.0
    try:
        cap = float(input("Price cap [0.5]: ").strip() or "0.5")
    except:
        cap = 0.5
    try:
        loop_s = int(input("Loop seconds (0=single pass) [3600]: ").strip() or "3600")
    except:
        loop_s = LOOP_DEFAULT_SEC

    excluded_str = input("Exclude categories/keywords (comma-separated, blank=none): ").strip()
    excluded = [x.strip() for x in excluded_str.split(",") if x.strip()]

    out_dir = os.path.join(LOGS_DIR, folder)
    ensure_dir(out_dir)
    snap_path = os.path.join(out_dir, "no_bet_snapshots.jsonl")

    print(f"{dt_iso()} Starting scan…")

    while True:
        uniq = read_unique_markets(folder)
        print(f"\n=== NO bet backtest (folder={folder}) | markets={len(uniq)} ===")

        snapshots: List[dict] = []
        skipped = 0
        errors = 0

        for i, (cid, meta) in enumerate(uniq.items(), 1):
            q = meta["question"]
            since_epoch = _parse_iso_to_epoch(meta["time_found"]) or 0

            if excluded and should_exclude_market(q, cid, excluded):
                skipped += 1
                if i % 50 == 0 or len(uniq) <= 50:
                    print(f"[{i}/{len(uniq)}] SKIP → {q}")
                continue

            print(f"[{i}/{len(uniq)}] {q}  (cid={cid[:10]}…)  since={meta['time_found']}")
            try:
                trades = fetch_all_trades_since(cid, since_epoch, page_limit=TRADES_PAGE_LIMIT)
            except Exception as e:
                errors += 1
                print(f"  [WARN trades] {e}")
                continue

            stats = analyze_no_trades(trades, cap=cap, bet_size_dollars=bet_size)

            # Per-market printout
            print(f"    lowest NO px = {stats['lowest_no_px']}  "
                  f"(dollars={stats['lowest_no_dollars']}, shares={stats['lowest_no_shares']})")
            print(f"    under cap (<= {cap}) → dollars={stats['under_cap_dollars']}  "
                  f"shares={stats['under_cap_shares']}  trades={stats['under_cap_trades']}")
            print(f"    over  cap (>  {cap}) → trades={stats['over_cap_trades']}")
            print(f"    SUCCESS @ ${bet_size}: {'YES' if stats['success_fill'] else 'NO'}")

            snapshot = {
                "ts": dt_iso(),
                "folder": folder,
                "conditionId": cid,
                "question": q,
                "time_found": meta["time_found"],
                "cap": cap,
                "bet_size": bet_size,
                "excluded_filters": excluded,
                **stats
            }
            snapshots.append(snapshot)

        # ---------- Rewrite snapshot atomically (no growth over time) ----------
        text = "\n".join(json.dumps(s, ensure_ascii=False) for s in snapshots) + ("\n" if snapshots else "")
        atomic_write_text(snap_path, text)
        print(f"\n[WRITE] {len(snapshots)} snapshot rows → {snap_path} (rewritten this pass)")

        # ---------- Overview ----------
        print_overview(snapshots, bet_size=bet_size, cap=cap, skipped=skipped, errors=errors)

        if loop_s <= 0:
            break
        print(f"\nSleeping {loop_s}s… (Ctrl+C to stop)")
        try:
            time.sleep(loop_s)
        except KeyboardInterrupt:
            print("\nbye.")
            break

if __name__ == "__main__":
    main()