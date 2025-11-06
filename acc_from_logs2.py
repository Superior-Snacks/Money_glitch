import os, sys, json, time, random, glob
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
    s.headers.update({"User-Agent": "pm-no-bet-scan/2.1"})
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

def _parse_iso_to_epoch(s: Optional[str]) -> Optional[int]:
    if not s: return None
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
                    old_tf = _parse_iso_to_epoch(uniq.get(cid, {}).get("time_found"))
                    new_tf = _parse_iso_to_epoch(tf)
                    if cid not in uniq or (new_tf or 10**18) < (old_tf or 10**18):
                        uniq[cid] = {"conditionId": cid, "question": q, "time_found": tf}
        except FileNotFoundError:
            continue
    print(f"[READ] files={len(paths)} rows≈{total} unique_by_cid={len(uniq)} bad_lines={bad}")
    return uniq

# ----------------- Market meta / status -----------------
MARKET_META_CACHE: Dict[str, dict] = {}

def fetch_market_full_by_cid(cid: str) -> dict:
    """Fetch the single market row by conditionId."""
    if not cid:
        return {}
    if cid in MARKET_META_CACHE:
        return MARKET_META_CACHE[cid]
    try:
        r = http_get_with_backoff(BASE_GAMMA, params={"conditionId": cid, "limit": 1}, timeout=15)
        arr = r.json() or []
        m = arr[0] if arr else {}
        MARKET_META_CACHE[cid] = m
        return m
    except Exception:
        MARKET_META_CACHE[cid] = {}
        print(f"GOT NO MARKET FROM {cid}")
        return {}

def norm_winner(m: dict) -> Optional[str]:
    for k in ["winningOutcome","resolvedOutcome","winner","resolveOutcome","resolution","outcome"]:
        v = m.get(k)
        if isinstance(v, str) and v.strip().lower() in ("yes","no"):
            return v.strip().upper()
    res = m.get("result") or m.get("resolutionData") or {}
    if isinstance(res, dict):
        for k in ["winner","winningOutcome","outcome"]:
            v = res.get(k)
            if isinstance(v, str) and v.strip().lower() in ("yes","no"):
                return v.strip().upper()
    return None

def resolved_ts_from_market(m: dict) -> Optional[int]:
    for k in ["resolvedTime","resolveTime","resolutionTime","closedTime","endDate","closeTime"]:
        ts = _parse_iso_to_epoch(m.get(k))
        if ts: return ts
    return None

def current_status(m: dict) -> str:
    """Return 'YES', 'NO', or 'TBD'."""
    win = norm_winner(m)
    if win in ("YES","NO"):
        return win
    closed_flag = m.get("closed")
    return "TBD" if (closed_flag is not True) else "TBD"
"""
this seems to not be working !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
"""

# ----------------- Trades pulling -----------------
def fetch_trades_page_ms(cid: str, limit=TRADES_PAGE_LIMIT, starting_before_s=None, offset=None):
    params = {"market": cid, "sort": "desc", "limit": int(limit)}
    if starting_before_s is not None:
        params["starting_before"] = int(starting_before_s * 1000)  # ms
    if offset is not None:
        params["offset"] = int(offset)
    r = http_get_with_backoff(DATA_TRADES, params=params, timeout=20)
    return r.json() or []

def fetch_all_trades_since(cid: str, since_epoch_s: int, page_limit=TRADES_PAGE_LIMIT) -> List[dict]:
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

    # de-dup and cut since; then sort ASC by timestamp
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
    cut.sort(key=lambda t: _to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0)
    return cut

# ----------------- NO-only fill + stats -----------------
def try_fill_no_from_trades(trades: List[dict], cap: float, bet_size_dollars: float) -> dict:
    """
    Ascending timestamp. Accumulate NO trades at/under cap until bet dollars reached.
    Returns fill_time/avg_px/shares/cost + flags for 'had_any_trades' and 'no_trades_zero'.
    """
    had_any_trades = bool(trades)

    # collect NO-only trades (ts, price, size)
    no_trades = []
    for t in trades:
        if str(t.get("outcome","")).lower() != "no":
            continue
        p = float(t.get("price") or 0.0)
        s = float(t.get("size") or 0.0)
        if p <= 0 or s <= 0:
            continue
        ts = _to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0
        no_trades.append((ts, p, s))

    if not no_trades:
        return {
            "had_any_trades": had_any_trades,  # False => "non active"
            "no_trades_zero": True,            # zero NO trades
            "success_fill": False,
            "fill_time": None,
            "avg_px": None,
            "shares": 0.0,
            "cost": 0.0,
            "lowest_no_px": None,
            "under_cap_dollars": 0.0,
            "under_cap_shares": 0.0,
            "under_cap_trades": 0,
            "over_cap_trades": 0,
        }

    no_trades.sort(key=lambda x: x[0])  # by ts
    lowest_no_px = min(p for _, p, _ in no_trades)

    under = [(ts,p,s) for (ts,p,s) in no_trades if p <= cap + 1e-12]
    over  = [(ts,p,s) for (ts,p,s) in no_trades if p  > cap + 1e-12]

    under_cap_dollars = sum(p*s for _,p,s in under)
    under_cap_shares  = sum(    s for _,p,s in under)

    # Try to fill
    cum_dollars = 0.0
    vwap_num = 0.0
    shares = 0.0
    fill_time = None

    for ts, p, s in under:
        dollars = p * s
        need = max(0.0, bet_size_dollars - cum_dollars)
        take_dollars = min(dollars, need)
        if take_dollars > 0:
            take_shares = take_dollars / p
            vwap_num += p * take_shares
            shares += take_shares
            cum_dollars += take_dollars
            if cum_dollars >= bet_size_dollars - 1e-9:
                fill_time = ts
                break

    if fill_time is None:
        return {
            "had_any_trades": had_any_trades,
            "no_trades_zero": False,
            "success_fill": False,
            "fill_time": None,
            "avg_px": None,
            "shares": round(shares, 6),
            "cost": round(cum_dollars, 2),
            "lowest_no_px": round(lowest_no_px, 6),
            "under_cap_dollars": round(under_cap_dollars, 2),
            "under_cap_shares": round(under_cap_shares, 6),
            "under_cap_trades": len(under),
            "over_cap_trades": len(over),
        }

    avg_px = vwap_num / shares if shares > 0 else None
    return {
        "had_any_trades": had_any_trades,
        "no_trades_zero": False,
        "success_fill": True,
        "fill_time": fill_time,
        "avg_px": round(avg_px, 6) if avg_px is not None else None,
        "shares": round(shares, 6),
        "cost": round(bet_size_dollars, 2),
        "lowest_no_px": round(lowest_no_px, 6),
        "under_cap_dollars": round(under_cap_dollars, 2),
        "under_cap_shares": round(under_cap_shares, 6),
        "under_cap_trades": len(under),
        "over_cap_trades": len(over),
    }

# ----------------- Overview helpers -----------------
def mean(xs: List[float]) -> float:
    return sum(xs)/len(xs) if xs else 0.0

def pct(n: int, d: int) -> str:
    return f"{(100.0*n/d):.2f}%" if d else "0.00%"

def print_overview(snapshots: List[dict], bet_size: float, cap: float, skipped: int, errors: int):
    n = len(snapshots)
    succ = sum(1 for s in snapshots if s.get("success_fill"))
    non_active = sum(1 for s in snapshots if s.get("had_any_trades") is False)          # zero trades total
    no_trades_on_no = sum(1 for s in snapshots if s.get("no_trades_zero") is True)      # had trades but none on NO

    lows = [s["lowest_no_px"] for s in snapshots if s.get("lowest_no_px") is not None]
    under_dollars = [s.get("under_cap_dollars",0.0) for s in snapshots]
    under_shares  = [s.get("under_cap_shares",0.0) for s in snapshots]

    open_count = sum(1 for s in snapshots if s.get("status") == "TBD")
    closed_count = sum(1 for s in snapshots if s.get("status") in ("YES","NO"))

    total_pl = sum(s.get("pl", 0.0) for s in snapshots if s.get("pl") is not None)

    print("\n===== PASS OVERVIEW =====")
    print(f"Markets scanned:      {n}")
    print(f"Skipped (filters):    {skipped}")
    print(f"Errors (fetch/etc):   {errors}")
    print(f"non active markets:   {non_active}")
    print(f"Open markets:         {open_count}")
    print(f"Closed markets:       {closed_count}")
    print(f"Cap:                  {cap}   Bet size: ${bet_size:.2f}")
    print(f"Total realized P/L:   {total_pl:+.2f}")
    print(f"Success fills:        {succ}  ({(100.0*succ/n):.2f}% if n else 0)")
    print(f"No trades on NO side: {no_trades_on_no}")
    print(f"Avg under-cap $:      ${mean(under_dollars):.2f}")
    print(f"Avg under-cap shares: {mean(under_shares):.4f}")
    if lows:
        print(f"Lowest(ever) NO px:   min={min(lows):.4f}  mean={mean(lows):.4f}  max={max(lows):.4f}")
    else:
        print(f"Lowest(ever) NO px:   (no data)")

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
    excluded = [x.strip().lower() for x in excluded_str.split(",") if x.strip()]

    out_dir = os.path.join(LOGS_DIR, folder)
    ensure_dir(out_dir)
    snap_path   = os.path.join(out_dir, "log_market_snapshots.jsonl")   # rewritten each pass
    closed_path = os.path.join(out_dir, "log_closed_markets.jsonl")     # rewritten each pass

    print(f"{dt_iso()} Starting scan…")

    while True:
        uniq = read_unique_markets(folder)
        print(f"\n=== NO bet backtest (folder={folder}) | markets={len(uniq)} ===")

        snapshots: List[dict] = []
        closed_rows: List[dict] = []
        skipped = 0
        errors = 0

        for i, (cid, meta) in enumerate(uniq.items(), 1):
            q = meta["question"]
            since_epoch = _parse_iso_to_epoch(meta["time_found"]) or 0

            # Category/keyword exclusion (by question text only; extend to metadata if you want)
            if excluded and any(tok in q.lower() for tok in excluded):
                skipped += 1
                if i % 50 == 0 or len(uniq) <= 50:
                    print(f"[{i}/{len(uniq)}] SKIP → {q}")
                continue

            print(f"[{i}/{len(uniq)}] {q}  (cid={cid[:10]}…)  since={meta['time_found']}")
            try:
                m = fetch_market_full_by_cid(cid)
                status = current_status(m)  # 'YES', 'NO', 'TBD'
            except Exception as e:
                errors += 1
                print(f"  [WARN meta] {e}")
                status = "TBD"

            try:
                trades = fetch_all_trades_since(cid, since_epoch, page_limit=TRADES_PAGE_LIMIT)
            except Exception as e:
                errors += 1
                print(f"  [WARN trades] {e}")
                continue

            stats = try_fill_no_from_trades(trades, cap=cap, bet_size_dollars=bet_size)

            # Per-market printout
            print(f"    lowest NO px = {stats['lowest_no_px']}  "
                  f"under_cap$: {stats['under_cap_dollars']}  shares: {stats['under_cap_shares']}  "
                  f"trades<=cap: {stats['under_cap_trades']}  trades>cap: {stats['over_cap_trades']}")
            print(f"    SUCCESS FILL @ ${bet_size}: {'YES' if stats['success_fill'] else 'NO'}"
                  + (f"  avg_px={stats['avg_px']}  fill_time={datetime.fromtimestamp(stats['fill_time'], tz=timezone.utc).isoformat()}" if stats['success_fill'] else ""))

            # Compute realized P/L if resolved and we had a filled position
            pl = None
            if stats["success_fill"] and status in ("YES","NO"):
                cost   = float(stats["cost"])
                shares = float(stats["shares"])
                # NO pays 1 on NO; 0 on YES
                payout = shares * (1.0 if status == "NO" else 0.0)
                pl = round(payout - cost, 2)

            snapshot = {
                "ts": dt_iso(),
                "folder": folder,
                "status": status,                     # 'YES' | 'NO' | 'TBD'
                "conditionId": cid,
                "question": q,
                "time_found": meta["time_found"],
                "cap": cap,
                "bet_size": bet_size,
                # stats from fill attempt
                **stats,
                "pl": pl,                             # realized P/L if resolved and filled, else None
            }
            snapshots.append(snapshot)

            # If market is closed (YES/NO), write a closed row with P/L (may be None if not filled)
            if status in ("YES","NO"):
                closed_row = {
                    "ts": dt_iso(),
                    "conditionId": cid,
                    "question": q,
                    "status": status,
                    "time_found": meta["time_found"],
                    "cap": cap,
                    "bet_size": bet_size,
                    "filled": bool(stats["success_fill"]),
                    "avg_px": stats["avg_px"],
                    "shares": stats["shares"],
                    "cost": stats["cost"],
                    "pl": pl,  # could be negative/positive/None if not filled
                }
                closed_rows.append(closed_row)

        # ---------- Rewrite snapshot atomically (no growth over time) ----------
        snap_text = "\n".join(json.dumps(s, ensure_ascii=False) for s in snapshots)
        atomic_write_text(snap_path, snap_text + ("\n" if snapshots else ""))
        print(f"\n[WRITE] {len(snapshots)} snapshot rows → {snap_path} (rewritten this pass)")

        # ---------- Rewrite closed-markets (with P/L) ----------
        closed_text = "\n".join(json.dumps(s, ensure_ascii=False) for s in closed_rows)
        atomic_write_text(closed_path, closed_text + ("\n" if closed_rows else ""))
        print(f"[WRITE] {len(closed_rows)} closed rows (with P/L) → {closed_path} (rewritten this pass)")

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