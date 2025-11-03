import os, sys, json, time, glob, random
from typing import Optional, Iterable, Tuple, List, Dict
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Config / endpoints
# =========================
BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"

LOGS_ROOT    = "logs"
SNAPSHOT_NAME= "no_bet_snapshots.jsonl"

# pacing
MARKET_REFRESH_SEC    = 5*60
SLEEP_BETWEEN_MARKETS = 0.0
SLEEP_AFTER_FULL_PASS = 5.0

# RPS (gentle)
RPS_TARGET = 3.0
_last_tokens_ts = time.monotonic()
_bucket = RPS_TARGET

def _rate_limit():
    global _last_tokens_ts, _bucket
    now = time.monotonic()
    _bucket = min(RPS_TARGET, _bucket + (now - _last_tokens_ts) * RPS_TARGET)
    _last_tokens_ts = now
    if _bucket < 1.0:
        need = (1.0 - _bucket) / RPS_TARGET
        time.sleep(need)
        now2 = time.monotonic()
        _bucket = min(RPS_TARGET, _bucket + (now2 - _last_tokens_ts) * RPS_TARGET)
        _last_tokens_ts = now2
    _bucket -= 1.0

def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.4,
        status_forcelist=(429,500,502,503,504),
        allowed_methods=("GET","POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pm-no-bet-rolling/1.0"})
    return s

SESSION = make_session()

# =========================
# Small utils
# =========================
def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def to_epoch_seconds(x) -> Optional[int]:
    """Accepts epoch s/ms, or ISO8601 string; returns epoch seconds or None."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        v = float(x)
        return int(v/1000) if v > 1e12 else int(v)
    if isinstance(x, str):
        s = x.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s).astimezone(timezone.utc)
            return int(dt.timestamp())
        except Exception:
            # maybe numeric string
            try:
                v = float(s)
                return int(v/1000) if v > 1e12 else int(v)
            except Exception:
                return None
    return None

def append_jsonl(path: str, record: dict):
    ensure_dir(os.path.dirname(path))
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

# =========================
# Robust market-log reader
# =========================
def _extract_markets_from_obj(obj) -> Iterable[dict]:
    """Yield markets from obj that may be a single dict, a list, or wrapped."""
    if obj is None:
        return
    if isinstance(obj, dict):
        if isinstance(obj.get("markets"), list):
            for m in obj["markets"]:
                if isinstance(m, dict): yield m
            return
        data = obj.get("data")
        if isinstance(data, dict) and isinstance(data.get("markets"), list):
            for m in data["markets"]:
                if isinstance(m, dict): yield m
            return
        # assume obj itself is a market row
        yield obj
        return
    if isinstance(obj, list):
        for m in obj:
            if isinstance(m, dict):
                yield m

def read_market_logs_rolling(folder: str) -> List[dict]:
    """
    Read all markets_*.jsonl in logs/<folder>/ (and logs/<folder>/marketfiles/ if present).
    Return a list of market dicts (deduped by 'id').
    """
    patterns = [
        os.path.join(LOGS_ROOT, folder, "markets_*.jsonl"),
        os.path.join(LOGS_ROOT, folder, "marketfiles", "markets_*.jsonl"),
    ]
    paths = []
    for pat in patterns:
        paths.extend(glob.glob(pat))
    paths = sorted(set(paths))  # unique + sorted

    out = []
    bad = 0
    if not paths:
        print(f"[READ] No files found. Looked for:\n  - {patterns[0]}\n  - {patterns[1]}")
        return out

    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    obj = json.loads(line)
                except Exception:
                    bad += 1
                    continue
                for m in _extract_markets_from_obj(obj):
                    if not isinstance(m, dict): continue
                    # normalize id
                    if "id" not in m and "market_id" in m:
                        m["id"] = m.get("market_id")
                    out.append(m)

    # Deduplicate by 'id' (last seen row wins)
    dedup: Dict[str, dict] = {}
    for m in out:
        mid = str(m.get("id") or "")
        if mid:
            dedup[mid] = m

    rows = list(dedup.values())
    print(f"[READ] files={len(paths)} rows≈{len(out)} unique_by_id={len(rows)} bad_lines={bad}")
    return rows

def infer_time_found(m: dict) -> int:
    """Prefer 'time_found'; fallback to created/start; else 0."""
    for k in ("time_found", "time_found_at"):
        tf = to_epoch_seconds(m.get(k))
        if tf is not None: return tf
    for k in ("createdAt", "startDate", "openDate", "listedAt"):
        tf = to_epoch_seconds(m.get(k))
        if tf is not None: return tf
    return 0

# =========================
# Gamma helpers
# =========================
def fetch_markets_by_ids(ids: List[str]) -> Dict[str, dict]:
    """
    Best-effort resolver: Gamma supports filtering by 'ids' (comma-separated).
    We’ll chunk to be safe. Returns {id: market_dict}.
    """
    out: Dict[str, dict] = {}
    chunk = 100
    for i in range(0, len(ids), chunk):
        part = ids[i:i+chunk]
        params = {"ids": ",".join(part)}
        _rate_limit()
        r = SESSION.get(BASE_GAMMA, params=params, timeout=20)
        if r.status_code >= 400:
            continue
        data = r.json() or []
        for m in data:
            mid = str(m.get("id") or "")
            if mid:
                out[mid] = m
    return out

def resolve_condition_ids(markets: List[dict], cache: Dict[str, str]) -> Dict[str, str]:
    """
    Return mapping market_id -> conditionId (using cache, embedded fields, or Gamma lookup).
    """
    result = dict(cache) if cache else {}
    missing: List[str] = []

    for m in markets:
        mid = str(m.get("id") or "")
        if not mid: continue
        if mid in result and result[mid]:
            continue
        cid = m.get("conditionId")
        if cid:
            result[mid] = cid
        else:
            missing.append(mid)

    if missing:
        looked = fetch_markets_by_ids(missing)
        for mid in missing:
            cid = (looked.get(mid) or {}).get("conditionId")
            if cid:
                result[mid] = cid
    return result

# simple JSON cache for (market id -> conditionId)
def cache_path(folder: str) -> str:
    return os.path.join(LOGS_ROOT, folder, "id_to_conditionId.json")

def load_cid_cache(folder: str) -> Dict[str, str]:
    p = cache_path(folder)
    if os.path.exists(p):
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cid_cache(folder: str, mapping: Dict[str, str]):
    p = cache_path(folder)
    ensure_dir(os.path.dirname(p))
    with open(p, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False)

# =========================
# Trades (NO only) helpers
# =========================
def fetch_trades_page_ms(cid: str, limit=250, starting_before_s=None, offset=None) -> list:
    params = {"market": cid, "sort": "desc", "limit": int(limit)}
    if starting_before_s is not None:
        params["starting_before"] = int(starting_before_s * 1000)  # milliseconds
    if offset is not None:
        params["offset"] = int(offset)
    _rate_limit()
    r = SESSION.get(DATA_TRADES, params=params, timeout=20)
    r.raise_for_status()
    return r.json() or []

def backfill_trades_since(cid: str, since_epoch_s: int, page_limit=250, max_pages=2000) -> list:
    """
    Pull trades backward in time until we reach <= since_epoch_s.
    Then filter to trades >= since_epoch_s.
    """
    out = []
    starting_before = None
    last_len = None

    # timestamp-first pass
    for _ in range(10000):
        data = fetch_trades_page_ms(cid, limit=page_limit, starting_before_s=starting_before)
        if not data: break
        out.extend(data)
        # oldest on this page
        oldest = min(to_epoch_seconds(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in data)
        if oldest <= since_epoch_s:
            break
        starting_before = oldest
        # progress check
        if last_len == len(data): break
        last_len = len(data)

    # if still not reached since, try offset paging a bit
    if not out or (min(to_epoch_seconds(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in out) > since_epoch_s):
        offset = 0
        for i in range(max_pages):
            data = fetch_trades_page_ms(cid, limit=page_limit, offset=offset)
            if not data: break
            out.extend(data)
            oldest = min(to_epoch_seconds(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in data)
            if oldest <= since_epoch_s:
                break
            offset += len(data)

    # de-dup
    seen, uniq = set(), []
    for t in out:
        key = t.get("id") or (t.get("timestamp"), t.get("price"), t.get("size"), t.get("side"), t.get("outcome"))
        if key in seen: continue
        seen.add(key); uniq.append(t)

    # keep only trades at/after since
    filtered = []
    for t in uniq:
        ts = to_epoch_seconds(t.get("timestamp") or t.get("time") or t.get("ts"))
        if ts is None: continue
        if ts >= since_epoch_s:
            filtered.append(t)
    return filtered

def analyse_no_trades(trades: list, cap: float) -> Tuple[int,int, Tuple[Optional[float], float], float]:
    """
    Return:
      - under_count
      - over_count
      - (min_no_price, min_no_notional_at_that_price)
      - total_under_notional
    Only counts outcome == 'NO'.
    """
    under = over = 0
    min_px: Optional[float] = None
    min_notional = 0.0
    total_under_notional = 0.0

    for t in trades:
        outcome = str(t.get("outcome") or "").lower()
        if outcome != "no":
            continue
        try:
            p = float(t.get("price", 0.0))
            s = float(t.get("size", 0.0))
        except Exception:
            continue
        if p <= 0 or s <= 0:
            continue

        # min price tracking (sum notional at that exact min)
        if (min_px is None) or (p < min_px - 1e-12):
            min_px = p
            min_notional = p * s
        elif (min_px is not None) and abs(p - min_px) <= 1e-12:
            min_notional += p * s

        # under/over cap
        if p <= cap + 1e-12:
            under += 1
            total_under_notional += p * s
        else:
            over += 1

    return under, over, (min_px, min_notional), total_under_notional

def compute_fill_from_under_notional(total_under_notional: float, cap: float, bet_size: float) -> dict:
    """
    Greedy assumption: you get filled at or below cap up to bet_size.
    We don't reconstruct the exact depth ladder here; use total notional as availability.
    """
    filled_dollars = min(bet_size, total_under_notional)
    filled = filled_dollars >= bet_size - 1e-9
    # approximate shares at cap (worst-case within cap)
    filled_shares = filled_dollars / cap if cap > 0 else 0.0
    vwap = cap if filled_shares > 0 else None
    return {
        "filled": filled,
        "filled_dollars": round(filled_dollars, 2),
        "filled_shares": round(filled_shares, 6),
        "vwap": vwap
    }

# =========================
# Main scan
# =========================
def scan_once(folder: str, bet_size: float, cap: float):
    out_dir = os.path.join(LOGS_ROOT, folder)
    ensure_dir(out_dir)
    snap_path = os.path.join(out_dir, SNAPSHOT_NAME)

    # read all rolling daily files
    markets = read_market_logs_rolling(folder)
    if not markets:
        print("[SCAN] No markets found — nothing to do.")
        return

    # resolve conditionIds
    cid_cache = load_cid_cache(folder)
    cid_map = resolve_condition_ids(markets, cid_cache)
    save_cid_cache(folder, cid_map)

    print(f"\n=== NO bet backtest (folder={folder}) | markets={len(markets)} ===")

    for i, m in enumerate(markets, 1):
        mid = str(m.get("id") or "")
        q   = m.get("question") or m.get("name") or m.get("title") or "(no title)"
        if not mid:
            if i % 50 == 0:
                print(f"[{i}/{len(markets)}] skip: missing market id")
            continue

        cid = cid_map.get(mid)
        if not cid:
            print(f"[{i}/{len(markets)}] SKIP: cannot resolve conditionId for id={mid}")
            continue

        tf = infer_time_found(m)
        tf_iso = datetime.fromtimestamp(tf, tz=timezone.utc).isoformat()

        print(f"\n[{i}/{len(markets)}] {cid} | {q}")
        print(f"  time_found: {tf_iso}  cap={cap}  bet=${bet_size:.2f}")

        # fetch trades since time_found
        trades = backfill_trades_since(cid, tf)
        # analyse NO-only trades against cap
        under, over, (min_px, min_not), total_under_notional = analyse_no_trades(trades, cap)

        # print per-market quick summary
        print(f"  trades_NO: under_cap={under} over_cap={over}")
        if min_px is not None:
            print(f"  lowest_NO: {min_px:.4f}  notional_at_lowest=${min_not:.2f}")
        else:
            print("  lowest_NO: N/A")
        print(f"  total_under_cap_notional=${total_under_notional:.2f}")

        # compute fill decision from total_under_notional
        fill = compute_fill_from_under_notional(total_under_notional, cap, bet_size)
        print(f"  fill: {'YES' if fill['filled'] else 'NO'} "
              f"spent=${fill['filled_dollars']:.2f} shares={fill['filled_shares']:.4f} "
              f"vwap={fill['vwap'] if fill['vwap'] is not None else 'N/A'}")

        # snapshot row
        row = {
            "time_logged": iso_now(),
            "market_id": mid,
            "conditionId": cid,
            "question": q,
            "time_found": tf_iso,
            "bet_size": bet_size,
            "cap": cap,
            "no_trades_under_cap": under,
            "no_trades_over_cap": over,
            "lowest_no_price": min_px,
            "lowest_no_notional": round(min_not, 2),
            "total_under_cap_notional": round(total_under_notional, 2),
            **fill
        }
        append_jsonl(snap_path, row)
        time.sleep(SLEEP_BETWEEN_MARKETS)

    print(f"\nSnapshots saved to {snap_path}")

# =========================
# CLI
# =========================
def prompt_float(prompt: str, default: float) -> float:
    s = input(f"{prompt} [{default}]: ").strip()
    if not s:
        return float(default)
    return float(s)

def prompt_int(prompt: str, default: int) -> int:
    s = input(f"{prompt} [{default}]: ").strip()
    if not s:
        return int(default)
    return int(s)

def main():
    folder = input("Folder name under logs/: ").strip()
    if not folder:
        print("Need a folder name (under logs/).")
        sys.exit(1)

    bet_size = prompt_float("Bet size $", 10.0)
    cap      = prompt_float("Price cap", 0.50)
    loop_s   = prompt_int("Loop seconds (0=single pass)", 0)

    print(f"{iso_now()} Starting scan…")
    try:
        if loop_s <= 0:
            scan_once(folder, bet_size, cap)
        else:
            while True:
                scan_once(folder, bet_size, cap)
                print(f"Sleeping {loop_s}s… (Ctrl+C to stop)")
                time.sleep(loop_s)
    except KeyboardInterrupt:
        print("\nbye.")

if __name__ == "__main__":
    main()