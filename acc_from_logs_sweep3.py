import os, sys, json, random, glob, time, csv
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import argparse

# ----------------- Constants -----------------
BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"
LOGS_DIR     = "logs"

TRADES_PAGE_LIMIT = 250
LOOP_DEFAULT_SEC  = 3600  # 0 = single pass

HINT_SPREAD = 0.98                  # decisive price hint barrier
FINAL_GRACE = timedelta(days=2)     # grace after close before trusting prices

CACHE_VERSION = 1  # bump if cache format/semantics change

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
    s.headers.update({"User-Agent": "pm-no-bet-scan/2.6-cached-final+basic"})
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

def _parse_dt_any(v):
    if not v:
        return None
    try:
        if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().isdigit()):
            ts = float(v)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        pass
    try:
        s = str(v).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _iso_or_none(dt):
    return dt.isoformat() if isinstance(dt, datetime) else None

# ----------------- FS helpers -----------------
def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def atomic_write_text(path: str, text: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)

def load_jsonl_by_key(path: str, key: str) -> Dict[str, dict]:
    out = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                k = rec.get(key)
                if k:
                    out[k] = rec
    except FileNotFoundError:
        pass
    return out

# ----------------- Read rolling market logs -----------------
def read_unique_markets(folder_name: str) -> Dict[str, dict]:
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
    if not cid:
        return {}
    if cid in MARKET_META_CACHE and MARKET_META_CACHE[cid]:
        return MARKET_META_CACHE[cid]
    try:
        # gamma expects 'condition_ids' plural with underscore
        r = http_get_with_backoff(BASE_GAMMA, params={"condition_ids": cid, "limit": 1}, timeout=15)
        rows = r.json() or []
        for m in rows:
            if m.get("conditionId") == cid:
                MARKET_META_CACHE[cid] = m
                return m
        MARKET_META_CACHE[cid] = {}
        return {}
    except Exception:
        MARKET_META_CACHE[cid] = {}
        return {}

def resolve_status(m: dict) -> tuple[bool, Optional[str], str]:
    uma = (m.get("umaResolutionStatus") or "").strip().lower()
    if uma in {"yes","no"}:
        return True, uma.upper(), "umaResolutionStatus"
    if uma.startswith("resolved_"):
        w = uma.split("_",1)[1].upper()
        if w in {"YES","NO"}:
            return True, w, "umaResolutionStatus"

    w = (m.get("winningOutcome") or m.get("winner") or "").strip().upper()
    if w in {"YES","NO"}:
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
        prices = json.loads(raw) if isinstance(raw, str) else (raw or ["0", "0"])
        try:
            y, n = float(prices[0]), float(prices[1])
        except Exception:
            y = n = None

        if age_ok and y is not None and n is not None:
            if y >= HINT_SPREAD and n <= 1 - HINT_SPREAD:
                return True, "YES", "terminal_outcomePrices"
            if n >= HINT_SPREAD and y <= 1 - HINT_SPREAD:
                return True, "NO", "terminal_outcomePrices"

        if y is not None and n is not None:
            if y >= 0.90 and n <= 0.10:
                return True, "YES", "closed_price_hint_yes"
            if n >= 0.90 and y <= 0.10:
                return True, "NO", "closed_price_hint_no"

    return False, None, "unresolved"

def current_status(m: dict) -> str:
    resolved, winner, _ = resolve_status(m)
    if resolved and winner in ("YES","NO"):
        return winner
    return "TBD"

def closed_time_iso(m: dict) -> Optional[str]:
    dt = (
        _parse_dt_any(m.get("closedTime"))
        or _parse_dt_any(m.get("umaEndDate"))
        or _parse_dt_any(m.get("endDate"))
        or _parse_dt_any(m.get("updatedAt"))
    )
    return _iso_or_none(dt)

# ----------------- Trades pulling -----------------
def fetch_trades_page_ms(cid: str, limit=TRADES_PAGE_LIMIT, starting_before_s=None, offset=None):
    params = {"market": cid, "sort": "desc", "limit": int(limit)}
    if starting_before_s is not None:
        params["starting_before"] = int(starting_before_s * 1000)
    if offset is not None:
        params["offset"] = int(offset)
    r = http_get_with_backoff(DATA_TRADES, params=params, timeout=20)
    return r.json() or []

def fetch_all_trades_since(cid: str, since_epoch_s: int, page_limit=TRADES_PAGE_LIMIT) -> List[dict]:
    out = []
    starting_before = None
    last_len = None

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

    # de-dup and cut since; sort ASC
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

# ----------------- Multi-cap accumulation -----------------
def _init_caps_state(caps: List[float], max_notional: float):
    return {
        round(cap, 2): {
            "cum_dollars": 0.0,
            "shares": 0.0,
            "fill_time": None,
            "success": False,
            "under_cap_seen": 0.0,
            "under_cap_shares": 0.0,
            "under_cap_trades": 0,
            "over_cap_trades": 0,
        } for cap in caps
    }

def _apply_trades_page_to_caps(trades: List[dict], caps_state: dict, since_epoch_s: int, lowest_holder: dict, max_notional: float):
    any_progress = False
    trades_sorted = sorted(trades, key=lambda t: _to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0)
    for t in trades_sorted:
        ts = _to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0
        if ts < since_epoch_s:
            continue
        if str(t.get("outcome","")).lower() != "no":
            continue
        try:
            p = float(t.get("price") or 0.0)
            s = float(t.get("size")  or 0.0)
        except Exception:
            continue
        if p <= 0 or s <= 0:
            continue

        if lowest_holder["lowest"] is None or p < lowest_holder["lowest"]:
            lowest_holder["lowest"] = p

        for cap, st in caps_state.items():
            if st["success"]:
                continue
            if p <= cap + 1e-12:
                st["under_cap_trades"] += 1
                dollars = p * s
                st["under_cap_seen"] += dollars
                st["under_cap_shares"] += s
                need = max(0.0, max_notional - st["cum_dollars"])
                take = min(need, dollars)
                if take > 0:
                    st["cum_dollars"] += take
                    st["shares"] += (take / p)
                    any_progress = True
                    if st["cum_dollars"] >= max_notional - 1e-9:
                        st["fill_time"] = ts
                        st["success"] = True
            else:
                st["over_cap_trades"] += 1

    all_filled = all(st["success"] for st in caps_state.values())
    return any_progress, all_filled

def fetch_until_caps_filled(cid: str, since_epoch_s: int, caps: List[float], max_notional: float):
    caps_state = _init_caps_state(caps, max_notional)
    lowest_holder = {"lowest": None}
    starting_before = None
    last_page_len = None

    for _ in range(10000):
        data = fetch_trades_page_ms(cid, limit=TRADES_PAGE_LIMIT, starting_before_s=starting_before)
        if not data:
            break
        any_progress, all_filled = _apply_trades_page_to_caps(data, caps_state, since_epoch_s, lowest_holder, max_notional)
        oldest_ts = min(_to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in data)
        if all_filled:
            break
        if oldest_ts <= since_epoch_s:
            break
        starting_before = oldest_ts
        if last_page_len == len(data) and not any_progress:
            break
        last_page_len = len(data)

    caps_out = {}
    for cap, st in caps_state.items():
        avg_px = (st["cum_dollars"] / st["shares"]) if st["shares"] > 1e-12 else None
        caps_out[cap] = {
            "success": st["success"],
            "fill_time": st["fill_time"],
            "avg_px": round(avg_px, 6) if avg_px is not None else None,
            "shares": round(st["shares"], 6),
            "cost": round(st["cum_dollars"], 2),
            "under_cap_dollars_seen": round(st["under_cap_seen"], 2),
            "under_cap_shares": round(st["under_cap_shares"], 6),
            "under_cap_trades": st["under_cap_trades"],
            "over_cap_trades": st["over_cap_trades"],
        }

    return {
        "lowest_no_px": (round(lowest_holder["lowest"], 6) if lowest_holder["lowest"] is not None else None),
        "caps": caps_out
    }

# Build caps from entire trade history (for final closed recompute)
def build_caps_from_all_trades(trades: List[dict], since_epoch_s: int, caps: List[float], max_notional: float):
    caps_state = _init_caps_state(caps, max_notional)
    lowest = None
    # sort asc by ts
    trades_sorted = sorted(trades, key=lambda t: _to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0)
    for t in trades_sorted:
        ts = _to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0
        if ts < since_epoch_s:
            continue
        if str(t.get("outcome","")).lower() != "no":
            continue
        p = float(t.get("price") or 0.0)
        s = float(t.get("size")  or 0.0)
        if p <= 0 or s <= 0:
            continue
        if lowest is None or p < lowest:
            lowest = p
        for cap, st in caps_state.items():
            if st["success"]:
                continue
            if p <= cap + 1e-12:
                st["under_cap_trades"] += 1
                dollars = p * s
                st["under_cap_seen"] += dollars
                st["under_cap_shares"] += s
                need = max(0.0, max_notional - st["cum_dollars"])
                take = min(need, dollars)
                if take > 0:
                    st["cum_dollars"] += take
                    st["shares"] += (take / p)
                    if st["cum_dollars"] >= max_notional - 1e-9:
                        st["fill_time"] = ts
                        st["success"] = True
            else:
                st["over_cap_trades"] += 1

    caps_out = {}
    for cap, st in caps_state.items():
        avg_px = (st["cum_dollars"] / st["shares"]) if st["shares"] > 1e-12 else None
        caps_out[cap] = {
            "success": st["success"],
            "fill_time": st["fill_time"],
            "avg_px": round(avg_px, 6) if avg_px is not None else None,
            "shares": round(st["shares"], 6),
            "cost": round(st["cum_dollars"], 2),
            "under_cap_dollars_seen": round(st["under_cap_seen"], 2),
            "under_cap_shares": round(st["under_cap_shares"], 6),
            "under_cap_trades": st["under_cap_trades"],
            "over_cap_trades": st["over_cap_trades"],
        }
    return {
        "lowest_no_px": (round(lowest, 6) if lowest is not None else None),
        "caps": caps_out
    }

# ----------------- "Basic" 0.40 @ $5 metrics -----------------
def basic_fill_from_trades(trades: List[dict], since_epoch_s: int, cap: float = 0.40, bet: float = 5.0) -> Tuple[Optional[int], int]:
    """
    Returns (fill_time_epoch_or_None, trades_processed_count) for NO trades at/under cap
    until cumulative dollars >= bet. Trades are processed in ascending timestamp.
    """
    cum = 0.0
    count = 0
    fill_ts = None
    for t in sorted(trades, key=lambda x: _to_epoch_any(x.get("timestamp") or x.get("time") or x.get("ts") or 0) or 0):
        ts = _to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0
        if ts < since_epoch_s:  # respect time_found
            continue
        if str(t.get("outcome","")).lower() != "no":
            continue
        p = float(t.get("price") or 0.0)
        s = float(t.get("size")  or 0.0)
        if p <= 0 or s <= 0:
            continue
        if p <= cap + 1e-12:
            count += 1
            dollars = p * s
            need = max(0.0, bet - cum)
            take = min(need, dollars)
            if take > 0:
                cum += take
                if cum >= bet - 1e-9 and fill_ts is None:
                    fill_ts = ts
                    break
    return fill_ts, count

# ----------------- Cache helpers -----------------
def caps_cache_matches(rec: dict, caps: List[float], max_notional: float) -> bool:
    if not rec or not rec.get("caps_cache") or not rec.get("finalized", False):
        return False
    if float(rec.get("max_notional", -1)) != float(max_notional):
        return False
    cached_caps = {float(k): v for k, v in rec["caps_cache"].items()}
    need = set(round(c, 2) for c in caps)
    have = set(round(c, 2) for c in cached_caps.keys())
    return need.issubset(have)

# ----------------- Overview helpers -----------------
def mean(xs: List[float]) -> float:
    return sum(xs)/len(xs) if xs else 0.0

def pct(n: int, d: int) -> str:
    return f"{(100.0*n/d):.2f}%" if d else "0.00%"

def print_view_overview(view_snapshots: List[dict], bet_size: float, cap: float, skipped: int, errors: int):
    n = len(view_snapshots)
    succ = sum(1 for s in view_snapshots if s.get("filled"))
    open_count = sum(1 for s in view_snapshots if s.get("status") == "TBD")
    closed_yes = sum(1 for s in view_snapshots if s.get("status") == "YES")
    closed_no  = sum(1 for s in view_snapshots if s.get("status") == "NO")
    closed_count = closed_yes + closed_no
    total_pl = sum(s.get("pl", 0.0) for s in view_snapshots if s.get("pl") is not None)
    closed_with_fills = sum(1 for s in view_snapshots if s.get("closed_filled") is True)
    closed_with_fills_yes = sum(1 for s in view_snapshots if s.get("closed_filled") is True and s.get("status") == "YES")
    closed_with_fills_no  = sum(1 for s in view_snapshots if s.get("closed_filled") is True and s.get("status") == "NO")

    lows = [s["lowest_no_px"] for s in view_snapshots if s.get("lowest_no_px") is not None]
    under_dollars = [s.get("under_cap_dollars_seen",0.0) for s in view_snapshots]
    under_shares  = [s.get("under_cap_shares",0.0) for s in view_snapshots]

    print("\n===== PASS OVERVIEW (view cap/bet) =====")
    print(f"Markets scanned:           {n}")
    print(f"Skipped (filters):         {skipped}")
    print(f"Errors (fetch/etc):        {errors}")
    print(f"Open markets:              {open_count}")
    print(f"Closed markets:        all:{closed_count}  no:{closed_no}  yes:{closed_yes}")
    print(f"Closed markets with fills: {closed_with_fills}  no:{closed_with_fills_no}  yes:{closed_with_fills_yes}")
    print(f"Cap:                       {cap}   Bet size: ${bet_size:.2f}")
    print(f"Total realized P/L:        {total_pl:+.2f}")
    print(f"Success fills:             {succ}  ({pct(succ, n)})")
    print(f"Avg under-cap $:           ${mean(under_dollars):.2f}")
    print(f"Avg under-cap shares:      {mean(under_shares):.4f}")
    if lows:
        print(f"Lowest(ever) NO px:        min={min(lows):.4f}  mean={mean(lows):.4f}  max={max(lows):.4f}")
    else:
        print(f"Lowest(ever) NO px:        (no data)")

# ----------------- Main -----------------
def main():
    ap = argparse.ArgumentParser(description="NO-bet scanner with final closed cache, multi-cap fills, basic=cap0.40@$5, and single-cap/bet view.")
    ap.add_argument("--folder", required=False, help="Folder under logs/", default=None)
    ap.add_argument("--loop", type=int, default=0, help="Loop seconds (0=single pass)")

    # view cap/bet (kept for interactive overview + logs)
    ap.add_argument("--bet", type=float, default=None, help="Bet size for view overview (prompted if omitted)")
    ap.add_argument("--cap", type=float, default=None, help="Cap for view overview (prompted if omitted)")

    # live fill controls
    ap.add_argument("--max_notional", type=float, default=500.0, help="Max $ per cap before stopping per-market fetch")
    ap.add_argument("--caps_start", type=float, default=0.20)
    ap.add_argument("--caps_stop",  type=float, default=0.80)
    ap.add_argument("--caps_step",  type=float, default=0.05)

    # filtering + caching
    ap.add_argument("--exclude", type=str, default="", help="Comma-separated keywords to exclude (by question text)")
    ap.add_argument("--open_cache_max_age_days", type=float, default=2.0, help="Snapshot cache age limit for OPEN markets")
    ap.add_argument("--recheck_all", action="store_true", help="Ignore caches and recompute for every market")
    args = ap.parse_args()

    folder = args.folder.strip() if args.folder else input("Folder name under logs/: ").strip()

    # Prompt for the single-cap/bet view if not provided
    if args.bet is None:
        try:
            bet_size = float(input("Bet size $ [10.0]: ").strip() or "10.0")
        except:
            bet_size = 10.0
    else:
        bet_size = float(args.bet)

    if args.cap is None:
        try:
            view_cap = float(input("Price cap [0.5]: ").strip() or "0.5")
        except:
            view_cap = 0.5
    else:
        view_cap = float(args.cap)

    try:
        loop_s = int(input("Loop seconds (0=single pass) [3600]: ").strip() or "3600") if args.loop == 0 else args.loop
    except:
        loop_s = LOOP_DEFAULT_SEC

    excluded = [x.strip().lower() for x in (args.exclude or "").split(",") if x.strip()]

    # Build caps list (include the view cap to avoid re-fetch)
    caps = []
    x = args.caps_start
    while x <= args.caps_stop + 1e-9:
        caps.append(round(x, 2))
        x += args.caps_step
    if round(view_cap,2) not in caps:
        caps.append(round(view_cap,2))
    caps = sorted(set(caps))

    # Paths
    out_dir = os.path.join(LOGS_DIR, folder)
    ensure_dir(out_dir)
    snap_path     = os.path.join(out_dir, "log_market_snapshots.jsonl")   # rewritten each pass
    closed_path   = os.path.join(out_dir, "log_closed_markets.jsonl")     # rewritten each pass
    pass_summary  = os.path.join(out_dir, "pass_summary.json")

    print(f"{dt_iso()} Starting scan… view_cap={view_cap} bet=${bet_size:.2f} | caps={caps} max_notional={args.max_notional}")

    while True:
        uniq = read_unique_markets(folder)
        print(f"\n=== NO fills (folder={folder}) | markets={len(uniq)} ===")

        skipped = 0
        errors = 0
        view_snapshots = []
        view_closed_rows = []

        # Load existing caches
        closed_cache_prev   = load_jsonl_by_key(closed_path, "conditionId")
        snapshot_cache_prev = load_jsonl_by_key(snap_path, "conditionId")

        for i, (cid, meta) in enumerate(uniq.items(), 1):
            q = meta["question"]
            since_epoch = _parse_iso_to_epoch(meta["time_found"]) or 0

            if excluded and any(tok in q.lower() for tok in excluded):
                skipped += 1
                if i % 50 == 0 or len(uniq) <= 50:
                    print(f"[{i}/{len(uniq)}] SKIP → {q}")
                continue

            print(f"[{i}/{len(uniq)}] {q}  (cid={cid[:10]}…)  since={meta['time_found']}")

            try:
                m = fetch_market_full_by_cid(cid)
                status = current_status(m)  # 'YES','NO','TBD'
                closed_iso = closed_time_iso(m)
            except Exception as e:
                errors += 1
                print(f"  [WARN meta] {e}")
                status = "TBD"
                closed_iso = None

            # ---------- Decide source of cap results ----------
            caps_result = None
            lowest_no_px = None
            basic_fill_time_epoch = None
            basic_trades_count = 0

            if args.recheck_all:
                if status in ("YES","NO"):
                    try:
                        trades = fetch_all_trades_since(cid, since_epoch, page_limit=TRADES_PAGE_LIMIT)
                        live = build_caps_from_all_trades(trades, since_epoch, caps, args.max_notional)
                        lowest_no_px = live.get("lowest_no_px")
                        caps_result = live["caps"]
                        # basic: 0.40 @ $5
                        basic_fill_time_epoch, basic_trades_count = basic_fill_from_trades(trades, since_epoch, 0.40, 5.0)
                    except Exception as e:
                        errors += 1
                        print(f"  [WARN closed recompute (recheck_all)] {e}")
                        caps_result = {c: {"success": False, "fill_time": None, "avg_px": None, "shares": 0.0, "cost": 0.0,
                                           "under_cap_dollars_seen": 0.0, "under_cap_shares": 0.0,
                                           "under_cap_trades": 0, "over_cap_trades": 0} for c in caps}
                else:
                    try:
                        live = fetch_until_caps_filled(cid, since_epoch, caps, args.max_notional)
                        lowest_no_px = live.get("lowest_no_px")
                        caps_result = live["caps"]
                        # for basic metrics on open, we still need trades — fetch minimally
                        trades = fetch_all_trades_since(cid, since_epoch, page_limit=TRADES_PAGE_LIMIT)
                        basic_fill_time_epoch, basic_trades_count = basic_fill_from_trades(trades, since_epoch, 0.40, 5.0)
                    except Exception as e:
                        errors += 1
                        print(f"  [WARN open fetch (recheck_all)] {e}")
                        caps_result = {c: {"success": False, "fill_time": None, "avg_px": None, "shares": 0.0, "cost": 0.0,
                                           "under_cap_dollars_seen": 0.0, "under_cap_shares": 0.0,
                                           "under_cap_trades": 0, "over_cap_trades": 0} for c in caps}
            else:
                if status in ("YES","NO"):
                    closed_prev = closed_cache_prev.get(cid)
                    if caps_cache_matches(closed_prev, caps, args.max_notional):
                        cached_caps = {float(k): v for k, v in closed_prev["caps_cache"].items()}
                        caps_result = {c: cached_caps.get(c, {"success": False, "fill_time": None, "avg_px": None, "shares": 0.0,
                                                              "cost": 0.0, "under_cap_dollars_seen": 0.0, "under_cap_shares": 0.0,
                                                              "under_cap_trades": 0, "over_cap_trades": 0}) for c in caps}
                        lowest_no_px = closed_prev.get("lowest_no_px")
                        # reuse basic if present, else compute once
                        basic_fill_time_epoch = closed_prev.get("basic_fill_time_040_5_epoch")
                        basic_trades_count = int(closed_prev.get("basic_trades_to_fill_040_5", 0))
                        if basic_fill_time_epoch is None:
                            try:
                                trades = fetch_all_trades_since(cid, since_epoch, page_limit=TRADES_PAGE_LIMIT)
                                basic_fill_time_epoch, basic_trades_count = basic_fill_from_trades(trades, since_epoch, 0.40, 5.0)
                            except Exception as e:
                                errors += 1
                                print(f"  [WARN closed basic recompute] {e}")
                    else:
                        try:
                            trades = fetch_all_trades_since(cid, since_epoch, page_limit=TRADES_PAGE_LIMIT)
                            live = build_caps_from_all_trades(trades, since_epoch, caps, args.max_notional)
                            lowest_no_px = live.get("lowest_no_px")
                            caps_result = live["caps"]
                            basic_fill_time_epoch, basic_trades_count = basic_fill_from_trades(trades, since_epoch, 0.40, 5.0)
                        except Exception as e:
                            errors += 1
                            print(f"  [WARN closed recompute] {e}")
                            caps_result = {c: {"success": False, "fill_time": None, "avg_px": None, "shares": 0.0, "cost": 0.0,
                                               "under_cap_dollars_seen": 0.0, "under_cap_shares": 0.0,
                                               "under_cap_trades": 0, "over_cap_trades": 0} for c in caps}
                else:
                    cached_caps = None
                    prev = snapshot_cache_prev.get(cid)
                    if prev and prev.get("caps_cache") and prev.get("max_notional") == args.max_notional:
                        computed_at = prev.get("computed_at")
                        fresh_ok = False
                        try:
                            if computed_at:
                                comp_dt = _parse_dt_any(computed_at)
                                if comp_dt:
                                    age_days = (datetime.now(timezone.utc) - comp_dt).total_seconds() / 86400.0
                                    fresh_ok = (age_days <= args.open_cache_max_age_days)
                        except Exception:
                            fresh_ok = False
                        if fresh_ok:
                            cached_caps = {float(k): v for k,v in prev["caps_cache"].items()}

                    if cached_caps:
                        missing = [c for c in caps if c not in cached_caps or not cached_caps[c].get("success")]
                        if missing:
                            try:
                                live = fetch_until_caps_filled(cid, since_epoch, missing, args.max_notional)
                                lowest_no_px = live.get("lowest_no_px")
                                caps_result = dict(cached_caps)
                                for c in missing:
                                    caps_result[c] = live["caps"][c]
                            except Exception as e:
                                errors += 1
                                print(f"  [WARN open missing caps fetch] {e}")
                                caps_result = dict(cached_caps)
                        else:
                            caps_result = dict(cached_caps)
                        # ensure basic present (may not be in snapshot cache)
                        try:
                            trades = fetch_all_trades_since(cid, since_epoch, page_limit=TRADES_PAGE_LIMIT)
                            basic_fill_time_epoch, basic_trades_count = basic_fill_from_trades(trades, since_epoch, 0.40, 5.0)
                        except Exception as e:
                            errors += 1
                            print(f"  [WARN open basic compute] {e}")
                    else:
                        try:
                            live = fetch_until_caps_filled(cid, since_epoch, caps, args.max_notional)
                            lowest_no_px = live.get("lowest_no_px")
                            caps_result = live["caps"]
                            # need trades once to compute basic trade count reliably
                            trades = fetch_all_trades_since(cid, since_epoch, page_limit=TRADES_PAGE_LIMIT)
                            basic_fill_time_epoch, basic_trades_count = basic_fill_from_trades(trades, since_epoch, 0.40, 5.0)
                        except Exception as e:
                            errors += 1
                            print(f"  [WARN open fetch] {e}")
                            caps_result = {c: {"success": False, "fill_time": None, "avg_px": None, "shares": 0.0, "cost": 0.0,
                                               "under_cap_dollars_seen": 0.0, "under_cap_shares": 0.0,
                                               "under_cap_trades": 0, "over_cap_trades": 0} for c in caps}

            # -------- Compute view snapshot & (optionally) realized P/L --------
            st_view = caps_result.get(round(view_cap,2), {"success": False, "avg_px": None, "shares": 0.0, "cost": 0.0,
                                                          "under_cap_dollars_seen": 0.0, "under_cap_shares": 0.0,
                                                          "under_cap_trades": 0, "over_cap_trades": 0})
            filled_view = bool(st_view.get("success"))
            pl_view = None
            closed_flag = status in ("YES","NO")
            closed_filled = bool(closed_flag and filled_view)
            if filled_view and status in ("YES","NO"):
                payout = st_view["shares"] * (1.0 if status == "NO" else 0.0)
                pl_view = round(payout - st_view["cost"], 2)

            # -------- Per-market output lines --------
            # (Short per-market print)
            print(f"    lowest NO px = {lowest_no_px} | view under_cap$: {st_view.get('under_cap_dollars_seen',0.0)} "
                  f"shares:{st_view.get('under_cap_shares',0.0)} trades<=cap:{st_view.get('under_cap_trades',0)} "
                  f"→ filled:{'YES' if filled_view else 'NO'}")
            if basic_fill_time_epoch:
                print(f"    BASIC cap=0.40 @ $5 → fill_time={datetime.fromtimestamp(basic_fill_time_epoch, tz=timezone.utc).isoformat()} trades_to_fill={basic_trades_count}")
            else:
                print(f"    BASIC cap=0.40 @ $5 → not filled since time_found (trades_seen={basic_trades_count})")

            # -------- Build snapshot row --------
            snapshot = {
                "ts": dt_iso(),
                "folder": folder,
                "status": status,
                "closed": closed_flag,
                "closed_time": closed_iso,
                "closed_filled": closed_filled,
                "conditionId": cid,
                "question": q,
                "time_found": meta["time_found"],
                "cap": view_cap,
                "bet_size": bet_size,  # reference only
                "filled": filled_view,
                "avg_px": st_view.get("avg_px"),
                "shares": st_view.get("shares"),
                "cost": st_view.get("cost"),
                "under_cap_dollars_seen": st_view.get("under_cap_dollars_seen"),
                "under_cap_shares": st_view.get("under_cap_shares"),
                "under_cap_trades": st_view.get("under_cap_trades"),
                "over_cap_trades": st_view.get("over_cap_trades"),
                "lowest_no_px": lowest_no_px,
                "pl": pl_view,
                # cache metadata for OPEN snapshot reuse
                "caps_cache": {str(float(k)): v for k,v in caps_result.items()},
                "max_notional": args.max_notional,
                "cache_version": CACHE_VERSION,
                "computed_at": dt_iso(),
                "finalized": bool(closed_flag),
                # BASIC metrics
                "basic_fill_time_040_5": (datetime.fromtimestamp(basic_fill_time_epoch, tz=timezone.utc).isoformat()
                                          if basic_fill_time_epoch else None),
                "basic_fill_time_040_5_epoch": basic_fill_time_epoch,
                "basic_trades_to_fill_040_5": basic_trades_count,
            }
            view_snapshots.append(snapshot)

            # -------- If closed, prepare a closed row (finalized) --------
            if closed_flag:
                closed_row = {
                    "ts": dt_iso(),
                    "conditionId": cid,
                    "question": q,
                    "status": status,
                    "closed_time": closed_iso,
                    "time_found": meta["time_found"],
                    # freeze the entire caps_result as final cache
                    "caps_cache": {str(float(k)): v for k,v in caps_result.items()},
                    "max_notional": args.max_notional,
                    "cache_version": CACHE_VERSION,
                    "computed_at": dt_iso(),
                    "lowest_no_px": lowest_no_px,
                    "finalized": True,
                    # also store view info & P/L (handy)
                    "view_cap": view_cap,
                    "view_filled": filled_view,
                    "view_avg_px": st_view.get("avg_px"),
                    "view_shares": st_view.get("shares"),
                    "view_cost": st_view.get("cost"),
                    "view_pl": pl_view,
                    # BASIC metrics (persist so we can reuse)
                    "basic_fill_time_040_5": (datetime.fromtimestamp(basic_fill_time_epoch, tz=timezone.utc).isoformat()
                                              if basic_fill_time_epoch else None),
                    "basic_fill_time_040_5_epoch": basic_fill_time_epoch,
                    "basic_trades_to_fill_040_5": basic_trades_count,
                }
                view_closed_rows.append(closed_row)

        # ---------- Rewrite snapshot / closed ----------
        snap_text = "\n".join(json.dumps(s, ensure_ascii=False) for s in view_snapshots)
        atomic_write_text(snap_path, snap_text + ("\n" if view_snapshots else ""))
        closed_text = "\n".join(json.dumps(s, ensure_ascii=False) for s in view_closed_rows)
        atomic_write_text(closed_path, closed_text + ("\n" if view_closed_rows else ""))
        print(f"[WRITE] snapshots={len(view_snapshots)} → {snap_path}")
        print(f"[WRITE] closed={len(view_closed_rows)} → {closed_path}")

        # ---------- Overview (view cap/bet) ----------
        print_view_overview(view_snapshots, bet_size=bet_size, cap=view_cap, skipped=skipped, errors=errors)

        # ---------- Compact pass summary ----------
        summary = {
            "ts": dt_iso(),
            "markets_scanned": len(view_snapshots),
            "skipped": skipped,
            "errors": errors,
            "view_cap": view_cap,
            "view_bet": bet_size,
            "closed_total": sum(1 for s in view_snapshots if s.get("closed")),
            "closed_with_fill": sum(1 for s in view_snapshots if s.get("closed_filled")),
            "closed_yes": sum(1 for s in view_snapshots if s.get("status") == "YES"),
            "closed_no": sum(1 for s in view_snapshots if s.get("status") == "NO"),
            "success_fills": sum(1 for s in view_snapshots if s.get("filled")),
            "realized_pl_total": round(sum(s.get("pl", 0.0) for s in view_snapshots if s.get("pl") is not None), 2),
            "lowest_no_px_min": min((s.get("lowest_no_px") for s in view_snapshots if s.get("lowest_no_px") is not None), default=None),
            "lowest_no_px_mean": (sum(s.get("lowest_no_px") for s in view_snapshots if s.get("lowest_no_px") is not None) / max(1, sum(1 for s in view_snapshots if s.get("lowest_no_px") is not None))),
        }
        atomic_write_text(pass_summary, json.dumps(summary, ensure_ascii=False, indent=2))
        print(f"[WRITE] pass summary → {pass_summary}")

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