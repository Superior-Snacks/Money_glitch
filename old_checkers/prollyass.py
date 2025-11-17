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

HINT_SPREAD = 0.98
FINAL_GRACE = timedelta(days=2)

# ----------------- Session + pacing -----------------
RPS_TARGET            = 3.5
_RPS_SCALE            = 1.0
_RPS_MIN              = 0.3
_RPS_RECOVER_PER_SEC  = 0.03

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
    s.headers.update({"User-Agent": "pm-no-bet-scan/2.7-bucketfill"})
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

# ----------------- Time / FS utils -----------------
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

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def atomic_write_text(path: str, text: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)

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

# ----------------- Trade paging -----------------
def fetch_trades_page_ms(cid: str, limit=TRADES_PAGE_LIMIT, starting_before_s=None, offset=None):
    params = {"market": cid, "sort": "desc", "limit": int(limit)}
    if starting_before_s is not None:
        params["starting_before"] = int(starting_before_s * 1000)
    if offset is not None:
        params["offset"] = int(offset)
    r = http_get_with_backoff(DATA_TRADES, params=params, timeout=20)
    return r.json() or []

# ----------------- Multi-cap bucket filling (simultaneous) -----------------
def _init_caps_state(caps: List[float], max_notional: float):
    # keep cumulative curve so any bet <= max_notional can be answered
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
            "curve": [(0.0, 0.0, None)],  # (cum_dollars, shares, avg_px)
        } for cap in caps
    }

def _apply_trades_to_buckets(trades: List[dict], caps_state: dict, since_epoch_s: int, lowest_holder: dict, max_notional: float):
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
                    any_progress = True
                    st["cum_dollars"] += take
                    st["shares"] += (take / p)
                    avg_px = st["cum_dollars"] / st["shares"] if st["shares"] > 1e-12 else None
                    st["curve"].append((st["cum_dollars"], st["shares"], avg_px))
                    if st["cum_dollars"] >= max_notional - 1e-9:
                        st["fill_time"] = ts
                        st["success"] = True
            else:
                st["over_cap_trades"] += 1

    all_filled = all(st["success"] for st in caps_state.values())
    return any_progress, all_filled

def build_buckets_until_max(cid: str, since_epoch_s: int, caps: List[float], max_notional: float):
    caps_state = _init_caps_state(caps, max_notional)
    lowest_holder = {"lowest": None}
    starting_before = None
    last_len = None

    for _ in range(10000):
        data = fetch_trades_page_ms(cid, limit=TRADES_PAGE_LIMIT, starting_before_s=starting_before)
        if not data:
            break
        any_progress, all_done = _apply_trades_to_buckets(data, caps_state, since_epoch_s, lowest_holder, max_notional)
        oldest_ts = min(_to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in data)
        if all_done:
            break
        if oldest_ts <= since_epoch_s:
            break
        starting_before = oldest_ts
        if last_len == len(data) and not any_progress:
            break
        last_len = len(data)

    out = {}
    for cap, st in caps_state.items():
        avg_px = (st["cum_dollars"] / st["shares"]) if st["shares"] > 1e-12 else None
        out[cap] = {
            "curve": st["curve"],
            "avg_px_at_max": round(avg_px, 6) if avg_px is not None else None,
            "shares_at_max": round(st["shares"], 6),
            "cost_at_max": round(st["cum_dollars"], 2),
            "under_cap_dollars_seen": round(st["under_cap_seen"], 2),
            "under_cap_shares": round(st["under_cap_shares"], 6),
            "under_cap_trades": st["under_cap_trades"],
            "over_cap_trades": st["over_cap_trades"],
            "fill_time": st["fill_time"],
            "success_to_max": st["success"],
        }

    return {
        "lowest_no_px": (round(lowest_holder["lowest"], 6) if lowest_holder["lowest"] is not None else None),
        "caps": out
    }

# ---- Query a bucket curve at an arbitrary bet (≤ max_notional) ----
def curve_fill_at_bet(curve: List[Tuple[float, float, Optional[float]]], bet: float):
    if not curve:
        return False, None, 0.0
    # exact hit?
    for cum, sh, avg in curve:
        if abs(cum - bet) < 1e-9:
            return True, avg, sh
    # first cum >= bet
    prev_cum, prev_sh, prev_avg = curve[0]
    for cum, sh, avg in curve[1:]:
        if cum >= bet - 1e-9:
            if cum == prev_cum:
                return False, None, 0.0
            d_cum = cum - prev_cum
            d_sh  = sh  - prev_sh
            if d_sh <= 1e-12 or d_cum <= 1e-12:
                return True, avg, sh
            p = d_cum / d_sh
            add_dollars = bet - prev_cum
            add_sh = add_dollars / p
            final_sh = prev_sh + add_sh
            final_avg = ( (prev_avg or 0.0)*prev_sh + p*add_sh ) / final_sh if final_sh > 1e-12 else None
            return True, round(final_avg,6) if final_avg is not None else None, round(final_sh,6)
        prev_cum, prev_sh, prev_avg = cum, sh, avg
    last_cum, last_sh, last_avg = curve[-1]
    if last_cum + 1e-9 >= bet:
        return True, last_avg, last_sh
    return False, None, 0.0

# ----------------- Overview (single view) -----------------
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

    lows = [s["lowest_no_px"] for s in view_snapshots if s.get("lowest_no_px") is not None]
    under_dollars = [s.get("under_cap_dollars_seen",0.0) for s in view_snapshots]
    under_shares  = [s.get("under_cap_shares",0.0) for s in view_snapshots]

    print("\n===== PASS OVERVIEW (view cap/bet) =====")
    print(f"Markets scanned:           {n}")
    print(f"Skipped (filters):         {skipped}")
    print(f"Errors (fetch/etc):        {errors}")
    print(f"Open markets:              {open_count}")
    print(f"Closed markets:        all:{closed_count}  no:{closed_no}  yes:{closed_yes}")
    print(f"Closed markets with fills: {sum(1 for s in view_snapshots if s.get('closed_filled'))}  "
          f"no:{sum(1 for s in view_snapshots if s.get('closed_filled') and s.get('status')=='NO')}  "
          f"yes:{sum(1 for s in view_snapshots if s.get('closed_filled') and s.get('status')=='YES')}")
    print(f"Cap:                       {cap}   Bet size: ${bet_size:.2f}")
    print(f"Total realized P/L:        {total_pl:+.2f}")
    print(f"Success fills:             {succ}  ({pct(succ, n)})")
    print(f"Avg under-cap $:           ${mean(under_dollars):.2f}")
    print(f"Avg under-cap shares:      {mean(under_shares):.4f}")
    if lows:
        print(f"Lowest(ever) NO px:        min={min(lows):.4f}  mean={mean(lows):.4f}  max={max(lows):.4f}")
    else:
        print(f"Lowest(ever) NO px:        (no data)")

# ----------------- Sweep (cap × bet) from bucket curves -----------------
def sweep_caps_bets_from_buckets(collected: List[dict], caps: List[float], bets: List[float]) -> List[dict]:
    rows = []
    total_markets = len(collected)
    for cap in caps:
        for bet in bets:
            fills = 0
            realized_pl = 0.0
            closed_w_pl = 0
            closed_f_n = 0
            closed_f_y = 0
            cost_all = 0.0
            cost_closed = 0.0

            for item in collected:
                status = item["status"]
                curve  = item["curves"].get(round(cap,2))
                if not curve:
                    continue
                filled, avg_px, shares = curve_fill_at_bet(curve, bet)
                if not filled or not avg_px:
                    continue
                fills += 1
                cost_all += bet
                if status in ("YES","NO"):
                    payout = shares * (1.0 if status == "NO" else 0.0)
                    realized_pl += (payout - bet)
                    closed_w_pl += 1
                    cost_closed += bet
                    if status == "YES": closed_f_y += 1
                    else:               closed_f_n += 1

            value_per_dollar = (realized_pl / cost_closed) if cost_closed > 0 else 0.0
            rows.append({
                "cap": round(cap, 2),
                "bet": int(bet),
                "markets": total_markets,
                "fills": fills,
                "closed_with_fill": closed_w_pl,
                "closed_y": closed_f_y,
                "closed_no": closed_f_n,
                "cost_all": round(cost_all, 2),
                "cost_closed": round(cost_closed, 2),
                "realized_pl": round(realized_pl, 2),
                "value_per_dollar": value_per_dollar,
            })
    return rows

def write_sweep_csv(path: str, rows: List[dict]):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["cap","bet","markets","fills","closed_with_fill","closed_y","closed_no","cost_all","cost_closed","realized_pl","value_per_dollar"]
        )
        w.writeheader()
        w.writerows(rows)

# ----------------- Main -----------------
def main():
    ap = argparse.ArgumentParser(description="Simultaneous multi-cap bucket filling + single view + cap×bet sweep.")
    ap.add_argument("--folder", required=False, help="Folder under logs/", default=None)
    ap.add_argument("--loop", type=int, default=0, help="Loop seconds (0=single pass)")

    # single view (interactive if omitted)
    ap.add_argument("--bet", type=float, default=None, help="Bet size for single view (prompted if omitted)")
    ap.add_argument("--cap", type=float, default=None, help="Cap for single view (prompted if omitted)")

    # caps (buckets) config
    ap.add_argument("--caps_start", type=float, default=0.20)
    ap.add_argument("--caps_stop",  type=float, default=0.80)
    ap.add_argument("--caps_step",  type=float, default=0.05)

    # bet sweep config
    ap.add_argument("--bet_start", type=int, default=5)
    ap.add_argument("--bet_stop",  type=int, default=500)
    ap.add_argument("--bet_step",  type=int, default=5)

    # max notional cap for bucket accumulation (defaults to bet_stop)
    ap.add_argument("--max_notional", type=float, default=None, help="Largest bet evaluated; stop accumulation per cap at this amount")

    # filtering
    ap.add_argument("--exclude", type=str, default="", help="Comma-separated keywords to exclude (by question text)")

    args = ap.parse_args()

    folder = args.folder.strip() if args.folder else input("Folder name under logs/: ").strip()

    # single view prompts if missing
    if args.bet is None:
        try:    bet_size = float(input("Bet size $ [10.0]: ").strip() or "10.0")
        except: bet_size = 10.0
    else:
        bet_size = float(args.bet)

    if args.cap is None:
        try:    view_cap = float(input("Price cap [0.5]: ").strip() or "0.5")
        except: view_cap = 0.5
    else:
        view_cap = float(args.cap)

    try:
        loop_s = int(input("Loop seconds (0=single pass) [3600]: ").strip() or "3600") if args.loop == 0 else args.loop
    except:
        loop_s = LOOP_DEFAULT_SEC

    excluded = [x.strip().lower() for x in (args.exclude or "").split(",") if x.strip()]

    # build caps list
    caps = []
    x = args.caps_start
    while x <= args.caps_stop + 1e-9:
        caps.append(round(x, 2))
        x += args.caps_step
    if round(view_cap,2) not in caps:
        caps.append(round(view_cap,2))
    caps = sorted(set(caps))

    # bet sweep list
    bets = list(range(int(args.bet_start), int(args.bet_stop)+1, int(args.bet_step)))
    max_notional = args.max_notional if args.max_notional is not None else float(args.bet_stop)

    # paths
    out_dir = os.path.join(LOGS_DIR, folder)
    ensure_dir(out_dir)
    snap_path     = os.path.join(out_dir, "log_market_snapshots.jsonl")   # rewritten each pass
    closed_path   = os.path.join(out_dir, "log_closed_markets.jsonl")     # rewritten each pass
    pass_summary  = os.path.join(out_dir, "pass_summary.json")            # rewritten each pass
    sweep_path    = os.path.join(out_dir, f"cap_bet_sweep_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.csv")

    print(f"{dt_iso()} Starting scan… view_cap={view_cap} bet=${bet_size:.2f} | caps={caps} | bets={bets[:3]}..{bets[-1]} | max_notional={max_notional}")

    while True:
        uniq = read_unique_markets(folder)
        print(f"\n=== Multi-cap bucket fill (folder={folder}) | markets={len(uniq)} ===")

        skipped = 0
        errors = 0
        view_snapshots = []
        view_closed_rows = []
        collected_for_sweep = []  # each: {"status": 'YES'|'NO'|'TBD', "curves": {cap: curve}}

        for i, (cid, meta) in enumerate(uniq.items(), 1):
            q = meta["question"]
            since_epoch = _parse_iso_to_epoch(meta["time_found"]) or 0

            if excluded and any(tok in q.lower() for tok in excluded):
                skipped += 1
                if i % 50 == 0 or len(uniq) <= 50:
                    print(f"[{i}/{len(uniq)}] SKIP → {q}")
                continue

            print(f"[{i}/{len(uniq)}] {q}  (cid={cid[:10]}…)  since={meta['time_found']}")

            # meta
            try:
                m = fetch_market_full_by_cid(cid)
                status = current_status(m)
                closed_iso = closed_time_iso(m)
            except Exception as e:
                errors += 1
                print(f"  [WARN meta] {e}")
                status = "TBD"
                closed_iso = None

            # build buckets up to max_notional (fills all caps simultaneously)
            try:
                buckets = build_buckets_until_max(cid, since_epoch, caps, max_notional)
            except Exception as e:
                errors += 1
                print(f"  [WARN trades/buckets] {e}")
                continue

            lowest_no_px = buckets["lowest_no_px"]
            caps_obj = buckets["caps"]

            # store curves for sweep
            collected_for_sweep.append({
                "status": status,
                "curves": {cap: caps_obj[cap]["curve"] for cap in caps_obj}
            })

            # ------- single-view snapshot from the curve at (view_cap, bet_size) -------
            st = caps_obj[round(view_cap,2)]
            filled_view, avg_px_view, shares_view = curve_fill_at_bet(st["curve"], bet_size)
            under_seen = st["under_cap_dollars_seen"]
            under_sh   = st["under_cap_shares"]
            u_tr       = st["under_cap_trades"]
            o_tr       = st["over_cap_trades"]

            pl_view = None
            closed_flag = status in ("YES","NO")
            closed_filled = bool(closed_flag and filled_view)
            if filled_view and status in ("YES","NO"):
                payout = shares_view * (1.0 if status == "NO" else 0.0)
                pl_view = round(payout - bet_size, 2)

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
                "bet_size": bet_size,
                "filled": bool(filled_view),
                "avg_px": avg_px_view,
                "shares": shares_view,
                "cost": (bet_size if filled_view else 0.0),
                "under_cap_dollars_seen": under_seen,
                "under_cap_shares": under_sh,
                "under_cap_trades": u_tr,
                "over_cap_trades": o_tr,
                "lowest_no_px": lowest_no_px,
                "pl": pl_view
            }
            view_snapshots.append(snapshot)

            if closed_flag:
                view_closed_rows.append({
                    "ts": dt_iso(),
                    "conditionId": cid,
                    "question": q,
                    "status": status,
                    "closed_time": closed_iso,
                    "time_found": meta["time_found"],
                    "cap": view_cap,
                    "bet_size": bet_size,
                    "filled": bool(filled_view),
                    "closed_filled": closed_filled,
                    "avg_px": avg_px_view,
                    "shares": shares_view,
                    "cost": (bet_size if filled_view else 0.0),
                    "pl": pl_view
                })

        # ---------- rewrite single-view logs ----------
        snap_text = "\n".join(json.dumps(s, ensure_ascii=False) for s in view_snapshots)
        atomic_write_text(snap_path, snap_text + ("\n" if view_snapshots else ""))
        closed_text = "\n".join(json.dumps(s, ensure_ascii=False) for s in view_closed_rows)
        atomic_write_text(closed_path, closed_text + ("\n" if view_closed_rows else ""))
        print(f"[WRITE] snapshots={len(view_snapshots)} → {snap_path}")
        print(f"[WRITE] closed={len(view_closed_rows)} → {closed_path}")

        # ---------- overview ----------
        print_view_overview(view_snapshots, bet_size=bet_size, cap=view_cap, skipped=skipped, errors=errors)

        # ---------- sweep CSV ----------
        sweep_rows = sweep_caps_bets_from_buckets(
            collected_for_sweep,
            caps=sorted(caps),
            bets=bets
        )
        write_sweep_csv(sweep_path, sweep_rows)
        print(f"[SWEEP] wrote {len(sweep_rows)} rows → {sweep_path}")

        # ---------- compact pass summary ----------
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