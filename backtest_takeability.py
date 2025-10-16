import os, csv, json, time
from datetime import datetime, timedelta, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================
# CONFIG (tweak freely)
# ============================
BASE_GAMMA  = "https://gamma-api.polymarket.com/markets"
DATA_TRADES = "https://data-api.polymarket.com/trades"   # token_id,start_time,end_time,sort,limit

# Sweep these thresholds (NO price caps) and windows (hours after start)
THRESHOLDS         = [0.55, 0.60, 0.65, 0.70, 0.75]
LOOKBACK_HOURS_LIST= [1, 3, 6, 12]

DAYS_BACK_STARTS   = 45         # only markets whose startDate >= now - N days
PAGE_SIZE          = 250
MAX_PAGES          = 40
MAX_TRADE_PULL     = 1500       # safety cap per token across the window
RPS_TARGET         = 8.0        # gentle rate limit

OUT_DIR            = "out"
DETAIL_CSV         = "takeability_detail.csv"   # per-market rows
SUMMARY_CSV        = "takeability_summary.csv"  # aggregate table

# ============================
# Networking + rate limit
# ============================
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429,500,502,503,504),
        allowed_methods=("GET","POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent":"takeability-sweep/1.0"})
    return s

SESSION = make_session()
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

# ============================
# Helpers
# ============================

CLOB_TRADES = "https://clob.polymarket.com/trades"

def robust_fetch_trades(token_id, start_iso, end_iso, limit_each=500, max_pull=1500):
    """
    Try data-api with start_time/end_time, then start/end.
    If still empty, try clob endpoint. Returns list of trades.
    """
    def _pull(url, params):
        _rate_limit()
        r = SESSION.get(url, params=params, timeout=20)
        if r.status_code >= 400:
            return []
        try:
            return r.json() or []
        except Exception:
            return []

    out, next_start, pulled = [], start_iso, 0

    while pulled < max_pull and next_start < end_iso:
        # 1) data-api with start_time/end_time
        params = {
            "token_id": token_id,
            "start_time": next_start,
            "end_time": end_iso,
            "sort": "asc",
            "limit": min(limit_each, max_pull - pulled),
        }
        rows = _pull(DATA_TRADES, params)

        # 2) fallback: data-api with start/end
        if not rows:
            params2 = {
                "token_id": token_id,
                "start": next_start,
                "end": end_iso,
                "sort": "asc",
                "limit": min(limit_each, max_pull - pulled),
            }
            rows = _pull(DATA_TRADES, params2)

        # 3) fallback: clob endpoint
        if not rows:
            params3 = {
                "token_id": token_id,
                "start_time": next_start,
                "end_time": end_iso,
                "sort": "asc",
                "limit": min(limit_each, max_pull - pulled),
            }
            rows = _pull(CLOB_TRADES, params3)

        if not rows:
            break

        out.extend(rows); pulled += len(rows)
        last_dt = parse_trade_dt(rows[-1])
        if not last_dt:
            break
        next_start = (last_dt + timedelta(milliseconds=1)).isoformat()

    return out


def min_no_price_from_both_sides(no_trades, yes_trades, start_dt, end_dt):
    """
    Compute the min NO price inside [start_dt, end_dt] using:
      - NO trades directly (price_no)
      - YES trades inverted: 1 - price_yes
    Return (min_no, n_trades_considered)
    """
    def within(dt): return (start_dt <= dt <= end_dt)

    vals = []
    for t in no_trades:
        dt = parse_trade_dt(t)
        if not dt or not within(dt): continue
        try:
            px = float(t.get("price") or 0.0)
            if px > 0: vals.append(px)
        except: pass

    for t in yes_trades:
        dt = parse_trade_dt(t)
        if not dt or not within(dt): continue
        try:
            py = float(t.get("price") or 0.0)
            if 0 < py < 1:
                vals.append(1.0 - py)
        except: pass

    if not vals:
        return None, 0
    return min(vals), len(vals)

def jloads_maybe(x):
    if isinstance(x, list): return x
    if isinstance(x, str):
        try: return json.loads(x)
        except: return None
    return None

def parse_trade_dt(tr):
    """
    Parse a trade timestamp from various formats:
    - ISO strings (2025-10-15T18:00:00Z)
    - Unix seconds (e.g. 1760635119)
    - Unix milliseconds (e.g. 1760635119123)
    """
    for k in ("timestamp","ts","time","created_at","createdAt"):
        v = tr.get(k)
        if v is None:
            continue

        # numeric timestamp
        if isinstance(v, (int, float)):
            try:
                # If it's > 1e12, it's ms; else s
                if v > 1e12:
                    return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
                else:
                    return datetime.fromtimestamp(v, tz=timezone.utc)
            except Exception:
                continue

        # ISO string
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                continue

    return None

def is_yesno(m):
    outs = jloads_maybe(m.get("outcomes")) or m.get("outcomes")
    if not isinstance(outs, list) or len(outs) != 2: return False
    s = [str(x).strip().lower() for x in outs]
    return set(s) == {"yes","no"}

def outcome_map_from_market(m):
    outs = jloads_maybe(m.get("outcomes")) or m.get("outcomes")
    toks = jloads_maybe(m.get("clobTokenIds")) or m.get("clobTokenIds")
    if isinstance(outs, list) and isinstance(toks, list) and len(outs)==2 and len(toks)==2:
        o0, o1 = str(outs[0]).strip().upper(), str(outs[1]).strip().upper()
        if (o0,o1)==("YES","NO"): return {"YES": toks[0], "NO": toks[1]}
        if (o0,o1)==("NO","YES"): return {"YES": toks[1], "NO": toks[0]}
        if "YES" in o0 and "NO" in o1: return {"YES": toks[0], "NO": toks[1]}
        if "NO"  in o0 and "YES" in o1: return {"YES": toks[1], "NO": toks[0]}
    if isinstance(toks, list) and len(toks)==2:
        return {"YES": toks[0], "NO": toks[1]}
    raise ValueError("cannot resolve YES/NO tokens")

def normalize_winner(m):
    # 1) UMA strings
    s = (m.get("umaResolutionStatus") or "").strip().lower()
    if s in {"yes", "no"}:
        return s.upper()
    if s.startswith(("resolved_", "finalized_")):
        tail = s.split("_", 1)[1].upper()
        if tail in {"YES", "NO"}:
            return tail

    # 2) winner/winningOutcome
    w = (m.get("winningOutcome") or m.get("winner") or "").strip().upper()
    if w in {"YES", "NO"}:
        return w

    # 3) Terminal price fallback (Gamma order is ["Yes","No"])
    raw = m.get("outcomePrices")
    prices = json.loads(raw) if isinstance(raw, str) else (raw or [])
    try:
        y = float(prices[0]); n = float(prices[1])
        if y >= 0.98 and n <= 0.02:
            return "YES"
        if n >= 0.98 and y <= 0.02:
            return "NO"
    except Exception:
        pass

    return ""

def parse_dt_any(s):
    if not s: return None
    try: return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc)
    except: return None
# ============================
# Fetchers
# ============================
def fetch_closed_yesno_markets(days_back_starts, page_size, max_pages):
    start_min = (datetime.now(timezone.utc) - timedelta(days=days_back_starts)).isoformat()
    out, seen = [], set()
    offset, pages = 0, 0
    while pages < max_pages:
        _rate_limit()
        q = {
            "limit": page_size,
            "offset": offset,
            "order": "startDate",
            "ascending": False,
            "closed": True,
            "start_date_min": start_min,
        }
        r = SESSION.get(BASE_GAMMA, params=q, timeout=20)
        if r.status_code >= 400:
            print(f"[WARN] gamma {r.status_code} @ offset {offset}")
            break
        rows = r.json() or []
        if not rows: break
        added = 0
        for m in rows:
            if not is_yesno(m): continue
            mid = m.get("id") or m.get("conditionId")
            if mid and mid not in seen:
                out.append(m); seen.add(mid); added += 1
        print(f"[PAGE {pages}] got={len(rows)} added_yesno={added} total={len(out)}")
        if len(rows) < page_size: break
        offset += page_size; pages += 1
    out.sort(key=lambda m: (m.get("startDate") or m.get("createdAt") or ""), reverse=True)
    return out

def fetch_token_trades_range(token_id, start_iso, end_iso, limit_each=500):
    out, next_start, pulled = [], start_iso, 0
    def _pull(params):
        _rate_limit()
        r = SESSION.get(DATA_TRADES, params=params, timeout=20)
        r.raise_for_status()
        return r.json() or []

    while pulled < MAX_TRADE_PULL and next_start < end_iso:
        params = {
            "token_id": token_id, "start_time": next_start, "end_time": end_iso,
            "sort": "asc", "limit": min(limit_each, MAX_TRADE_PULL - pulled),
        }
        rows = _pull(params)
        if not rows:
            params2 = params.copy()
            params2.pop("start_time"); params2.pop("end_time")
            params2["start"] = next_start; params2["end"] = end_iso
            rows = _pull(params2)
            if not rows:
                break

        out.extend(rows); pulled += len(rows)
        last_dt = parse_trade_dt(rows[-1])
        if not last_dt: break
        next_start = (last_dt + timedelta(milliseconds=1)).isoformat()
    return out


def first_no_trade_px_in_horizon(trades, start_dt, horizon=timedelta(minutes=30)):
    """
    Return the FIRST trade price within [start_dt, start_dt+horizon] or None.
    """
    end_dt = start_dt + horizon
    first_dt, first_px = None, None
    for t in trades:
        dt = parse_trade_dt(t)
        if not dt or not (start_dt <= dt <= end_dt):
            continue
        try:
            px = float(t.get("price") or 0.0)
        except Exception:
            continue
        if first_dt is None or dt < first_dt:
            first_dt, first_px = dt, px
    return first_px

# ============================
# Main backtest
# ============================
def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    detail_path  = os.path.join(OUT_DIR, DETAIL_CSV)
    summary_path = os.path.join(OUT_DIR, SUMMARY_CSV)

    markets = fetch_closed_yesno_markets(DAYS_BACK_STARTS, PAGE_SIZE, MAX_PAGES)
    print(f"✅ closed yes/no markets considered: {len(markets)} (start >= last {DAYS_BACK_STARTS} days)")

    from collections import defaultdict
    agg = defaultdict(lambda: {
        "tot": 0,
        "no_wins": 0,
        "take_any": 0,
        "take_no_wins": 0,
        "instant_ge_095": 0,
    })

    def ttfb_col(thr): return f"ttfb_{int(round(thr*100))}_sec"

    detail_fields = [
        "market_id","condition_id","question","start","closed","winner",
        "window_hours","threshold","min_no_price_in_window","takeable_no",
        "no_trades_in_window","first_30m_no_px","first_30m_ge_095"
    ] + [ttfb_col(t) for t in THRESHOLDS]

    with open(detail_path, "w", newline="", encoding="utf-8") as fdet:
        wr = csv.DictWriter(fdet, fieldnames=detail_fields)
        wr.writeheader()

        for i, m in enumerate(markets, 1):
            cid = m.get("conditionId")
            mid = m.get("id") or cid
            q   = m.get("question") or ""
            sd  = parse_dt_any(m.get("startDate") or m.get("createdAt"))
            cd  = parse_dt_any(m.get("closedTime") or m.get("umaEndDate") or m.get("endDate"))
            if not (cid and sd and cd):
                continue

            try:
                om = outcome_map_from_market(m)
                no_tok = om["NO"]
                yes_tok = om["YES"]
            except Exception:
                continue

            winner = normalize_winner(m)

            hard_end = min(sd + timedelta(hours=max(LOOKBACK_HOURS_LIST)), cd)
            if hard_end <= sd:
                continue

            # Fetch trades for both sides
            no_trades  = robust_fetch_trades(no_tok, sd.isoformat(), hard_end.isoformat(), limit_each=500)
            yes_trades = robust_fetch_trades(yes_tok, sd.isoformat(), hard_end.isoformat(), limit_each=500)

            # Skip if absolutely no trades
            if not (no_trades or yes_trades):
                continue

            # --- FIX 1: Align analysis window to real trade timestamps ---
            trade_dts = [parse_trade_dt(t) for t in (no_trades + yes_trades)]
            trade_dts = [d for d in trade_dts if d]
            if not trade_dts:
                continue
            first_trade_dt = min(trade_dts)
            last_trade_dt  = max(trade_dts)
            analysis_start = first_trade_dt
            analysis_end   = min(last_trade_dt, cd)

            # --- first 30m NO price ---
            first_30m_end = analysis_start + timedelta(minutes=30)
            mn_30, cnt_30 = min_no_price_from_both_sides(no_trades, yes_trades, analysis_start, first_30m_end)
            px0_30m = mn_30
            first_30m_ge_095_flag = int(px0_30m is not None and px0_30m >= 0.95)

            #window logic
            for win_h in LOOKBACK_HOURS_LIST:
                end_w = min(analysis_start + timedelta(hours=win_h), analysis_end)

                # Build one merged stream of NO-prices inside [analysis_start, end_w]
                merged = []  # list of (dt, p_no)
                # NO side (direct)
                for t in no_trades:
                    dt = parse_trade_dt(t)
                    if not dt or not (analysis_start <= dt <= end_w):
                        continue
                    try:
                        px = float(t.get("price") or 0.0)
                    except:
                        continue
                    if 0 < px < 1:
                        merged.append((dt, px))
                # YES side (invert)
                for t in yes_trades:
                    dt = parse_trade_dt(t)
                    if not dt or not (analysis_start <= dt <= end_w):
                        continue
                    try:
                        py = float(t.get("price") or 0.0)
                    except:
                        continue
                    if 0 < py < 1:
                        merged.append((dt, 1.0 - py))

                merged.sort(key=lambda x: x[0])
                n_considered = len(merged)
                if n_considered == 0 and px0_30m is not None:
                    print(f"[DEBUG] mismatch: have 30m px ({px0_30m}) but no trades in {win_h}h window for {mid} — {q[:70]}")
                min_no = min((p for _, p in merged), default=None)

                # --- compute TTFB in the same pass to avoid any mismatch ---
                first_below_dt = {thr: None for thr in THRESHOLDS}
                for dt, p_no in merged:
                    for thr in THRESHOLDS:
                        if p_no <= thr and first_below_dt[thr] is None:
                            first_below_dt[thr] = dt

                # --- aggregate ---
                for thr in THRESHOLDS:
                    a = agg[(thr, win_h)]
                    a["tot"] += 1
                    if winner == "NO":
                        a["no_wins"] += 1
                    if n_considered > 0:
                        a["take_any"] += 1
                    if first_30m_ge_095_flag:
                        a["instant_ge_095"] += 1

                    takeable = (min_no is not None and min_no <= thr)
                    if winner == "NO" and takeable:
                        a["take_no_wins"] += 1

                    # write row
                    row = {
                        "market_id": mid,
                        "condition_id": cid,
                        "question": q,
                        "start": sd.isoformat(),
                        "closed": cd.isoformat(),
                        "winner": winner,
                        "window_hours": win_h,
                        "threshold": thr,
                        "min_no_price_in_window": f"{min_no:.4f}" if min_no is not None else "",
                        "takeable_no": int(bool(takeable)),
                        "no_trades_in_window": n_considered,
                        "first_30m_no_px": f"{px0_30m:.4f}" if px0_30m is not None else "",
                        "first_30m_ge_095": first_30m_ge_095_flag,
                    }
                    for tthr in THRESHOLDS:
                        dtfb = first_below_dt[tthr]
                        row[f"ttfb_{int(round(tthr*100))}_sec"] = (
                            int((dtfb - analysis_start).total_seconds()) if dtfb else ""
                        )
                    wr.writerow(row)

            if i % 100 == 0:
                print(f"… processed {i}/{len(markets)}")

    # --- Summary ---
    with open(summary_path, "w", newline="", encoding="utf-8") as fs:
        fields = [
            "threshold","window_hours","markets","no_winners","pct_no_winners",
            "takeable_no_winners","pct_takeable_of_no_winners",
            "markets_with_any_no_trades_in_window","pct_instant_ge_095_30m"
        ]
        wrs = csv.DictWriter(fs, fieldnames=fields)
        wrs.writeheader()
        print("\n================ SUMMARY (by threshold & window) ================")
        for (thr, win_h) in sorted(agg.keys(), key=lambda x: (x[1], x[0])):
            a = agg[(thr, win_h)]
            tot, no_w, tk_no, any_tr, inst = a["tot"], a["no_wins"], a["take_no_wins"], a["take_any"], a["instant_ge_095"]
            pct_no  = (no_w/tot*100.0) if tot>0 else 0.0
            pct_tk  = (tk_no/no_w*100.0) if no_w>0 else 0.0
            pct_095 = (inst/tot*100.0) if tot>0 else 0.0

            print(f"win={str(win_h).rjust(3)}h  thr={thr:0.2f}  markets={tot:5d}  "
                  f"NO_wins={no_w:5d} ({pct_no:5.1f}%)  takeable_NO_wins={tk_no:5d} ({pct_tk:5.1f}%)  "
                  f"any_NO_trades={any_tr:5d}  instant>=0.95(30m)={inst:5d} ({pct_095:5.1f}%)")
            wrs.writerow({
                "threshold": thr,
                "window_hours": win_h,
                "markets": tot,
                "no_winners": no_w,
                "pct_no_winners": round(pct_no, 2),
                "takeable_no_winners": tk_no,
                "pct_takeable_of_no_winners": round(pct_tk, 2),
                "markets_with_any_no_trades_in_window": any_tr,
                "pct_instant_ge_095_30m": round(pct_095, 2),
            })
        print("=================================================================\n")
        print(f"Detail CSV : {os.path.join(OUT_DIR, DETAIL_CSV)}")
        print(f"Summary CSV: {os.path.join(OUT_DIR, SUMMARY_CSV)}")
if __name__ == "__main__":
    main()