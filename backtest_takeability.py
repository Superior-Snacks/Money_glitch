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
def jloads_maybe(x):
    if isinstance(x, list): return x
    if isinstance(x, str):
        try: return json.loads(x)
        except: return None
    return None

def parse_dt_any(s):
    if not s: return None
    try: return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc)
    except: return None

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
    out = []
    next_start = start_iso
    pulled = 0
    while True:
        if pulled >= MAX_TRADE_PULL: break
        _rate_limit()
        params = {
            "token_id": token_id,
            "start_time": next_start,
            "end_time": end_iso,
            "sort": "asc",
            "limit": min(limit_each, MAX_TRADE_PULL - pulled),
        }
        r = SESSION.get(DATA_TRADES, params=params, timeout=20)
        r.raise_for_status()
        rows = r.json() or []
        if not rows: break
        out.extend(rows); pulled += len(rows)
        last_ts = rows[-1].get("timestamp") or rows[-1].get("ts") or rows[-1].get("time")
        if not last_ts: break
        try:
            dt = datetime.fromisoformat(last_ts.replace("Z","+00:00"))
            next_start = (dt + timedelta(milliseconds=1)).isoformat()
        except:
            break
        if next_start >= end_iso: break
    return out

# ============================
# Main backtest
# ============================
def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    detail_path  = os.path.join(OUT_DIR, DETAIL_CSV)
    summary_path = os.path.join(OUT_DIR, SUMMARY_CSV)

    markets = fetch_closed_yesno_markets(DAYS_BACK_STARTS, PAGE_SIZE, MAX_PAGES)
    print(f"✅ closed yes/no markets considered: {len(markets)} (start >= last {DAYS_BACK_STARTS} days)")

    # Prepare summary accumulators: {(thr,win): {tot, no_wins, take_any, take_no_wins}}
    from collections import defaultdict
    agg = defaultdict(lambda: {"tot":0, "no_wins":0, "take_any":0, "take_no_wins":0})

    # Detail CSV header
    detail_fields = [
        "market_id","condition_id","question","start","closed","winner",
        "window_hours","threshold","min_no_price_in_window","takeable_no",
        "no_trades_in_window"
    ]
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

            # token ids
            try:
                no_tok = outcome_map_from_market(m)["NO"]
            except Exception:
                continue

            winner = normalize_winner(m)

            # We’ll reuse the NO trades for different windows (pull once for the max window we need)
            max_win = max(LOOKBACK_HOURS_LIST)
            hard_end = min(sd + timedelta(hours=max_win), cd)
            if hard_end <= sd:
                continue

            trades = fetch_token_trades_range(no_tok, sd.isoformat(), hard_end.isoformat(), limit_each=500)
            prices = []
            ts = []
            for t in trades:
                try:
                    px = float(t.get("price") or 0.0)
                    tt = t.get("timestamp") or t.get("ts") or t.get("time")
                    if px > 0 and tt:
                        prices.append(px); ts.append(tt)
                except: 
                    pass

            # For each window and threshold, evaluate takeability
            for win_h in LOOKBACK_HOURS_LIST:
                end_w = min(sd + timedelta(hours=win_h), cd)
                # slice trades within this smaller window
                win_prices = []
                if prices:
                    for px, tstamp in zip(prices, ts):
                        try:
                            dt = datetime.fromisoformat(tstamp.replace("Z","+00:00")).astimezone(timezone.utc)
                            if sd <= dt <= end_w:
                                win_prices.append(px)
                        except:
                            pass

                min_no = min(win_prices) if win_prices else None

                for thr in THRESHOLDS:
                    agg[(thr, win_h)]["tot"] += 1
                    if winner == "NO":
                        agg[(thr, win_h)]["no_wins"] += 1

                    takeable = (min_no is not None and min_no <= thr)
                    if win_prices:
                        agg[(thr, win_h)]["take_any"] += 1
                    if winner == "NO" and takeable:
                        agg[(thr, win_h)]["take_no_wins"] += 1

                    wr.writerow({
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
                        "no_trades_in_window": len(win_prices),
                    })

            if i % 100 == 0:
                print(f"… processed {i}/{len(markets)}")

    # Build summary table
    with open(summary_path, "w", newline="", encoding="utf-8") as fs:
        fields = [
            "threshold","window_hours","markets","no_winners","pct_no_winners",
            "takeable_no_winners","pct_takeable_of_no_winners",
            "markets_with_any_no_trades_in_window"
        ]
        wrs = csv.DictWriter(fs, fieldnames=fields); wrs.writeheader()
        print("\n================ SUMMARY (by threshold & window) ================")
        combos = sorted(agg.keys(), key=lambda x:(x[1], x[0]))  # by window then threshold
        for (thr, win_h) in combos:
            a = agg[(thr, win_h)]
            tot = a["tot"]
            no_w = a["no_wins"]
            tk_no = a["take_no_wins"]
            any_tr = a["take_any"]
            pct_no = (no_w/tot*100.0) if tot>0 else 0.0
            pct_tk = (tk_no/no_w*100.0) if no_w>0 else 0.0
            print(f"win={str(win_h).rjust(3)}h  thr={thr:0.2f}  markets={tot:5d}  NO_wins={no_w:5d} ({pct_no:5.1f}%)  "
                  f"takeable_NO_wins={tk_no:5d} ({pct_tk:5.1f}%)  any_NO_trades={any_tr:5d}")
            wrs.writerow({
                "threshold": thr,
                "window_hours": win_h,
                "markets": tot,
                "no_winners": no_w,
                "pct_no_winners": round(pct_no,2),
                "takeable_no_winners": tk_no,
                "pct_takeable_of_no_winners": round(pct_tk,2),
                "markets_with_any_no_trades_in_window": any_tr,
            })
        print("=================================================================\n")
        print(f"Detail CSV : {os.path.join(OUT_DIR, DETAIL_CSV)}")
        print(f"Summary CSV: {os.path.join(OUT_DIR, SUMMARY_CSV)}")

if __name__ == "__main__":
    main()