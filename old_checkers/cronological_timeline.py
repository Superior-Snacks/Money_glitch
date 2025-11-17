import os, sys, json, time, random
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =============================== CONFIG ===============================
BASE_GAMMA  = "https://gamma-api.polymarket.com/markets"
DATA_TRADES = "https://data-api.polymarket.com/trades"
TRADES_PAGE_LIMIT = 250

# Adaptive RPS (gentle)
RPS_TARGET = 3.5
_RPS_SCALE = 1.0
_RPS_MIN   = 0.3
_RPS_RECOVER_PER_SEC = 0.03

# ============================ Session & pacing =========================
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429,500,502,503,504),
        allowed_methods=("GET","POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=40, pool_maxsize=40)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pm-old-markets-chrono-no/1.0"})
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
            time.sleep(back + random.random()*0.25)
            back = min(back*1.7, 20.0); tries += 1
            if tries >= max_tries: raise
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
            back = min(back*1.8, 30.0); tries += 1
            if tries >= max_tries: r.raise_for_status()
            continue

        if 500 <= r.status_code < 600:
            time.sleep(back + random.random()*0.25)
            back = min(back*1.7, 20.0); tries += 1
            if tries >= max_tries: r.raise_for_status()
            continue

        r.raise_for_status()

# ================================ Utils ================================
def dt_iso(): return datetime.now(timezone.utc).isoformat()

def parse_iso_to_epoch(s: Optional[str]) -> Optional[int]:
    if not s: return None
    try:
        return int(datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc).timestamp())
    except Exception:
        return None

def epoch_to_iso(ts: Optional[int]) -> Optional[str]:
    if ts is None: return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

def _to_epoch_any(x) -> Optional[int]:
    try:
        f = float(x)
        return int(f/1000) if f > 1e12 else int(f)
    except Exception:
        return None

# ================================ Fetch ================================
def fetch_yesno_markets_by_created(days_back: int, include_closed=True, page_limit=250) -> List[dict]:
    """
    Pull markets ordered by startDate (desc), stop when pages get older than created cutoff.
    Keep only strict YES/NO binaries. Return list sorted by createdAt ascending (creation order).
    """
    since_epoch = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp())

    def _market_time_epoch(m):
        for k in ("createdAt","startDate","endDate","closedTime","resolveTime","resolvedTime"):
            ts = parse_iso_to_epoch(m.get(k))
            if ts: return ts
        return None

    def _pull(closed_flag):
        seen, out = set(), []
        offset = 0
        while True:
            params = {
                "limit": page_limit,
                "order": "startDate",
                "ascending": False,
                "enableOrderBook": False,
                "closed": closed_flag,
                "offset": offset,
            }
            r = http_get_with_backoff(BASE_GAMMA, params=params, timeout=30)
            data = r.json() or []
            if not data: break

            oldest_on_page = None
            for m in data:
                outs = m.get("outcomes")
                if isinstance(outs, str):
                    try: outs = json.loads(outs)
                    except: outs = None
                if not (isinstance(outs, list) and len(outs) == 2 and
                        {str(x).strip().lower() for x in outs} == {"yes","no"}):
                    continue
                key = m.get("conditionId") or m.get("id")
                if not key or key in seen:
                    mt = _market_time_epoch(m)
                    if mt is not None:
                        oldest_on_page = mt if oldest_on_page is None else min(oldest_on_page, mt)
                    continue
                out.append(m); seen.add(key)
                mt = _market_time_epoch(m)
                if mt is not None:
                    oldest_on_page = mt if oldest_on_page is None else min(oldest_on_page, mt)

            if len(data) < page_limit: break
            if oldest_on_page is not None and oldest_on_page < since_epoch: break
            offset += page_limit
            time.sleep(0.3)
        return out

    mkts = _pull(False)
    if include_closed:
        mkts += _pull(True)

    # De-dupe prefer ones with conditionId
    by_key = {}
    for m in mkts:
        key = m.get("conditionId") or m.get("id")
        if not key: continue
        if key not in by_key or (by_key[key].get("conditionId") is None and m.get("conditionId")):
            by_key[key] = m

    # Sort by createdAt ascending (creation order)
    def created_epoch(m): return parse_iso_to_epoch(m.get("createdAt")) or 0
    rows = sorted(by_key.values(), key=lambda m: (created_epoch(m), m.get("id") or ""))
    return rows

def fetch_trades_page_ms(cid: str, limit=TRADES_PAGE_LIMIT, starting_before_s=None, offset=None):
    params = {"market": cid, "sort": "desc", "limit": int(limit)}
    if starting_before_s is not None:
        params["starting_before"] = int(starting_before_s * 1000)  # ms
    if offset is not None:
        params["offset"] = int(offset)
    r = http_get_with_backoff(DATA_TRADES, params=params, timeout=20)
    return r.json() or []

def fetch_all_trades_full(cid: str) -> List[dict]:
    """
    Fetch full trade history (timestamp paging then offset), return ascending.
    """
    out = []
    starting_before = None
    last_len = None

    for _ in range(10000):
        data = fetch_trades_page_ms(cid, limit=TRADES_PAGE_LIMIT, starting_before_s=starting_before)
        if not data: break
        out.extend(data)
        oldest_ts = min(_to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in data)
        starting_before = oldest_ts
        if last_len == len(data): break
        last_len = len(data)

    if len(out) < TRADES_PAGE_LIMIT * 2:
        offset = 0
        for _ in range(2000):
            data = fetch_trades_page_ms(cid, limit=TRADES_PAGE_LIMIT, offset=offset)
            if not data: break
            out.extend(data)
            offset += len(data)

    # de-dup & sort asc
    seen, uniq = set(), []
    for t in out:
        key = t.get("id") or (t.get("timestamp"), t.get("price"), t.get("size"), t.get("side"), t.get("outcome"))
        if key in seen: continue
        seen.add(key); uniq.append(t)
    uniq.sort(key=lambda t: _to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0)
    return uniq

# ============================= Normalizers =============================
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
        ts = parse_iso_to_epoch(m.get(k))
        if ts: return ts
    return None

# ============================ NO Fill engine ===========================
def try_fill_no(bet: float, cap: float, trades: List[dict]) -> Tuple[Optional[int], Optional[float], Optional[float]]:
    """
    Try to fill a NO bet using all trades (ascending) at/under cap.
    Returns: (fill_time, avg_px, shares) OR (None,None,None) if never filled.
    """
    cum_dollars = 0.0
    vwap_num = 0.0
    shares = 0.0
    fill_time = None

    for t in trades:
        if str(t.get("outcome","")).lower() != "no":
            continue
        ts = _to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0
        p = float(t.get("price") or 0.0)
        s = float(t.get("size") or 0.0)
        if p <= 0 or s <= 0: continue

        if p <= cap + 1e-12:
            # how many dollars at this price?
            dollars = p * s
            need = max(0.0, bet - cum_dollars)
            take_dollars = min(dollars, need)
            if take_dollars > 0:
                take_shares = take_dollars / p
                vwap_num += p * take_shares
                shares += take_shares
                cum_dollars += take_dollars
                if cum_dollars >= bet - 1e-9:
                    fill_time = ts
                    break

    if fill_time is None or shares <= 0:
        return None, None, None
    avg_px = vwap_num / shares
    return fill_time, avg_px, shares

# ============================== Main flow ==============================
def main():
    try:
        days_back = int(input("Days back [200]: ").strip() or "200")
    except:
        days_back = 200
    try:
        bet = float(input("Bet size $ [10.0]: ").strip() or "10.0")
    except:
        bet = 10.0
    try:
        cap = float(input("Price cap [0.5]: ").strip() or "0.5")
    except:
        cap = 0.5

    print(f"{dt_iso()} Fetching markets ~{days_back} days back (Yes/No only)…")
    markets = fetch_yesno_markets_by_created(days_back=days_back, include_closed=True, page_limit=250)
    print(f"Loaded {len(markets)} markets.")

    # Build events from markets in creation order
    events: List[dict] = []
    for i, m in enumerate(markets, 1):
        cid = m.get("conditionId") or m.get("id")
        question = m.get("question") or ""
        created = parse_iso_to_epoch(m.get("createdAt"))
        end_or_res = resolved_ts_from_market(m)
        winner = norm_winner(m)

        if not cid:
            continue

        # Fetch full trade history
        try:
            trades = fetch_all_trades_full(cid)
        except Exception as e:
            print(f"[WARN trades {cid[:10]}] {e}")
            continue

        # Attempt to fill NO bet
        fill_time, avg_px, shares = try_fill_no(bet=bet, cap=cap, trades=trades)

        # If filled → BUY event (lock capital starting from fill_time)
        if fill_time is not None:
            events.append({
                "type": "BUY",
                "ts": fill_time,
                "ts_iso": epoch_to_iso(fill_time),
                "cid": cid,
                "question": question,
                "createdAt": created,
                "endOrResolved": end_or_res,
                "avg_px": round(avg_px, 6),
                "cost": round(bet, 2),
                "shares": round(shares, 6),
            })

        # Always add a RESOLVE/END event to unlock capital if we had a position,
        # or just to record outcome.
        if end_or_res is not None:
            events.append({
                "type": "RESOLVE",
                "ts": end_or_res,
                "ts_iso": epoch_to_iso(end_or_res),
                "cid": cid,
                "question": question,
                "createdAt": created,
                "endOrResolved": end_or_res,
                "winner": winner,
            })

        # Progress ping
        if i % 200 == 0:
            print(f" … processed {i}/{len(markets)}")

    # Sort timeline:
    #   1) timestamp
    #   2) BUY before RESOLVE if same ts
    #   3) earlier createdAt first (for stable ordering on ties)
    def key_ev(e):
        return (
            e.get("ts") or 10**18,
            0 if e["type"] == "BUY" else 1,
            e.get("createdAt") or 0,
        )
    events.sort(key=key_ev)

    # Walk the stream and print chronological ledger
    w = l = 0
    running_pl = 0.0
    locked_by_cid: Dict[str, float] = {}
    shares_by_cid: Dict[str, float] = {}

    max_locked = 0.0

    print("\n===== Chronological simulation (NO, cap-fill) =====")
    print(f"bet=${bet:.2f}, cap={cap:.3f}")
    print("---------------------------------------------------")

    for ev in events:
        if ev["type"] == "BUY":
            cid = ev["cid"]
            if cid in locked_by_cid:
                # already open (rare tie-case); ignore re-buy
                continue
            locked_by_cid[cid] = ev["cost"]
            shares_by_cid[cid] = ev["shares"]
            locked_now = sum(locked_by_cid.values())
            max_locked = max(max_locked, locked_now)
            print(f"{ev['ts_iso']} BUY  | lock=${ev['cost']:.2f} @ {ev['avg_px']:.4f}  | locked=${locked_now:.2f} (max=${max_locked:.2f})  :: {ev['question'][:90]}")

        elif ev["type"] == "RESOLVE":
            cid = ev["cid"]
            if cid not in locked_by_cid:
                # no position → nothing to realize, just a marker
                print(f"{ev['ts_iso']} END   | no position | winner={ev.get('winner')} :: {ev['question'][:90]}")
                continue

            cost = locked_by_cid.pop(cid)
            sh   = shares_by_cid.pop(cid)
            winner = ev.get("winner")

            if winner in ("YES","NO"):
                payout = sh * (1.0 if winner == "NO" else 0.0)
                pl = payout - cost
                running_pl += pl
                locked_now = sum(locked_by_cid.values())
                if pl > 0:
                    w += 1
                elif pl < 0:
                    l += 1
                print(f"{ev['ts_iso']} RES  | winner={winner:3s}  P/L={pl:+.2f}  cumP/L={running_pl:+.2f}  | locked=${locked_now:.2f} (max=${max_locked:.2f}) :: {ev['question'][:90]}")
            else:
                # unlock but unresolved winner (rare if going far back)
                locked_now = sum(locked_by_cid.values())
                print(f"{ev['ts_iso']} END  | unresolved winner; unlock ${cost:.2f} | locked=${locked_now:.2f} (max=${max_locked:.2f}) :: {ev['question'][:90]}")

    print("---------------------------------------------------")
    print(f"RESULTS → W:{w}  L:{l}  Realized P/L: {running_pl:+.2f}  Max Locked: ${max_locked:.2f}")
    print("===== DONE =====")

if __name__ == "__main__":
    main()