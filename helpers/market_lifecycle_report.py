import csv, json, os, random, sys, time
from datetime import datetime, timedelta, timezone
from statistics import mean, median
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from email.utils import parsedate_to_datetime

# =================== Config ===================
BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"

# Rate limiting (adaptive)
RPS_TARGET = 3.0         # gentle peak ~3 req/s
_RPS_SCALE = 1.0         # auto-tunes 0.3–1.0
_RPS_MIN   = 0.3
_RPS_RECOVER_PER_SEC = 0.04

# HTTP session with retries for transient errors
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=40, pool_maxsize=40)
    s.mount("https://", adapter); s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pm-lifecycle-report/1.0"})
    return s

SESSION = make_session()
_last_tokens_ts = time.monotonic()
_bucket = RPS_TARGET

def _rate_limit():
    """Token bucket on top of requests' retries."""
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
    if not ra:
        return 0.0
    # numeric seconds?
    try:
        return max(0.0, float(ra))
    except Exception:
        pass
    # HTTP-date format
    try:
        dt = parsedate_to_datetime(ra)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
    except Exception:
        return 0.0

def http_get_with_backoff(url, *, params=None, timeout=25, max_tries=8):
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
            sleep_s = max(_retry_after_seconds(r), back) + random.random()*0.3
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

# =================== Time helpers ===================
def now_iso(): return datetime.now(timezone.utc).isoformat()

def _parse_iso_utc(s):
    if not s: return None
    try:
        return datetime.fromisoformat(str(s).replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def _to_secs(dta):
    return None if dta is None else int(dta.timestamp())

# =================== Market helpers ===================
def is_yesno(m):
    outs = m.get("outcomes")
    if isinstance(outs, str):
        try: outs = json.loads(outs)
        except Exception: return False
    return isinstance(outs, list) and len(outs) == 2 and \
        {str(x).strip().lower() for x in outs} == {"yes","no"}

def winner_from_market(m):
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

def field_times(m):
    created = _parse_iso_utc(m.get("createdAt"))
    start   = _parse_iso_utc(m.get("startDate"))
    enddt   = _parse_iso_utc(m.get("endDate"))
    closed  = _parse_iso_utc(m.get("closedTime"))
    resolve = _parse_iso_utc(m.get("resolvedTime") or m.get("resolveTime") or m.get("resolutionTime"))
    return created, start, enddt, closed, resolve

def activity_anchor_start(m):
    created, start, *_ = field_times(m)
    return start or created

def activity_anchor_end(m):
    _, _, enddt, closed, _ = field_times(m)
    # active until endDate or closedTime, whichever is earlier if both exist
    if enddt and closed:
        return min(enddt, closed)
    return enddt or closed

def payout_anchor_start(m):
    # when do we consider "waiting for payout" to begin?
    _, _, enddt, closed, _ = field_times(m)
    return closed or enddt

def payout_anchor_end(m):
    _, _, _, _, resolve = field_times(m)
    return resolve

def market_time_key(m):
    # For pagination sanity checks
    _, _, enddt, _, _ = field_times(m)
    return enddt or _parse_iso_utc(m.get("startDate")) or _parse_iso_utc(m.get("createdAt"))

# =================== Fetch markets by endDate window ===================
def fetch_yesno_by_enddate_window(days_back: int, page_limit=250, include_closed=True):
    """
    Pull YES/NO markets whose endDate is within [now - days_back, now], scanning newest→older on endDate.
    Includes open and closed.
    """
    since_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
    since_secs = _to_secs(since_dt)

    def _pull(closed_flag):
        seen, out = set(), []
        offset = 0
        while True:
            params = {
                "limit": page_limit,
                "order": "endDate",
                "ascending": False,          # newest endDate first
                "enableOrderBook": False,    # we only need metadata
                "closed": closed_flag,
                "offset": offset,
            }
            r = http_get_with_backoff(BASE_GAMMA, params=params, timeout=25)
            data = r.json() or []
            if not data:
                break

            oldest_on_page = None
            for m in data:
                if not is_yesno(m):
                    continue
                key = m.get("conditionId") or m.get("id")
                if not key or key in seen:
                    # still update oldest stamp
                    mk = market_time_key(m)
                    if mk:
                        t = _to_secs(mk)
                        oldest_on_page = t if oldest_on_page is None else min(oldest_on_page, t)
                    continue

                # filter by endDate window
                enddt = _parse_iso_utc(m.get("endDate"))
                if (enddt is None) or (_to_secs(enddt) < since_secs):
                    # if endDate missing/older than window, we can stop only if page is strictly older
                    mk = market_time_key(m)
                    if mk:
                        t = _to_secs(mk)
                        oldest_on_page = t if oldest_on_page is None else min(oldest_on_page, t)
                    continue

                out.append(m); seen.add(key)

                mk = market_time_key(m)
                if mk:
                    t = _to_secs(mk)
                    oldest_on_page = t if oldest_on_page is None else min(oldest_on_page, t)

            if len(data) < page_limit:
                break
            # stop when the page’s oldest is older than our window start
            if oldest_on_page is not None and oldest_on_page < since_secs:
                break

            offset += page_limit
        return out

    mkts = _pull(False)
    if include_closed:
        mkts += _pull(True)

    # final de-dupe (prefer records with conditionId)
    by_key = {}
    for m in mkts:
        key = m.get("conditionId") or m.get("id")
        if not key: 
            continue
        if key not in by_key or (by_key[key].get("conditionId") is None and m.get("conditionId")):
            by_key[key] = m

    # sort by endDate desc
    def sort_key(m):
        ed = _parse_iso_utc(m.get("endDate"))
        return ed or datetime.fromtimestamp(0, tz=timezone.utc)
    return sorted(by_key.values(), key=sort_key, reverse=True)

# =================== Metrics & report ===================
def sec(delta):
    if delta is None: return None
    return int(delta.total_seconds())

def fmt_h(ms):
    if ms is None: return "-"
    h = ms / 3600
    if h >= 24:
        d = h/24
        return f"{d:.1f}d"
    return f"{h:.1f}h"

def compute_lifecycle_row(m):
    q = m.get("question")
    cid = m.get("conditionId") or m.get("id")
    slug = m.get("slug") or m.get("market_slug")
    win  = winner_from_market(m)

    created, start, enddt, closed, resolve = field_times(m)
    active_start = activity_anchor_start(m)
    active_end   = activity_anchor_end(m)
    payout_start = payout_anchor_start(m)
    payout_end   = payout_anchor_end(m)

    active_secs = None
    if active_start and active_end and active_end >= active_start:
        active_secs = sec(active_end - active_start)

    payout_secs = None
    if payout_start and payout_end and payout_end >= payout_start:
        payout_secs = sec(payout_end - payout_start)

    return {
        "market_id": cid,
        "slug": slug,
        "question": q,
        "winner": win,
        "createdAt": created.isoformat() if created else None,
        "startDate": start.isoformat() if start else None,
        "endDate": enddt.isoformat() if enddt else None,
        "closedTime": closed.isoformat() if closed else None,
        "resolvedTime": resolve.isoformat() if resolve else None,
        "active_secs": active_secs,
        "active_h": round(active_secs/3600, 3) if active_secs is not None else None,
        "payout_secs": payout_secs,
        "payout_h": round(payout_secs/3600, 3) if payout_secs is not None else None,
    }

def summarize(nums):
    xs = [x for x in nums if isinstance(x, (int, float)) and x is not None]
    if not xs:
        return {"n":0}
    xs_sorted = sorted(xs)
    def pct(p):
        k = max(0, min(len(xs_sorted)-1, int(round(p*(len(xs_sorted)-1)))))
        return xs_sorted[k]
    return {
        "n": len(xs_sorted),
        "mean_h": round(mean(xs_sorted)/3600, 2),
        "median_h": round(median(xs_sorted)/3600, 2),
        "p10_h": round(pct(0.10)/3600, 2),
        "p25_h": round(pct(0.25)/3600, 2),
        "p75_h": round(pct(0.75)/3600, 2),
        "p90_h": round(pct(0.90)/3600, 2),
        "min_h": round(xs_sorted[0]/3600, 2),
        "max_h": round(xs_sorted[-1]/3600, 2),
    }

def main():
    try:
        days = input("Days back (by endDate): ").strip()
        days_back = int(days) if days else 30
    except Exception:
        print("Please enter an integer number of days.")
        sys.exit(1)

    print(f"→ scanning markets with endDate in the last {days_back} day(s) @ {now_iso()}")
    mkts = fetch_yesno_by_enddate_window(days_back, page_limit=250, include_closed=True)
    print(f"[MKT] collected {len(mkts)} YES/NO markets in window")

    rows = [compute_lifecycle_row(m) for m in mkts]

    # Split buckets for your analysis
    open_like   = [r for r in rows if r["resolvedTime"] is None]       # unresolved
    closed_yes  = [r for r in rows if r["winner"] == "YES"]
    closed_no   = [r for r in rows if r["winner"] == "NO"]
    closed_any  = [r for r in rows if r["resolvedTime"] is not None]

    # Stats
    active_all   = summarize([r["active_secs"] for r in rows if r["active_secs"] is not None])
    payout_all   = summarize([r["payout_secs"] for r in closed_any if r["payout_secs"] is not None])
    payout_yes   = summarize([r["payout_secs"] for r in closed_yes if r["payout_secs"] is not None])
    payout_no    = summarize([r["payout_secs"] for r in closed_no  if r["payout_secs"] is not None])

    print("\n===== LIFECYCLE REPORT =====")
    print(f"Window: last {days_back} day(s) by endDate")
    print(f"Total markets:    {len(rows)}")
    print(f"Open/unresolved:  {len(open_like)}")
    print(f"Closed (any):     {len(closed_any)}   | YES: {len(closed_yes)}  NO: {len(closed_no)}")
    print("\nActive duration (start→min(endDate,closed)):")
    if active_all.get("n",0):
        print(f"  n={active_all['n']}  mean={active_all['mean_h']}h  median={active_all['median_h']}h  "
              f"p10={active_all['p10_h']}h  p90={active_all['p90_h']}h  min={active_all['min_h']}h  max={active_all['max_h']}h")
    else:
        print("  (no data)")

    print("\nPayout lag (resolve − (closed or endDate)) for resolved markets:")
    if payout_all.get("n",0):
        print(f"  ALL n={payout_all['n']}  mean={payout_all['mean_h']}h  median={payout_all['median_h']}h  "
              f"p25={payout_all['p25_h']}h  p75={payout_all['p75_h']}h")
    else:
        print("  (no data)")

    if payout_yes.get("n",0):
        print(f"  YES n={payout_yes['n']}  mean={payout_yes['mean_h']}h  median={payout_yes['median_h']}h")
    if payout_no.get("n",0):
        print(f"  NO  n={payout_no['n']}  mean={payout_no['mean_h']}h  median={payout_no['median_h']}h")

    # Save CSV
    out_name = f"lifecycle_report_{days_back}d_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.csv"
    cols = ["market_id","slug","question","winner","createdAt","startDate","endDate","closedTime","resolvedTime","active_secs","active_h","payout_secs","payout_h"]
    with open(out_name, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in cols})
    print(f"\nSaved CSV → {out_name}")
    print("============================\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nbye.")