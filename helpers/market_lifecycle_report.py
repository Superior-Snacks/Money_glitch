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
    s.headers.update({"User-Agent": "pm-lifecycle-report/1.1"})
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
    try:
        return max(0.0, float(ra))
    except Exception:
        pass
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

def _parse_ts_any(v):
    """
    Accepts:
      - ISO8601 strings (with or without 'Z')
      - epoch seconds (int/float)
      - epoch milliseconds (int/float > 1e12)
    Returns: aware datetime in UTC or None
    """
    if v is None:
        return None
    # numeric epoch?
    if isinstance(v, (int, float)):
        try:
            s = float(v)
            if s > 1e12:   # ms → s
                s = s / 1000.0
            return datetime.fromtimestamp(s, tz=timezone.utc)
        except Exception:
            return None
    # string ISO?
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return None
    return None

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
    """
    Returns: (created, start, enddt, closed, resolve) as datetimes (UTC) or None.
    We check multiple key aliases and support ISO or epoch(ms/s).
    """
    created = None
    for k in ("createdAt", "created_at"):
        created = _parse_ts_any(m.get(k))
        if created: break

    start = None
    for k in ("startDate", "start_time", "start"):
        start = _parse_ts_any(m.get(k))
        if start: break

    enddt = None
    for k in ("endDate", "end_time", "end"):
        enddt = _parse_ts_any(m.get(k))
        if enddt: break

    closed = None
    for k in ("closedTime", "closeTime", "closed_at", "closed"):
        closed = _parse_ts_any(m.get(k))
        if closed: break

    resolve = None
    # include many aliases seen in the wild
    for k in ("resolvedTime", "resolveTime", "resolutionTime", "resolved_at",
              "resolve_at", "resolution_at", "settledTime", "settled_at",
              "resolveDate", "resolutionDate"):
        resolve = _parse_ts_any(m.get(k))
        if resolve: break

    return created, start, enddt, closed, resolve

def activity_anchor_start(m):
    created, start, *_ = field_times(m)
    return start or created

def activity_anchor_end(m):
    _, _, enddt, closed, _ = field_times(m)
    if enddt and closed:
        return min(enddt, closed)
    return enddt or closed

def payout_anchor_start(m):
    # when “counting” payout lag from: closedTime if present, else endDate
    _, _, enddt, closed, _ = field_times(m)
    return closed or enddt

def payout_anchor_end(m):
    # when “counting” payout lag to: resolved/settled time (any alias we parse)
    _, _, _, _, resolve = field_times(m)
    if resolve is None and winner_from_market(m) in ("YES","NO"):
    # assume instant payout when winner exists but no resolve timestamp available
        return payout_anchor_start(m)
    return resolve

def market_time_key(m):
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
                "ascending": False,
                "enableOrderBook": False,
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
                    mk = market_time_key(m)
                    if mk:
                        t = _to_secs(mk)
                        oldest_on_page = t if oldest_on_page is None else min(oldest_on_page, t)
                    continue

                enddt = _parse_iso_utc(m.get("endDate"))
                if (enddt is None) or (_to_secs(enddt) < since_secs):
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
            if oldest_on_page is not None and oldest_on_page < since_secs:
                break

            offset += page_limit
        return out

    mkts = _pull(False)
    if include_closed:
        mkts += _pull(True)

    by_key = {}
    for m in mkts:
        key = m.get("conditionId") or m.get("id")
        if not key: 
            continue
        if key not in by_key or (by_key[key].get("conditionId") is None and m.get("conditionId")):
            by_key[key] = m

    def sort_key(m):
        ed = _parse_iso_utc(m.get("endDate"))
        return ed or datetime.fromtimestamp(0, tz=timezone.utc)
    return sorted(by_key.values(), key=sort_key, reverse=True)

# =================== Robust stats (DAYS) ===================
def _iqr(xs_sorted):
    if not xs_sorted:
        return None
    n = len(xs_sorted)
    def q(p):
        idx = int(round(p*(n-1)))
        return xs_sorted[max(0, min(n-1, idx))]
    return q(0.75) - q(0.25)

def trimmed_mean_days(values_seconds, trim_p=0.10):
    xs = [v/86400.0 for v in values_seconds if isinstance(v, (int,float))]
    if not xs: return None
    xs.sort()
    n = len(xs)
    k = int(n * trim_p)
    if k*2 >= n:
        return median(xs)  # degenerate: too few points, fall back
    core = xs[k:n-k]
    return sum(core) / len(core)

def winsorized_mean_days(values_seconds, winsor_p=0.05):
    xs = [v/86400.0 for v in values_seconds if isinstance(v, (int,float))]
    if not xs: return None
    xs.sort()
    n = len(xs)
    k = int(n * winsor_p)
    lo = xs[k] if k < n else xs[-1]
    hi = xs[-k-1] if k < n else xs[0]
    clipped = [min(max(x, lo), hi) for x in xs]
    return sum(clipped) / len(clipped)

def summarize_days(values_seconds):
    xs = [v for v in values_seconds if isinstance(v, (int,float))]
    if not xs:
        return {"n":0}
    xs_days = sorted([v/86400.0 for v in xs])
    n = len(xs_days)
    med = median(xs_days)
    t10 = trimmed_mean_days(xs, 0.10)
    w05 = winsorized_mean_days(xs, 0.05)
    iqr = _iqr(xs_days)
    return {
        "n": n,
        "median_d": round(med, 3),
        "trimmed10_mean_d": round(t10, 3) if t10 is not None else None,
        "winsor05_mean_d": round(w05, 3) if w05 is not None else None,
        "iqr_d": round(iqr, 3) if iqr is not None else None,
        "min_d": round(xs_days[0], 3),
        "max_d": round(xs_days[-1], 3),
    }

# =================== Rows & report ===================
def sec(delta):
    if delta is None: return None
    return int(delta.total_seconds())

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
        "payout_secs": payout_secs,
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

    active_secs = [r["active_secs"] for r in rows if r["active_secs"] is not None]
    payout_secs_any = [r["payout_secs"] for r in rows if r["payout_secs"] is not None]
    payout_secs_yes = [r["payout_secs"] for r in rows if r["payout_secs"] is not None and r["winner"] == "YES"]
    payout_secs_no  = [r["payout_secs"] for r in rows if r["payout_secs"] is not None and r["winner"] == "NO"]

    active_stats = summarize_days(active_secs)
    payout_stats = summarize_days(payout_secs_any)
    payout_yes_stats = summarize_days(payout_secs_yes)
    payout_no_stats  = summarize_days(payout_secs_no)

    print("\n===== LIFECYCLE REPORT (robust days) =====")
    print(f"Window: last {days_back} day(s) by endDate")
    print(f"Total markets in window: {len(rows)}")
    print(f"Resolved markets: {payout_stats.get('n',0)} | YES: {payout_yes_stats.get('n',0)}  NO: {payout_no_stats.get('n',0)}")

    def _p(label, s):
        if s.get("n",0)==0:
            print(f"{label}: (no data)")
            return
        print(
            f"{label}: n={s['n']}  "
            f"median={s['median_d']}d  "
            f"trimmed-mean(10%)={s['trimmed10_mean_d']}d  "
            f"winsor-mean(5%)={s['winsor05_mean_d']}d  "
            f"IQR={s['iqr_d']}d  "
            f"min={s['min_d']}d  max={s['max_d']}d"
        )

    print("\nActive duration (start → min(endDate, closedTime))")
    _p("  ACTIVE", active_stats)

    print("\nPayout lag (resolvedTime − (closedTime or endDate))")
    _p("  ALL", payout_stats)
    _p("  YES", payout_yes_stats)
    _p("  NO ", payout_no_stats)

    # Save CSV of rows + a tiny summary JSON next to it
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    csv_name = f"lifecycle_rows_{days_back}d_{stamp}.csv"
    cols = ["market_id","slug","question","winner","createdAt","startDate","endDate","closedTime","resolvedTime","active_secs","payout_secs"]
    with open(csv_name, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in cols})
    print(f"\nSaved per-market rows → {csv_name}")

    summary = {
        "window_days": days_back,
        "generated_at": now_iso(),
        "active_days_stats": active_stats,
        "payout_days_stats_all": payout_stats,
        "payout_days_stats_yes": payout_yes_stats,
        "payout_days_stats_no": payout_no_stats,
    }
    json_name = f"lifecycle_summary_{days_back}d_{stamp}.json"
    with open(json_name, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"Saved summary stats → {json_name}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nbye.")