import os, json, time, requests
from collections import defaultdict, OrderedDict
from datetime import datetime, timezone
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"

def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET","POST"),
        raise_on_status=False,
    )
    ad = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update({"User-Agent": "yesno-monthly/1.0"})
    return s

SESSION = make_session()

def parse_dt_any(v):
    """Best-effort parse: ISO, with/without Z, or epoch seconds/millis."""
    if not v:
        return None
    # numeric timestamp?
    try:
        if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
            ts = float(v)
            if ts > 1e12:  # millis
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        pass
    # strings
    try:
        s = str(v).strip().replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None

def month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")

def is_yesno(m):
    outs = m.get("outcomes")
    if isinstance(outs, str):
        try: outs = json.loads(outs)
        except: outs = None
    if not (isinstance(outs, list) and len(outs) == 2):
        return False
    s = {str(x).strip().lower() for x in outs}
    return s == {"yes","no"}

def infer_resolution_and_winner(m: dict):
    """
    Returns (is_resolved: bool, winner: 'YES'|'NO'|None)
    Strategy:
      - Trust UMA status when it explicitly encodes a winner
      - Else, if closed and prices are extreme (≈1 vs ≈0), infer winner
      - Else, not resolved
    """
    uma = (m.get("umaResolutionStatus") or "").strip().lower()
    if uma in {"yes", "no"}:
        return True, uma.upper()
    if uma.startswith("resolved_"):
        w = uma.split("_", 1)[1].upper()
        if w in {"YES", "NO"}:
            return True, w

    if m.get("closed"):
        raw = m.get("outcomePrices", ["0", "0"])
        prices = json.loads(raw) if isinstance(raw, str) else raw
        try:
            y = float(prices[0]); n = float(prices[1])
            # Require *extreme* prices to infer resolution
            if y >= 0.98 and n <= 0.02:
                return True, "YES"
            if n >= 0.98 and y <= 0.02:
                return True, "NO"
        except Exception:
            pass
    print("NOT RESOLVED")
    return False, None

def resolved_datetime(m):
    """
    Best-effort 'resolution-ish' timestamp.
    Prefer closed/resolved fields, avoid start/end dates for resolution month.
    """
    for k in ("closedTime", "resolvedTime", "resolveTime", "updatedAt"):
        dt = parse_dt_any(m.get(k))
        if dt:
            return dt
    # last resort: endDate (still better than startDate for resolution)
    dt = parse_dt_any(m.get("endDate"))
    if dt:
        return dt
    return None

def fetch_page(offset, limit=250, since=None, until=None):
    params = {
        "limit": limit,
        "offset": offset,
        "closed": True,      # only closed markets
        "order": "startDate",
        "ascending": False,
    }
    # optional time filters: if you want to restrict via start date
    if since: params["start_date_min"] = since
    if until: params["start_date_max"] = until
    r = SESSION.get(BASE_GAMMA, params=params, timeout=30)
    r.raise_for_status()
    return r.json() or []

def monthly_yes_no(since_iso=None, until_iso=None):
    per_month = defaultdict(lambda: {"YES": 0, "NO": 0})

    offset, limit = 0, 250
    empty_streak = 0
    while True:
        page = fetch_page(offset, limit, since_iso, until_iso)
        if not page:
            empty_streak += 1
            if empty_streak >= 3:
                break
        else:
            empty_streak = 0

        for m in page:
            if not is_yesno(m):
                continue
            is_resolved, winner = infer_resolution_and_winner(m)
            if not is_resolved or winner is None:
                continue
            dt = resolved_datetime(m)
            if not dt:
                continue
            per_month[month_key(dt)][winner] += 1

        offset += limit
        time.sleep(0.30)  # polite pacing

    # sort by month
    return OrderedDict(sorted(per_month.items()))

def print_report(per_month):
    # headers
    print("Month     Total   YES   NO    %YES   %NO   CUM-YES  CUM-NO  CUM-%YES  CUM-%NO")
    cum_yes = cum_no = 0
    for mk, counts in per_month.items():
        y = counts["YES"]; n = counts["NO"]; t = y + n
        pct_yes = (y / t * 100.0) if t else 0.0
        pct_no  = (n / t * 100.0) if t else 0.0
        cum_yes += y; cum_no += n
        ct = cum_yes + cum_no
        cp_yes = (cum_yes / ct * 100.0) if ct else 0.0
        cp_no  = (cum_no  / ct * 100.0) if ct else 0.0
        print(f"{mk}  {t:5d}  {y:5d} {n:5d}  {pct_yes:5.1f}% {pct_no:5.1f}%  "
              f"{cum_yes:7d} {cum_no:6d}   {cp_yes:6.1f}%  {cp_no:6.1f}%")

def save_csv(per_month, path="yesno_monthly.csv"):
    with open(path, "w", encoding="utf-8") as f:
        f.write("month,total,yes,no,pct_yes,pct_no,cum_yes,cum_no,cum_pct_yes,cum_pct_no\n")
        cum_yes = cum_no = 0
        for mk, counts in OrderedDict(sorted(per_month.items())).items():
            y = counts["YES"]; n = counts["NO"]; t = y + n
            pct_yes = (y / t * 100.0) if t else 0.0
            pct_no  = (n / t * 100.0) if t else 0.0
            cum_yes += y; cum_no += n
            ct = cum_yes + cum_no
            cp_yes = (cum_yes / ct * 100.0) if ct else 0.0
            cp_no  = (cum_no  / ct * 100.0) if ct else 0.0
            f.write(f"{mk},{t},{y},{n},{pct_yes:.3f},{pct_no:.3f},{cum_yes},{cum_no},{cp_yes:.3f},{cp_no:.3f}\n")

if __name__ == "__main__":
    # If you want to limit the time window, set these to ISO strings like "2024-01-01T00:00:00Z"
    SINCE = None
    UNTIL = None

    stats = monthly_yes_no(SINCE, UNTIL)
    print_report(stats)
    save_csv(stats)  # writes yesno_monthly.csv next to the script