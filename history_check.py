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

def resolved_winner(m):
    winner = (m.get("winningOutcome") or m.get("winner") or "").strip().upper()
    resolved = bool(m.get("resolved") or m.get("isResolved"))
    if resolved and winner in ("YES","NO"):
        return winner
    return None

def resolved_datetime(m):
    # prefer explicit resolution-ish fields
    for k in ("resolutionTime","resolvedTime","resolveTime","endDate","closeDate","startDate"):
        dt = parse_dt_any(m.get(k))
        if dt: return dt
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
    while True:
        page = fetch_page(offset, limit, since_iso, until_iso)
        if not page:
            break
        for m in page:
            if not is_yesno(m):
                continue
            w = resolved_winner(m)
            if not w:
                continue
            dt = resolved_datetime(m)
            if not dt:
                continue
            mk = month_key(dt)
            per_month[mk][w] += 1

        offset += limit
        time.sleep(0.30)  # polite pacing

    # sort by month
    ordered = OrderedDict(sorted(per_month.items()))
    return ordered

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
