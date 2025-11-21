#!/usr/bin/env python3
import csv
import json
import time
import requests
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import argparse
import re

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"

# ------------------------
# HTTP session / retry
# ------------------------
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "polymarket-monthly-stats/1.0"})
    return s

SESSION = make_session()

def safe_get(url, *, params=None, timeout=(5, 20)):
    r = SESSION.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r

# ------------------------
# Time / parsing helpers
# ------------------------
def _parse_dt_any(v):
    if not v:
        return None
    # numeric timestamp?
    try:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(float(v), tz=timezone.utc)
        s = str(v).strip()
        if s.isdigit():
            return datetime.fromtimestamp(float(s), tz=timezone.utc)
    except Exception:
        pass

    # ISO-ish
    try:
        s = str(v).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


# ------------------------
# Robust resolution logic
# ------------------------
HINT_SPREAD = 0.98
FINAL_GRACE_SEC = 2 * 24 * 3600  # 2 days

def resolve_status(m: dict):
    """
    Return (resolved_bool, winner_str_or_None, source_tag)
    winner_str in {"YES","NO"} if resolved_bool is True.
    """
    # 1) UMA resolution
    uma = (m.get("umaResolutionStatus") or "").lower()
    if uma in ("yes", "no"):
        return True, uma.upper(), "umaResolutionStatus"
    if uma.startswith("resolved_"):
        w = uma.split("_", 1)[1].upper()
        if w in ("YES", "NO"):
            return True, w, "umaResolutionStatus"

    # 2) Direct winningOutcome / winner
    w = (m.get("winningOutcome") or m.get("winner") or "").upper()
    if w in ("YES", "NO"):
        return True, w, "winningOutcome"

    # 3) Heuristic from prices for closed markets
    if m.get("closed"):
        end_dt = (
            _parse_dt_any(m.get("closedTime"))
            or _parse_dt_any(m.get("umaEndDate"))
            or _parse_dt_any(m.get("endDate"))
            or _parse_dt_any(m.get("updatedAt"))
        )
        age_ok = True
        if end_dt is not None:
            age_ok = (datetime.now(timezone.utc) - end_dt).total_seconds() >= FINAL_GRACE_SEC

        raw = m.get("outcomePrices", ["0", "0"])
        prices = json.loads(raw) if isinstance(raw, str) else raw
        try:
            y, n = float(prices[0]), float(prices[1])
        except Exception:
            y, n = None, None

        if age_ok and y is not None and n is not None:
            # strong hint
            if y >= HINT_SPREAD and n <= 1 - HINT_SPREAD:
                return True, "YES", "terminal_prices"
            if n >= HINT_SPREAD and y <= 1 - HINT_SPREAD:
                return True, "NO", "terminal_prices"

        if y is not None and n is not None:
            # weaker 90/10 hint
            if y >= 0.90 and n <= 0.10:
                return True, "YES", "closed_hint_yes"
            if n >= 0.90 and y <= 0.10:
                return True, "NO", "closed_hint_no"

    return False, None, "unresolved"

def extract_categories(m: dict) -> list[str]:
    out = []

    # 1) top-level category (string)
    cat = m.get("category")
    if isinstance(cat, str) and cat.strip():
        out.append(cat)

    # 2) top-level categories (list of strings or objects)
    cats = m.get("categories") or []
    if isinstance(cats, (list, tuple)):
        for c in cats:
            if isinstance(c, str):
                out.append(c)
            elif isinstance(c, dict):
                label = c.get("label") or c.get("slug") or c.get("id")
                if label:
                    out.append(label)

    # 3) event-level categories (optional but useful)
    events = m.get("events") or []
    if isinstance(events, (list, tuple)):
        for ev in events:
            if not isinstance(ev, dict):
                continue
            # event.category (string)
            ev_cat = ev.get("category")
            if isinstance(ev_cat, str) and ev_cat.strip():
                out.append(ev_cat)

            # event.categories (list of strings or objects)
            ev_cats = ev.get("categories") or []
            if isinstance(ev_cats, (list, tuple)):
                for c in ev_cats:
                    if isinstance(c, str):
                        out.append(c)
                    elif isinstance(c, dict):
                        label = c.get("label") or c.get("slug") or c.get("id")
                        if label:
                            out.append(label)

    return out


# ------------------------
# Market fetcher
# ------------------------
def fetch_all_yes_no_markets():
    """
    Walk Gamma until exhaustion and return all YES/NO markets.
    """
    all_markets = []
    offset = 0
    limit = 500

    print("[FETCH] Starting full market scan (YES/NO only)")

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "outcomes": ["YES", "NO"],
            "sortBy": "startDate",
        }
        print(f"[FETCH] offset={offset} (so far={len(all_markets)})")
        r = safe_get(BASE_GAMMA, params=params, timeout=(5, 30))
        rows = r.json() or []

        if not rows:
            print("[FETCH] No more markets; done.")
            break

        all_markets.extend(rows)
        offset += len(rows)

        if len(rows) < limit:
            print("[FETCH] Last partial page; done.")
            break

        # small politeness pause
        time.sleep(0.2)

    print(f"[FETCH] Total markets fetched: {len(all_markets)}")
    return all_markets


# ------------------------
# Category handling
# ------------------------
def slugify_category(name: str) -> str:
    """
    Turn weird category names into safe column-friendly slugs.
    """
    s = name.strip().lower()
    # replace non alnum with underscore
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")
    return s or "uncategorized"


# ------------------------
# Aggregation
# ------------------------
def aggregate_by_month(markets):
    """
    Returns:
      month_stats: dict[month_key] -> stats dict
      all_cat_slugs: sorted list of all category slugs
      cat_slug_to_orig: slug -> first-seen original name
    """
    month_stats = {}
    all_cat_slugs = set()
    cat_slug_to_orig = {}

    for m in markets:
        resolved, winner, src = resolve_status(m)
        if not resolved or winner not in ("YES", "NO"):
            # skip unresolved/TBD markets for win-rate stats
            continue

        # month bucket by createdAt/startDate
        created_raw = m.get("createdAt") or m.get("startDate") or m.get("updatedAt")
        dt = _parse_dt_any(created_raw)
        if dt is None:
            continue
        month_key = dt.strftime("%Y-%m")

        stat = month_stats.setdefault(month_key, {
            "total_markets": 0,
            "yes_won_total": 0,
            "no_won_total": 0,
            "categories": {},  # slug -> {"yes": int, "no": int}
        })

        stat["total_markets"] += 1
        if winner == "YES":
            stat["yes_won_total"] += 1
        else:
            stat["no_won_total"] += 1

        cats = extract_categories(m)

        for c in cats:
            orig = str(c)
            slug = slugify_category(orig)
            all_cat_slugs.add(slug)
            cat_slug_to_orig.setdefault(slug, orig)

            cat_bucket = stat["categories"].setdefault(slug, {"yes": 0, "no": 0})
            if winner == "YES":
                cat_bucket["yes"] += 1
            else:
                cat_bucket["no"] += 1

            return month_stats, sorted(all_cat_slugs), cat_slug_to_orig


# ------------------------
# CSV writer
# ------------------------
def write_monthly_csv(month_stats, cat_slugs, out_path):
    """
    Columns:
      month,
      total_markets,
      yes_won_total,
      no_won_total,
      yes_percent_total,
      no_percent_total,
      <cat_slug>_yes,
      <cat_slug>_no,
      ...
    """
    # build header
    header = [
        "month",
        "total_markets",
        "yes_won_total",
        "no_won_total",
        "yes_percent_total",
        "no_percent_total",
    ]
    for slug in cat_slugs:
        header.append(f"{slug}_yes")
        header.append(f"{slug}_no")

    # sort months chronologically
    months_sorted = sorted(month_stats.keys())

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for month in months_sorted:
            stat = month_stats[month]
            total = stat["total_markets"]
            yes = stat["yes_won_total"]
            no = stat["no_won_total"]

            if total > 0:
                yes_pct = 100.0 * yes / total
                no_pct = 100.0 * no / total
            else:
                yes_pct = 0.0
                no_pct = 0.0

            row = [
                month,
                total,
                yes,
                no,
                f"{yes_pct:.4f}",
                f"{no_pct:.4f}",
            ]

            cats = stat["categories"]
            for slug in cat_slugs:
                cb = cats.get(slug, {"yes": 0, "no": 0})
                row.append(cb["yes"])
                row.append(cb["no"])

            writer.writerow(row)

    print(f"[CSV] Wrote monthly stats to {out_path}")


# ------------------------
# Main
# ------------------------
def main():
    parser = argparse.ArgumentParser(description="Polymarket monthly YES/NO stats by category")
    parser.add_argument("--out", type=str, default="polymarket_monthly_stats.csv",
                        help="Output CSV path")
    parser.add_argument("--sleep", type=float, default=0.0,
                        help="Extra sleep between pages (seconds, default 0)")
    args = parser.parse_args()

    markets = fetch_all_yes_no_markets()
    month_stats, cat_slugs, cat_slug_to_orig = aggregate_by_month(markets)
    write_monthly_csv(month_stats, cat_slugs, args.out)

    # print categories
    print("\n=== Categories observed (slug -> original) ===")
    for slug in sorted(cat_slugs):
        orig = cat_slug_to_orig.get(slug, slug)
        print(f"{slug}  ->  {orig}")


if __name__ == "__main__":
    main()
