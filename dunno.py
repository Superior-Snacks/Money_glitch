#!/usr/bin/env python3
import csv
import json
import time
import requests
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import argparse
from collections import defaultdict

# ------------------------
# Endpoints
# ------------------------
BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"

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
    s.headers.update({"User-Agent": "polymarket-no-buckets/1.0"})
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

# ------------------------
# Market fetcher
# ------------------------
def fetch_all_yes_no_markets(limit_per_page=500, extra_sleep=0.0, max_markets=None):
    """
    Walk Gamma until exhaustion and return all YES/NO markets.
    """
    all_markets = []
    offset = 0

    print("[FETCH] Starting full market scan (YES/NO only)")

    while True:
        if max_markets is not None and len(all_markets) >= max_markets:
            print(f"[FETCH] Reached max_markets={max_markets}, stopping.")
            break

        params = {
            "limit": limit_per_page,
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

        if len(rows) < limit_per_page:
            print("[FETCH] Last partial page; done.")
            break

        if extra_sleep > 0:
            time.sleep(extra_sleep)

    print(f"[FETCH] Total markets fetched: {len(all_markets)}")
    return all_markets

# ------------------------
# Trade fetcher
# ------------------------
def fetch_trades_for_market(cid: str, *, max_pages=None, page_limit=250, sleep=0.0):
    """
    Fetch all trades for a given market conditionId via DATA_TRADES.

    Assumes Polymarket trades API supports:
      ?market=<cid>&sort=asc&limit=<page_limit>&offset=<offset>

    Adjust if your local tests show a different schema.
    """
    trades = []
    offset = 0
    page = 0

    while True:
        if max_pages is not None and page >= max_pages:
            break

        params = {
            "market": cid,
            "sort": "asc",
            "limit": page_limit,
            "offset": offset,
        }

        try:
            r = safe_get(DATA_TRADES, params=params, timeout=(5, 20))
        except Exception as e:
            print(f"[TRADES] Error fetching trades for {cid} at offset={offset}: {e}")
            break

        payload = r.json() or {}

        # depending on the API, adjust this extraction:
        rows = payload if isinstance(payload, list) else payload.get("data") or payload.get("trades") or []
        if not rows:
            break

        trades.extend(rows)
        got = len(rows)
        offset += got
        page += 1

        if got < page_limit:
            break

        if sleep > 0:
            time.sleep(sleep)

    return trades

# ------------------------
# Price extraction from trades
# ------------------------
def parse_float(x):
    try:
        return float(x)
    except Exception:
        return None

def derive_no_vwap_from_trades(trades):
    """
    Derive a VWAP NO price from trades assuming 'price' is YES price in [0,1]
    and 'size' is the notional or shares.

    NO_VWAP = 1 - YES_VWAP
    """
    num = 0.0
    den = 0.0
    for t in trades:
        p = parse_float(t.get("price"))
        if p is None:
            continue
        sz = parse_float(t.get("size", 1.0))
        if sz is None:
            sz = 1.0
        num += p * sz
        den += sz

    if den == 0:
        return None

    yes_vwap = num / den
    no_vwap = 1.0 - yes_vwap
    return no_vwap

# ------------------------
# Bucketing logic
# ------------------------
def make_bucket(price: float, width: float) -> str:
    """
    Map a price in [0,1] to a bucket label like '0.30-0.35'.
    """
    if price is None:
        return None
    if price < 0.0 or price > 1.0:
        return None
    idx = int(price / width)
    lo = idx * width
    hi = min(1.0, lo + width)
    return f"{lo:.2f}-{hi:.2f}"

def bucket_no_edge(rows, bucket_width: float, out_path: str):
    """
    rows: iterable of dicts with keys:
      - 'no_price'
      - 'winner' ("YES" or "NO")
    """
    buckets = defaultdict(lambda: {"count": 0, "no_wins": 0, "yes_wins": 0, "sum_price": 0.0})

    total_rows = 0
    skipped_rows = 0

    for row in rows:
        total_rows += 1
        p = row.get("no_price")
        winner = (row.get("winner") or "").upper()

        if p is None:
            skipped_rows += 1
            continue

        b = make_bucket(p, bucket_width)
        if b is None:
            skipped_rows += 1
            continue

        if winner not in ("YES", "NO"):
            skipped_rows += 1
            continue

        st = buckets[b]
        st["count"] += 1
        st["sum_price"] += p
        if winner == "NO":
            st["no_wins"] += 1
        else:
            st["yes_wins"] += 1

    bucket_keys = sorted(
        buckets.keys(),
        key=lambda b: float(b.split("-")[0])
    )

    with open(out_path, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.writer(f_out)
        writer.writerow([
            "bucket",
            "markets",
            "no_wins",
            "yes_wins",
            "no_win_rate",
            "avg_no_price",
            "EV_per_share_if_buy_NO",        # win_rate - avg_price
            "return_pct_on_stake_if_NO(%)",  # (win_rate - avg_price)/avg_price * 100
        ])

        for b in bucket_keys:
            st = buckets[b]
            n = st["count"]
            if n == 0:
                continue
            no_wins = st["no_wins"]
            yes_wins = st["yes_wins"]
            win_rate = no_wins / n
            avg_price = st["sum_price"] / n if n > 0 else 0.0

            ev_per_share = win_rate - avg_price
            if avg_price > 0:
                ret_pct = (ev_per_share / avg_price) * 100.0
            else:
                ret_pct = 0.0

            writer.writerow([
                b,
                n,
                no_wins,
                yes_wins,
                f"{win_rate:.4f}",
                f"{avg_price:.4f}",
                f"{ev_per_share:.4f}",
                f"{ret_pct:.2f}",
            ])

    print(f"[BUCKET] Processed {total_rows} rows, skipped {skipped_rows} bad/invalid rows.")
    print(f"[BUCKET] Wrote bucket stats to {out_path}")

# ------------------------
# Main pipeline
# ------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Fetch Polymarket markets+trades and bucket NO edge by price."
    )
    parser.add_argument("--bucket-width", type=float, default=0.05,
                        help="Price bucket width (default 0.05)")
    parser.add_argument("--out", default="no_buckets.csv",
                        help="Output CSV for bucket stats")
    parser.add_argument("--market-sleep", type=float, default=0.2,
                        help="Sleep between market pages (Gamma) in seconds")
    parser.add_argument("--trade-sleep", type=float, default=0.0,
                        help="Sleep between trade pages in seconds")
    parser.add_argument("--max-markets", type=int, default=None,
                        help="Optional cap on number of markets (for testing)")
    parser.add_argument("--max-trade-pages", type=int, default=None,
                        help="Optional cap on pages per market for trades (for testing)")
    args = parser.parse_args()

    # 1) Fetch markets
    markets = fetch_all_yes_no_markets(extra_sleep=args.market_sleep,
                                       max_markets=args.max_markets)

    # 2) For each resolved market, fetch trades and derive NO VWAP
    per_market_rows = []
    total_markets = 0
    used_markets = 0

    for m in markets:
        total_markets += 1
        resolved, winner, src = resolve_status(m)
        if not resolved or winner not in ("YES", "NO"):
            continue

        cid = m.get("conditionId") or m.get("id")
        if not cid:
            continue

        trades = fetch_trades_for_market(
            cid,
            max_pages=args.max_trade_pages,
            sleep=args.trade_sleep,
        )
        if not trades:
            continue

        no_price = derive_no_vwap_from_trades(trades)
        if no_price is None:
            continue

        per_market_rows.append({
            "no_price": no_price,
            "winner": winner,
        })
        used_markets += 1

        if used_markets % 1000 == 0:
            print(f"[PIPE] Processed {used_markets} resolved markets with trades...")

    print(f"[PIPE] Total markets seen: {total_markets}")
    print(f"[PIPE] Markets with resolution + trades + price: {used_markets}")

    # 3) Bucket and write CSV
    bucket_no_edge(per_market_rows, bucket_width=args.bucket_width, out_path=args.out)

if __name__ == "__main__":
    main()
