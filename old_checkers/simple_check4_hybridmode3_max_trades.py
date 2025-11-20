import json
import time
import requests
from datetime import datetime, timezone, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import re
import glob
import argparse

# ======================================================
#  CONFIG: tweak these in ONE place (defaults)
# ======================================================
BET_SIZE     = 100.0     # dollars per market
CAP_NO       = 0.40      # maker NO cap
FEE_BPS      = 0         # 600 = 6.00%
SLIP_BPS     = 0         # 200 = 2.00%
START_BANK   = 5000.0
BATCH_LIMIT  = 50        # (legacy, no longer used for reporting)
MIN_BANK     = 10.0      # stop if bank below this

# How often to print a global summary (in number of markets processed)
GLOBAL_REPORT_INTERVAL = 400
TRADES_PAGE_LIMIT = 250
MAX_TRADE_PAGES = 1000

BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"

LOGS_DIR             = "logs"
STATUS_REPORT_PATH   = "status_report_saved_markets.txt"
RUN_LOG_PATH         = "look_saved_markets.txt"

# Starting point for "max" historical lookback
POLYMARKET_INCEPTION = 1590969600  # ~2020-06-01T00:00:00Z

# ======================================================
#  GLOBAL STATUS SNAPSHOT (for soft interrupt)
# ======================================================

STATUS = {
    "bank": START_BANK,
    "idx": 0,
    "total_pl": 0.0,
    "total_markets_done": 0,
    "total_spent": 0.0,
    "total_reports": 0,
    "last_report_pl": 0.0,
    "last_report_markets": 0,
    "last_market_cid": None,
    "last_market_question": None,
    "last_time_found": None,
    "bet_size": BET_SIZE,
    "cap_no": CAP_NO,
    "fee_bps": FEE_BPS,
    "slip_bps": SLIP_BPS,
    "last_update_ts": None,
}

# For two-level Ctrl+C: first = soft snapshot, second = hard exit
SOFT_INTERRUPTED_ONCE = False

def update_status(
    *,
    bank=None,
    idx=None,
    total_pl=None,
    total_markets_done=None,
    total_spent=None,
    total_reports=None,
    last_report_pl=None,
    last_report_markets=None,
    last_market_cid=None,
    last_market_question=None,
    last_time_found=None,
):
    if bank is not None:
        STATUS["bank"] = bank
    if idx is not None:
        STATUS["idx"] = idx
    if total_pl is not None:
        STATUS["total_pl"] = total_pl
    if total_markets_done is not None:
        STATUS["total_markets_done"] = total_markets_done
    if total_spent is not None:
        STATUS["total_spent"] = total_spent
    if total_reports is not None:
        STATUS["total_reports"] = total_reports
    if last_report_pl is not None:
        STATUS["last_report_pl"] = last_report_pl
    if last_report_markets is not None:
        STATUS["last_report_markets"] = last_report_markets
    if last_market_cid is not None:
        STATUS["last_market_cid"] = last_market_cid
    if last_market_question is not None:
        STATUS["last_market_question"] = last_market_question
    if last_time_found is not None:
        STATUS["last_time_found"] = last_time_found

    STATUS["last_update_ts"] = datetime.now(timezone.utc).isoformat()


def write_status_report(path: str = STATUS_REPORT_PATH):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    s = STATUS.copy()
    lines = [
        "================= STATUS SNAPSHOT =================",
        f"timestamp             : {datetime.now(timezone.utc).isoformat()}",
        "",
        "CONFIG:",
        f"  BET_SIZE            : {s['bet_size']}",
        f"  CAP_NO              : {s['cap_no']}",
        f"  FEE_BPS             : {s['fee_bps']}",
        f"  SLIP_BPS            : {s['slip_bps']}",
        "",
        "ACCOUNT / P&L:",
        f"  bank                : {s['bank']:.2f}",
        f"  total P/L           : {s['total_pl']:.2f}",
        f"  total spent         : {s['total_spent']:.2f}",
        "",
        "MARKET PROGRESS:",
        f"  index               : {s['idx']}",
        f"  total done          : {s['total_markets_done']}",
        f"  total reports       : {s['total_reports']}",
        "",
        "LAST MARKET:",
        f"  last cid            : {s['last_market_cid']}",
        f"  last question       : {s['last_market_question']}",
        f"  last time_found     : {s['last_time_found']}",
        "",
        "LAST REPORT:",
        f"  last report P/L     : {s['last_report_pl']:.2f}",
        f"  last report markets : {s['last_report_markets']}",
        "===================================================",
        "",
    ]
    text = "\n".join(lines)
    with open(path, "a", encoding="utf-8") as f:
        f.write(text)
    print(f"[SOFT INTERRUPT] Status snapshot appended to {path}")

# ======================================================
#  GLOBAL RATE LIMITER + SESSION
# ======================================================

# Global RPS target (can be overridden via --rps)
RPS_TARGET           = 1.0
_RPS_SCALE           = 1.0
_RPS_MIN             = 0.2
_RPS_RECOVER_PER_SEC = 0.03
_last_tokens_ts      = time.monotonic()
_bucket              = RPS_TARGET
_num_429             = 0

def make_session():
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "research-bot/1.0"})
    return s

SESSION = make_session()

def _rate_limit():
    global _last_tokens_ts, _bucket, _RPS_SCALE
    now = time.monotonic()
    rps_now = max(RPS_TARGET * _RPS_SCALE, 0.1)
    # refill
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
    global _RPS_SCALE, _num_429
    _num_429 += 1
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
        return 0.0

def http_get_with_backoff(url, *, params=None, timeout=(5, 20), max_tries=8):
    back = 0.5
    tries = 0
    last_t = time.monotonic()

    while True:
        _rate_limit()
        try:
            r = SESSION.get(url, params=params or {}, timeout=timeout)
        except requests.RequestException as e:
            print(f"      [HTTP EXC] {e} → sleep {back:.1f}s")
            time.sleep(back)
            back = min(back * 1.7, 20.0)
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
            sleep_s = max(ra, back)
            print(f"      [429] rate limited → sleep {sleep_s:.1f}s (tries={tries+1})")
            time.sleep(sleep_s)
            back = min(back * 1.8, 30.0)
            tries += 1
            if tries >= max_tries:
                r.raise_for_status()
            continue

        if 500 <= r.status_code < 600:
            print(f"      [5xx={r.status_code}] → sleep {back:.1f}s (tries={tries+1})")
            time.sleep(back)
            back = min(back * 1.7, 20.0)
            tries += 1
            if tries >= max_tries:
                r.raise_for_status()
            continue

        print(f"      [HTTP {r.status_code}] fatal")
        r.raise_for_status()

def safe_get(url, *, params=None, timeout=(5, 20)):
    return http_get_with_backoff(url, params=params, timeout=timeout)

# ======================================================
#  SimMarket: maker-at-cap NO (same logic)
# ======================================================

class SimMarket:
    """
    blocks: list of dicts with keys:
      time, side ("yes"|"no"), price_yes, price_no,
      notional_yes, notional_no, shares
    """
    def __init__(self, blocks, fee_bps=0, slip_bps=20):
        self.blocks = sorted(blocks, key=lambda b: b["time"])
        self.fee  = fee_bps / 10000.0
        self.slip = slip_bps / 10000.0

    def take_first_no(self, t_from, dollars=100.0, max_no_price=None):
        """
        MAKER-at-cap NO:

        - Look at all NO-side blocks with price_no <= cap and time >= t_from
        - Convert those to available SHARES
        - You place a limit NO at cap; you can buy up to dollars/cap shares
        - Cost is shares * cap
        """
        if isinstance(t_from, datetime):
            t_from_ts = int(t_from.replace(tzinfo=timezone.utc).timestamp())
        else:
            t_from_ts = int(t_from)

        cap = 1.0 if max_no_price is None else float(max_no_price)

        eligible = []
        total_shares = 0.0

        for b in self.blocks:
            if int(b.get("time", 0)) < t_from_ts:
                continue
            if b.get("side") != "no":
                continue

            p_no = float(b.get("price_no", 0.0))
            sh   = float(b.get("shares", 0.0))
            if sh <= 0.0:
                continue
            if p_no > cap:
                continue

            eligible.append(b)
            total_shares += sh

        if total_shares <= 0.0:
            print("    [SIM] No NO-side liquidity under cap; no fill.")
            return 0.0, 0.0, 0.0, []

        max_shares_by_bank = dollars / cap
        target_shares = min(total_shares, max_shares_by_bank)

        spent_pre   = target_shares * cap
        spent_after = spent_pre * (1.0 + self.fee + self.slip)
        avg_no      = spent_after / target_shares

        fills = []
        remaining = target_shares

        for b in eligible:
            if remaining <= 0:
                break
            block_sh = float(b.get("shares", 0.0))
            if block_sh <= 0:
                continue

            take_sh = min(block_sh, remaining)
            if take_sh <= 0:
                continue

            p_no  = float(b.get("price_no", 0.0))
            p_yes = float(b.get("price_yes", 0.0))

            fills.append({
                "time": b["time"],
                "side": "no",
                "price_no": p_no,
                "price_yes": p_yes,
                "take_shares": take_sh,
                "take_notional_at_cap": take_sh * cap,
                "block": b,
            })

            remaining -= take_sh

        print(f"    [SIM] maker NO fill: shares={target_shares:.2f}, spent_after={spent_after:.2f}, avg={avg_no:.4f}")
        return target_shares, spent_after, avg_no, fills

# ======================================================
#  Read markets from folder: markets_YYYY-MM-DD.jsonl
# ======================================================

def parse_iso(value):
    if not value:
        return None
    s = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def read_unique_markets_from_folder(folder_name: str):
    """
    Reads logs/<folder_name>/markets_*.jsonl
    Lines look like:
      {"time_found": "...", "conditionId": "...", "question": "..."}

    Returns a list of dicts:
      {"conditionId", "question", "time_found", "time_found_epoch"}
    deduped by conditionId, keeping earliest time_found.
    """
    base_dir = os.path.join(LOGS_DIR, folder_name)
    pattern = os.path.join(base_dir, "markets_*.jsonl")
    paths = sorted(glob.glob(pattern))

    uniq = {}
    total_lines = 0
    bad_lines = 0

    if not paths:
        print(f"[read_unique_markets_from_folder] No files found at {pattern}")

    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    total_lines += 1
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        bad_lines += 1
                        continue

                    cid = rec.get("conditionId")
                    q   = rec.get("question")
                    tf  = rec.get("time_found")
                    if not cid or not q or not tf:
                        continue

                    dt = parse_iso(tf)
                    if not dt:
                        continue
                    ts = int(dt.timestamp())

                    old = uniq.get(cid)
                    if (old is None) or (ts < old["time_found_epoch"]):
                        uniq[cid] = {
                            "conditionId": cid,
                            "question": q,
                            "time_found": tf,
                            "time_found_epoch": ts,
                        }
        except FileNotFoundError:
            continue

    markets = list(uniq.values())
    markets.sort(key=lambda x: x["time_found_epoch"])

    print(f"[READ] files={len(paths)} lines={total_lines} unique_markets={len(markets)} bad_lines={bad_lines}")
    return markets

# ======================================================
#  Historical market discovery via Gamma
# ======================================================

def _to_epoch_any(x):
    try:
        f = float(x)
        return int(f / 1000) if f > 1e12 else int(f)
    except Exception:
        pass

    if isinstance(x, str):
        s = x.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return None
    return None


def fetch_markets_since(start_epoch: int):
    """
    Historical mode: walk Gamma with sortBy=startDate and outcomes YES/NO,
    keep markets whose created/startDate >= start_epoch.
    Returns a list of dicts with:
      {"conditionId","question","createdAt","created_epoch"}
    """
    out = []
    offset = 0
    limit = 500

    print(f"{datetime.now(timezone.utc).isoformat()} [HIST] Fetching markets since epoch={start_epoch}")

    while True:
        print(f"   [HIST] Fetching offset={offset} (found={len(out)})")
        params = {
            "limit": limit,
            "offset": offset,
            "outcomes": ["YES", "NO"],
            "sortBy": "startDate",
        }
        r = safe_get(BASE_GAMMA, params=params, timeout=(5, 20))
        try:
            r.raise_for_status()
        except Exception as e:
            print(f"   [HIST] HTTP error at offset={offset}: {e}")
            break

        rows = r.json() or []
        if not rows:
            print("   [HIST] No more markets returned by API.")
            break

        kept_this_page = 0
        for m in rows:
            cid = m.get("conditionId")
            if not cid:
                continue
            q = m.get("question")

            created_raw = m.get("createdAt") or m.get("startDate") or m.get("updatedAt")
            created_ts = _to_epoch_any(created_raw)
            if created_ts is None:
                continue
            if created_ts < start_epoch:
                continue

            out.append({
                "conditionId": cid,
                "question": q,
                "createdAt": created_raw,
                "created_epoch": created_ts,
            })
            kept_this_page += 1

        print(f"   [HIST] got={len(rows)} kept={kept_this_page} total_kept={len(out)}")
        offset += len(rows)
        if len(rows) < limit:
            print("   [HIST] Reached end of market list.")
            break

    out.sort(key=lambda m: m["created_epoch"])
    print(f"[HIST] Total markets kept: {len(out)}")
    return out

# ======================================================
#  Market meta + trades
# ======================================================

MARKET_META_CACHE = {}

def fetch_market_meta(cid: str):
    if cid in MARKET_META_CACHE:
        return MARKET_META_CACHE[cid]
    try:
        print(f"    [META] fetching meta for cid={cid[:10]}…")
        r = safe_get(
            BASE_GAMMA,
            params={"condition_ids": cid, "limit": 1},
            timeout=(5, 20),
        )
        r.raise_for_status()
        rows = r.json() or []
        m = {}
        for row in rows:
            if row.get("conditionId") == cid:
                m = row
                break
        MARKET_META_CACHE[cid] = m
        print(f"    [META] received={bool(m)} for cid={cid[:10]}")
        return m
    except Exception as e:
        print(f"    [META WARN] {cid[:10]}… {e}")
        MARKET_META_CACHE[cid] = {}
        return {}


def fetch_trades(market_dict):
    """
    Fetch ALL trades for a market using pagination.

    - Uses limit/offset over DATA_TRADES
    - Ensures uniqueness via a stable trade key (no offset)
    - Stops if:
        * page is empty
        * page size < limit (end of stream)
        * page has 0 new trades (stagnation)
        * page_num hits MAX_TRADE_PAGES
    """
    cid = market_dict["conditionId"]
    print(f"    [TRADES] fetching ALL trades cid={cid[:10]}…")

    offset = 0
    limit = TRADES_PAGE_LIMIT
    all_trades = []
    seen_ids = set()
    page_num = 0

    while page_num < MAX_TRADE_PAGES:
        params = {
            "market": cid,
            "sort": "asc",
            "limit": limit,
            "offset": offset,
        }
        print(f"    [TRADES] page {page_num} offset={offset} limit={limit}")
        try:
            resp = safe_get(DATA_TRADES, params=params, timeout=(5, 20))
            resp.raise_for_status()
        except Exception as e:
            print(f"    [TRADES WARN] {cid[:10]}… HTTP error on page {page_num}: {e}")
            break

        try:
            page = resp.json() or []
        except Exception as e:
            print(f"    [TRADES WARN] {cid[:10]}… JSON decode error on page {page_num}: {e}")
            break

        n_page = len(page)
        print(f"    [TRADES] page {page_num} got {n_page} trades")

        if not page:
            print(f"    [TRADES] page {page_num} empty → end of stream")
            break

        added = 0
        for t in page:
            # Build a stable key that does NOT depend on offset
            tid = t.get("id")
            if not tid:
                tid = (
                    f"{t.get('market')}-"
                    f"{t.get('match_time') or t.get('timestamp') or t.get('time')}-"
                    f"{t.get('price')}-"
                    f"{t.get('size')}-"
                    f"{t.get('side')}-"
                    f"{t.get('outcome')}"
                )

            if tid in seen_ids:
                continue
            seen_ids.add(tid)
            all_trades.append(t)
            added += 1

        print(
            f"    [TRADES] page {page_num} added {added} new trades "
            f"(total={len(all_trades)})"
        )

        # If we got a full page but 0 new trades, we're looping/repeating
        if added == 0:
            print(
                f"    [TRADES] page {page_num} had 0 new trades "
                f"(possible repetition) → stopping to avoid infinite loop"
            )
            break

        # If page smaller than limit, end of stream
        if n_page < limit:
            print(
                f"    [TRADES] page {page_num} size < limit "
                f"({n_page} < {limit}) → end of stream"
            )
            break

        offset += limit
        page_num += 1

    if page_num >= MAX_TRADE_PAGES:
        print(
            f"    [TRADES] reached MAX_TRADE_PAGES={MAX_TRADE_PAGES} for cid={cid[:10]} → "
            "stopping pagination for safety"
        )

    print(f"    [TRADES] DONE cid={cid[:10]} total_trades={len(all_trades)}")

    # Keep behaviour: sorted by timestamp ascending
    try:
        all_trades.sort(key=lambda t: t["timestamp"])
    except Exception:
        pass

    return all_trades


# ======================================================
#  Robust market resolution
# ======================================================

HINT_SPREAD = 0.98
FINAL_GRACE = timedelta(days=2)

def _parse_dt_any(v):
    if not v:
        return None
    try:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(float(v), tz=timezone.utc)
        if isinstance(v, str) and v.strip().isdigit():
            return datetime.fromtimestamp(float(v), tz=timezone.utc)
    except Exception:
        pass

    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def resolve_status(m: dict):
    """
    Return (resolved_bool, winner_str_or_None, source_tag)
    winner_str is "YES" or "NO" if resolved_bool is True.
    """
    uma = (m.get("umaResolutionStatus") or "").lower()
    if uma in ("yes", "no"):
        return True, uma.upper(), "umaResolutionStatus"
    if uma.startswith("resolved_"):
        w = uma.split("_", 1)[1].upper()
        if w in ("YES", "NO"):
            return True, w, "umaResolutionStatus"

    w = (m.get("winningOutcome") or m.get("winner") or "").upper()
    if w in ("YES", "NO"):
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
        prices = json.loads(raw) if isinstance(raw, str) else raw
        try:
            y, n = float(prices[0]), float(prices[1])
        except Exception:
            y, n = None, None

        if age_ok and y is not None and n is not None:
            if y >= HINT_SPREAD and n <= 1 - HINT_SPREAD:
                return True, "YES", "terminal_prices"
            if n >= HINT_SPREAD and y <= 1 - HINT_SPREAD:
                return True, "NO", "terminal_prices"

        if y is not None and n is not None:
            if y >= 0.90 and n <= 0.10:
                return True, "YES", "closed_hint_yes"
            if n >= 0.90 and y <= 0.10:
                return True, "NO", "closed_hint_no"

    return False, None, "unresolved"


def current_status(m: dict) -> str:
    resolved, winner, src = resolve_status(m)
    print(f"    [STATUS] resolved={resolved} winner={winner} source={src}")
    if resolved and winner in ("YES", "NO"):
        return winner
    return "TBD"

# ======================================================
#  Time + trade normalization (SIMPLE, no aggregation)
# ======================================================

def normalize_time(value, default=None):
    if value is None or value == "":
        return default

    if isinstance(value, (int, float)) or re.match(r"^\d{10,13}$", str(value)):
        try:
            ts = float(value)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return default

    val = str(value).strip()
    val = val.replace("Z", "+00:00")
    val = re.sub(r"\s+", "T", val)

    if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
        val += "T00:00:00+00:00"

    try:
        dt = datetime.fromisoformat(val)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return default


def clamp01(x, eps=1e-6):
    return min(1.0 - eps, max(eps, float(x)))


def snap_price(p, tick=0.01):
    return round(round(float(p) / tick) * tick, 2)


def take_yes(trade):
    out = trade["outcome"].lower()
    side = trade["side"].lower()
    return (out == "no" and side == "sell") or (out == "yes" and side == "buy")


def take_no(trade):
    out = trade["outcome"].lower()
    side = trade["side"].lower()
    return (out == "yes" and side == "sell") or (out == "no" and side == "buy")


def notion_yes(trade):
    return float(trade["size"]) * float(trade["price"])


def notion_no(trade):
    return float(trade["size"]) * (1 - float(trade["price"]))


def valid_trade(trade, min_spend=2, extreme_price=0.05, min_extreme_notional=20.0):
    if not trade:
        return False

    price = float(trade["price"])
    size = float(trade["size"])
    outcome = str(trade.get("outcome", "")).strip().lower()

    if outcome == "yes":
        cost = price * size
    elif outcome == "no":
        cost = (1 - price) * size
    else:
        return False

    if cost < min_spend:
        return False

    if price < extreme_price or price > 1.0 - extreme_price:
        return cost >= min_extreme_notional

    return True


def normalize_trades(trades):
    if not trades:
        print("    [TRADES] ERROR NO TRADES AVAILABLE")
        return []

    trades = sorted(trades, key=lambda t: t["timestamp"])

    blocks = []
    for tr in trades:
        if not valid_trade(tr):
            continue

        try:
            ts = int(float(tr["timestamp"]))
        except Exception:
            continue

        p_yes_raw = snap_price(tr["price"], 0.01)
        p_no_raw  = round(1.0 - p_yes_raw, 6)

        if take_yes(tr):
            side = "yes"
        elif take_no(tr):
            side = "no"
        else:
            continue

        size = float(tr["size"])
        if size <= 0:
            continue

        if side == "yes":
            notional_yes = size * p_yes_raw
            notional_no  = 0.0
        else:
            notional_no  = size * p_no_raw
            notional_yes = 0.0

        blocks.append({
            "time": ts,
            "side": side,
            "price_yes": round(p_yes_raw, 6),
            "price_no":  round(p_no_raw, 6),
            "shares": size,
            "notional_yes": round(notional_yes, 6),
            "notional_no":  round(notional_no, 6),
            "compressed": 1,
        })

    print(f"    [TRADES] normalized: raw={len(trades)} valid_blocks={len(blocks)}")
    return sorted(blocks, key=lambda b: b["time"])

# ======================================================
#  Misc helpers
# ======================================================

def write_to_file(filename, data):
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
    with open(filename, "a", encoding="utf-8") as file:
        file.seek(0, os.SEEK_END)
        if file.tell() > 0:
            file.write("\n")
        file.write(data + "\n")

# ======================================================
#  COMMON MARKET SIM LOGIC (used by both modes)
# ======================================================

def simulate_market_maker_no(cid, question, trades_raw, bank, all_pl, cap_no, fee_bps, slip_bps):
    """
    Shared core: given cid, question, raw trades, and current bank,
    run maker-at-cap NO, resolve status, and return:
      (pnl_for_this_market, spent_after, shares, status, won_bool, avg_price)
    If trades or status invalid, returns zeros and status indicator.
    """
    if not trades_raw:
        print("    [SIM] No raw trades; skipping market.")
        return 0.0, 0.0, 0.0, "NOTRADES", False, 0.0

    trades = normalize_trades(trades_raw)
    if not trades:
        print("    [SIM] No valid blocks after normalization; skipping.")
        return 0.0, 0.0, 0.0, "NOBLOCKS", False, 0.0

    # start from first trade
    t_from = trades[0]["time"]

    if bank < MIN_BANK:
        print("    [SIM] Bank below MIN_BANK inside sim; skipping.")
        return 0.0, 0.0, 0.0, "LOWBANK", False, 0.0

    # bet is determined by current bank + not-yet-applied batch pnl
    if bank >= BET_SIZE:
        bet = BET_SIZE
    else:
        bet = float(bank)

    print(f"    [SIM] starting maker NO sim: bet={bet:.2f}, cap={cap_no:.3f}")
    sim = SimMarket(trades, fee_bps=fee_bps, slip_bps=slip_bps)
    shares, spent_after, avg_, fills = sim.take_first_no(
        t_from, dollars=bet, max_no_price=cap_no
    )

    if shares == 0.0 or spent_after == 0.0:
        print("    [SIM] No fill under cap; skipping.")
        return 0.0, 0.0, 0.0, "NOFILL", False, 0.0

    full_meta = fetch_market_meta(cid)
    status = current_status(full_meta)

    if status not in ("YES", "NO"):
        print(f"    [SIM] Unresolved/TBD status={status}; skipping P/L.")
        return 0.0, 0.0, shares, status, False, avg_

    # We always simulate BUY NO; profit depends on whether NO actually won
    won = (status == "NO")
    pnl = (shares - spent_after) if won else (-spent_after)

    print(f"    [SIM] result: status={status}, won={won}, pnl={pnl:.2f}")
    return pnl, spent_after, shares, status, won, avg_

# ======================================================
#  GLOBAL REPORT LOGIC
# ======================================================

def print_and_log_report(
    mode_label,
    report_idx,
    markets_since_report,
    total_done,
    report_pl,
    total_pl,
    bank,
    spent,
    extra_label,
):
    print("-" * 61)
    print(
        f"[{mode_label}] report #{report_idx} | markets_this_report: {markets_since_report} "
        f"| total_markets_done: {total_done} | report P/L: {report_pl:.2f} "
        f"| total P/L: {total_pl:.2f} | bank: {bank:.2f} {extra_label}"
    )
    print("-" * 61)

    write_to_file(
        RUN_LOG_PATH,
        f"[{mode_label}] report:{report_idx} | markets_total:{total_done} | "
        f"markets_this_report:{markets_since_report} | total spent {spent:.2f} "
        f"| report P/L: {report_pl:.2f} | total P/L: {total_pl:.2f} "
        f"| bank: {bank:.2f} {extra_label}"
    )

# ======================================================
#  MAIN SIM LOGIC: CURRENT MODE (saved markets)
# ======================================================

def run_from_saved_markets(folder: str | None):
    global SOFT_INTERRUPTED_ONCE

    if not folder:
        folder = input("Folder name under logs/ (e.g. curr): ").strip()
    if not folder:
        print("No folder given, exiting.")
        return

    markets = read_unique_markets_from_folder(folder)
    if not markets:
        print("No markets found in that folder, exiting.")
        return

    total_markets = len(markets)
    print(f"Loaded {total_markets} unique markets from logs/{folder}")

    bank = START_BANK
    idx = 0
    all_pl = 0.0
    all_done = 0          # total markets *visited*
    spent = 0.0

    report_idx = 0
    markets_since_report = 0
    pl_since_report = 0.0

    update_status(
        bank=bank,
        idx=idx,
        total_pl=all_pl,
        total_markets_done=all_done,
        total_spent=spent,
        total_reports=report_idx,
    )

    print(f"START (CURRENT): bank={bank:.2f}, BET_SIZE={BET_SIZE}, CAP_NO={CAP_NO}")
    print(f"Total markets to process: {total_markets}")
    print(f"Reporting every {GLOBAL_REPORT_INTERVAL} markets.")

    while idx < total_markets and bank >= MIN_BANK:
        try:
            m_meta = markets[idx]
            cid = m_meta["conditionId"]
            q   = m_meta["question"]
            tf  = m_meta["time_found"]

            print(f"\n[CUR #{idx+1}/{total_markets}] {q} (cid={cid[:10]}…) time_found={tf}")

            market_obj = {"conditionId": cid, "question": q}

            all_done += 1
            markets_since_report += 1
            last_time_found = tf

            trades_raw = fetch_trades(market_obj)

            pnl, spent_after, shares, status, won, avg_ = simulate_market_maker_no(
                cid, q, trades_raw, bank + pl_since_report, all_pl, CAP_NO, FEE_BPS, SLIP_BPS
            )

            # If status indicates no fill / unresolved, just move on (still counts as visited)
            if status not in ("YES", "NO"):
                idx += 1
                update_status(
                    bank=bank,
                    idx=idx,
                    total_pl=all_pl + pl_since_report,
                    total_markets_done=all_done,
                    total_spent=spent,
                    total_reports=report_idx,
                    last_report_pl=pl_since_report,
                    last_report_markets=markets_since_report,
                    last_market_cid=cid,
                    last_market_question=q,
                    last_time_found=tf,
                )
                continue

            # Apply P/L
            bank += pnl
            pl_since_report += pnl
            spent += spent_after

            running_pl_global = all_pl + pl_since_report

            print(q)
            print(
                f"shares={shares:.2f} | spent={spent_after:.2f} | cap={CAP_NO:.3f} "
                f"| avg={avg_:.4f} | status={status} ({'WON' if won else 'LOST'}) "
                f"| pnl={pnl:.2f} | running_PL={running_pl_global:.2f} | bank={bank:.2f}"
            )

            update_status(
                bank=bank,
                idx=idx,
                total_pl=running_pl_global,
                total_markets_done=all_done,
                total_spent=spent,
                total_reports=report_idx,
                last_report_pl=pl_since_report,
                last_report_markets=markets_since_report,
                last_market_cid=cid,
                last_market_question=q,
                last_time_found=tf,
            )

            idx += 1
            time.sleep(0.2)

            # Global report if interval reached or end of list
            if markets_since_report >= GLOBAL_REPORT_INTERVAL or idx >= total_markets:
                report_idx += 1
                all_pl += pl_since_report

                print_and_log_report(
                    "CUR",
                    report_idx,
                    markets_since_report,
                    all_done,
                    pl_since_report,
                    all_pl,
                    bank,
                    spent,
                    extra_label=f"| folder={folder}",
                )

                update_status(
                    total_pl=all_pl,
                    total_reports=report_idx,
                    last_report_pl=pl_since_report,
                    last_report_markets=markets_since_report,
                )

                # reset per-report counters
                markets_since_report = 0
                pl_since_report = 0.0

        except KeyboardInterrupt:
            if not SOFT_INTERRUPTED_ONCE:
                SOFT_INTERRUPTED_ONCE = True
                print("\n[SOFT INTERRUPT] Ctrl+C detected → writing status report (press Ctrl+C again to exit).")
                write_status_report()
                continue
            raise

    print("\n=== DONE processing saved markets (CURRENT MODE) ===")
    print(f"Final bank: {bank:.2f}, total P/L: {all_pl + pl_since_report:.2f}, markets_done: {all_done}")
    print(f"Total 429 hits observed: {_num_429}")

# ======================================================
#  MAIN SIM LOGIC: HISTORICAL MODE (Gamma)
# ======================================================

def run_historical(start_epoch: int, label: str):
    global SOFT_INTERRUPTED_ONCE

    print(f"Historical mode: {label}, start_epoch={start_epoch}")
    markets = fetch_markets_since(start_epoch)
    total_markets = len(markets)
    if total_markets == 0:
        print("No markets found in the given historical window; exiting.")
        return

    bank = START_BANK
    idx = 0
    all_pl = 0.0
    all_done = 0
    spent = 0.0

    report_idx = 0
    markets_since_report = 0
    pl_since_report = 0.0

    update_status(
        bank=bank,
        idx=idx,
        total_pl=all_pl,
        total_markets_done=all_done,
        total_spent=spent,
        total_reports=report_idx,
    )

    print(f"START (HISTORICAL): bank={bank:.2f}, BET_SIZE={BET_SIZE}, CAP_NO={CAP_NO}")
    print(f"Total historical markets to process: {total_markets}")
    print(f"Reporting every {GLOBAL_REPORT_INTERVAL} markets.")

    while idx < total_markets and bank >= MIN_BANK:
        try:
            m_meta = markets[idx]
            cid = m_meta["conditionId"]
            q   = m_meta["question"]
            createdAt = m_meta["createdAt"]

            print(f"\n[HIST #{idx+1}/{total_markets}] {q} (cid={cid[:10]}…) createdAt={createdAt}")

            market_obj = {"conditionId": cid, "question": q}

            all_done += 1
            markets_since_report += 1
            last_created = createdAt

            trades_raw = fetch_trades(market_obj)

            pnl, spent_after, shares, status, won, avg_ = simulate_market_maker_no(
                cid, q, trades_raw, bank + pl_since_report, all_pl, CAP_NO, FEE_BPS, SLIP_BPS
            )

            if status not in ("YES", "NO"):
                idx += 1
                update_status(
                    bank=bank,
                    idx=idx,
                    total_pl=all_pl + pl_since_report,
                    total_markets_done=all_done,
                    total_spent=spent,
                    total_reports=report_idx,
                    last_report_pl=pl_since_report,
                    last_report_markets=markets_since_report,
                    last_market_cid=cid,
                    last_market_question=q,
                    last_time_found=createdAt,
                )
                continue

            bank += pnl
            pl_since_report += pnl
            spent += spent_after

            running_pl_global = all_pl + pl_since_report

            print(q)
            print(
                f"shares={shares:.2f} | spent={spent_after:.2f} | cap={CAP_NO:.3f} "
                f"| avg={avg_:.4f} | status={status} ({'WON' if won else 'LOST'}) "
                f"| pnl={pnl:.2f} | running_PL={running_pl_global:.2f} | bank={bank:.2f}"
            )

            update_status(
                bank=bank,
                idx=idx,
                total_pl=running_pl_global,
                total_markets_done=all_done,
                total_spent=spent,
                total_reports=report_idx,
                last_report_pl=pl_since_report,
                last_report_markets=markets_since_report,
                last_market_cid=cid,
                last_market_question=q,
                last_time_found=createdAt,
            )

            idx += 1
            time.sleep(0.2)

            if markets_since_report >= GLOBAL_REPORT_INTERVAL or idx >= total_markets:
                report_idx += 1
                all_pl += pl_since_report

                print_and_log_report(
                    "HIST",
                    report_idx,
                    markets_since_report,
                    all_done,
                    pl_since_report,
                    all_pl,
                    bank,
                    spent,
                    extra_label=f"| start_label={label}",
                )

                update_status(
                    total_pl=all_pl,
                    total_reports=report_idx,
                    last_report_pl=pl_since_report,
                    last_report_markets=markets_since_report,
                )

                markets_since_report = 0
                pl_since_report = 0.0

        except KeyboardInterrupt:
            if not SOFT_INTERRUPTED_ONCE:
                SOFT_INTERRUPTED_ONCE = True
                print("\n[SOFT INTERRUPT] Ctrl+C detected → writing status report (press Ctrl+C again to exit).")
                write_status_report()
                continue
            raise

    print("\n=== DONE processing historical markets ===")
    print(f"Final bank: {bank:.2f}, total P/L: {all_pl + pl_since_report:.2f}, markets_done: {all_done}")
    print(f"Total 429 hits observed: {_num_429}")

# ======================================================
#  MAIN / ARGPARSE
# ======================================================

def main():
    global BET_SIZE, CAP_NO, START_BANK, RPS_TARGET

    parser = argparse.ArgumentParser()
    parser.add_argument("--current",    action="store_true", help="Use saved markets from logs/<folder>")
    parser.add_argument("--historical", action="store_true", help="Use historical markets via Gamma")
    parser.add_argument("--folder",     type=str, help="Folder under logs/ for current mode")
    parser.add_argument("--daysback",   type=str, default="30",
                        help="Days back for historical mode, or 'max' for all-time")
    parser.add_argument("--bet",        type=float, default=BET_SIZE, help="Bet size per market (default from config)")
    parser.add_argument("--cap-no",     type=float, default=CAP_NO, help="NO cap price (default from config)")
    parser.add_argument("--start-bank", type=float, default=START_BANK, help="Starting bank")
    parser.add_argument("--rps",        type=float, default=RPS_TARGET,
                        help="Global HTTP requests per second target")

    args = parser.parse_args()

    # apply overrides
    BET_SIZE   = args.bet
    CAP_NO     = args.cap_no
    START_BANK = args.start_bank
    RPS_TARGET = max(0.1, args.rps)

    STATUS["bet_size"] = BET_SIZE
    STATUS["cap_no"]   = CAP_NO

    # decide mode
    if args.historical and args.current:
        print("Cannot choose both --current and --historical; pick one.")
        return
    if args.historical:
        mode = "historical"
    elif args.current:
        mode = "current"
    else:
        # default to current (folder) mode if nothing specified
        mode = "current"

    if mode == "current":
        run_from_saved_markets(args.folder)
    else:
        # daysback can be int or "max"
        db_raw = args.daysback.strip().lower()
        if db_raw == "max":
            start_epoch = POLYMARKET_INCEPTION
            label = "max"
        else:
            try:
                days_back = int(db_raw)
            except ValueError:
                print("Error: --daysback must be an integer or 'max'")
                return
            start_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
            start_epoch = int(start_dt.timestamp())
            label = f"daysback_{days_back}"

        run_historical(start_epoch, label)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[HARD INTERRUPT] Ctrl+C at top-level → writing final status and exiting.")
        write_status_report()
