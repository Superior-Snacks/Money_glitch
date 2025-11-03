import os, sys, json, time, glob, math, random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========================== CONFIG / INPUT ==========================

GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"
TRADES_API    = "https://data-api.polymarket.com/trades"

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def parse_iso_to_epoch(s: str) -> Optional[int]:
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None

def ask_float(prompt: str, default: Optional[float] = None) -> float:
    while True:
        raw = input(f"{prompt}{' ['+str(default)+']' if default is not None else ''}: ").strip()
        if not raw and default is not None:
            return float(default)
        try:
            return float(raw)
        except:
            print("Please enter a number.")

def ask_str(prompt: str) -> str:
    s = input(f"{prompt}: ").strip()
    return s

# ========================== LOG PATHS / IO ==========================

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def open_jsonl_write(path: str, rec: dict):
    ensure_dir(os.path.dirname(path))
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

def write_text(path: str, text: str):
    ensure_dir(os.path.dirname(path))
    with open(path, "a", encoding="utf-8") as f:
        f.write(text)

def write_json(path: str, obj: Any):
    ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, path)

# ========================== SESSION / RPS ===========================

# Adaptive token bucket + proper backoff honoring Retry-After
RPS_TARGET = 3.5         # desired peak
_RPS_SCALE = 1.0         # auto-tuned [0.3 .. 1.0]
_RPS_MIN   = 0.3
_RPS_RECOVER_PER_SEC = 0.03

_last_tokens_ts = time.monotonic()
_bucket = RPS_TARGET

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pm-lifecycle-watch/1.0"})
    return s

SESSION = make_session()

def _rate_limit():
    global _last_tokens_ts, _bucket
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
    except:
        return 0.0

def http_get(url: str, *, params=None, timeout=25, max_tries=8, stats=None) -> requests.Response:
    back = 0.5
    tries = 0
    last_t = time.monotonic()
    while True:
        _rate_limit()
        try:
            r = SESSION.get(url, params=params or {}, timeout=timeout)
        except requests.RequestException:
            time.sleep(back + random.random() * 0.2)
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
            if stats: stats.http_429s += 1
            _rps_on_429()
            ra = _retry_after_seconds(r)
            sleep_s = max(ra, back) + random.random() * 0.3
            time.sleep(sleep_s)
            back = min(back * 1.8, 30.0)
            tries += 1
            if tries >= max_tries:
                r.raise_for_status()
            continue

        if 500 <= r.status_code < 600:
            time.sleep(back + random.random() * 0.2)
            back = min(back * 1.7, 20.0)
            tries += 1
            if tries >= max_tries:
                r.raise_for_status()
            continue

        r.raise_for_status()

# ========================== HELPERS / API ===========================

def market_is_closed(m: dict) -> bool:
    if m.get("closed") is True:
        return True
    # winner keys
    for k in ("winningOutcome","resolvedOutcome","winner","resolveOutcome","resolution","outcome"):
        v = m.get(k)
        if isinstance(v, str) and v.strip().lower() in ("yes", "no"):
            return True
    return False

def market_winner(m: dict) -> Optional[str]:
    for k in ("winningOutcome","resolvedOutcome","winner","resolveOutcome","resolution","outcome"):
        v = m.get(k)
        if isinstance(v, str) and v.strip().lower() in ("yes", "no"):
            return v.strip().upper()
    res = m.get("result") or m.get("resolutionData") or {}
    if isinstance(res, dict):
        for k in ("winner","winningOutcome","outcome"):
            v = res.get(k)
            if isinstance(v, str) and v.strip().lower() in ("yes","no"):
                return v.strip().upper()
    return None

def market_resolved_ts(m: dict) -> Optional[int]:
    for k in ("resolvedTime","resolveTime","resolutionTime","closedTime","endDate","closeTime"):
        s = m.get(k)
        if not s: continue
        ts = parse_iso_to_epoch(str(s))
        if ts: return ts
    return None

def fetch_market_by_id_or_cid(id_or_cid: str, stats=None) -> Optional[dict]:
    """
    Try to fetch a single market row by conditionId or id by scanning pages (newest→oldest).
    Efficient enough for hourly checks; avoids complex query params.
    """
    limit = 250
    offset = 0
    seen = 0
    while True:
        params = {"limit": limit, "order": "startDate", "ascending": False, "enableOrderBook": False, "offset": offset}
        r = http_get(GAMMA_MARKETS, params=params, stats=stats)
        rows = r.json() or []
        if not rows: break
        for m in rows:
            cid = m.get("conditionId") or m.get("id")
            mid = m.get("id")
            if id_or_cid == cid or id_or_cid == mid:
                return m
        seen += len(rows)
        if len(rows) < limit: break
        offset += limit
        # small pause is handled via rate limiter
    return None

def fetch_trades_after(cid: str, since_epoch: int, cap: float, stats=None) -> Tuple[List[dict], int, int]:
    """
    Pull trades for a market strictly AFTER since_epoch.
    Count trades with price <= cap (under) and > cap (over).
    Prints each trade line while scanning.
    """
    out = []
    under, over = 0, 0
    limit = 250
    # We'll page backwards by starting_before (ms). Start at "now".
    starting_before = int(time.time())
    printed_header = False

    while True:
        params = {
            "market": cid,
            "sort": "desc",
            "limit": limit,
            "starting_before": int(starting_before * 1000)  # ms
        }
        r = http_get(TRADES_API, params=params, stats=stats)
        data = r.json() or []
        if not data:
            break
        out.extend(data)

        # print per-trade (after since)
        oldest = None
        for t in data:
            ts = t.get("timestamp") or t.get("time") or t.get("ts")
            try:
                tsf = float(ts)
                ts_s = int(tsf/1000) if tsf > 1e12 else int(tsf)
            except:
                ts_s = None
            price = float(t.get("price", 0) or 0)
            size  = float(t.get("size", 0) or 0)
            outcome = str(t.get("outcome","")).upper()
            side = str(t.get("side","")).upper()

            if ts_s is not None:
                oldest = ts_s if oldest is None else min(oldest, ts_s)

            if ts_s is None or ts_s <= since_epoch:
                continue

            if not printed_header:
                print(f"--- Trades AFTER {datetime.fromtimestamp(since_epoch, tz=timezone.utc).isoformat()} for {cid} ---")
                printed_header = True

            flag = "UNDER" if price <= cap + 1e-12 else "OVER"
            if price <= cap + 1e-12:
                under += 1
            else:
                over += 1

            print(f"[TRADE] {datetime.fromtimestamp(ts_s, tz=timezone.utc).isoformat()} "
                  f"price={price:.4f} size={size:.4f} outcome={outcome} side={side} → {flag}")

        if oldest is None or oldest <= since_epoch:
            break
        starting_before = oldest

        # light pacing is controlled globally

    return out, under, over

# ====================== ECON / P&L CALC =============================

def pnl_for_no_bet_at_price(bet_size: float, price: float, winner: str) -> float:
    """
    Assume we spent `bet_size` USD buying NO at `price` (per-share price).
    Shares = bet_size / price. If NO wins, value = shares*1 → profit = shares - bet_size.
    If YES wins, profit = -bet_size.
    """
    if price <= 0:
        return 0.0
    shares = bet_size / price
    if winner == "NO":
        return shares - bet_size
    else:
        return -bet_size

# ====================== RUN STATS & SUMMARY =========================

@dataclass
class RunStats:
    scanned: int = 0
    newly_closed: int = 0
    unresolved_seen: int = 0
    http_429s: int = 0

    def est_locked_dollars(self, bet_size):
        try:
            return float(bet_size) * float(self.unresolved_seen)
        except:
            return None

def write_run_summary(summary_path: str, when_iso: str, stats: RunStats, cap: float, bet_size: float, took_seconds: float):
    h = int(took_seconds // 3600)
    m = int((took_seconds % 3600) // 60)
    s = int(took_seconds % 60)
    pretty = f"{h:02d}:{m:02d}:{s:02d}"
    locked = stats.est_locked_dollars(bet_size)
    locked_str = f"${locked:,.2f}" if locked is not None else "n/a"
    line = (f"{when_iso} | scanned={stats.scanned} | newly_closed={stats.newly_closed} | "
            f"unresolved_seen={stats.unresolved_seen} | est_locked={locked_str} | "
            f"cap={cap:.2f} | bet_size=${bet_size:.2f} | r429={stats.http_429s} | took={pretty}\n")
    write_text(summary_path, line)
    # JSONL twin
    js_path = os.path.splitext(summary_path)[0] + ".jsonl"
    open_jsonl_write(js_path, {
        "time": when_iso,
        "scanned": stats.scanned,
        "newly_closed": stats.newly_closed,
        "unresolved_seen": stats.unresolved_seen,
        "est_locked": stats.est_locked_dollars(bet_size),
        "cap": cap,
        "bet_size": bet_size,
        "http_429s": stats.http_429s,
        "took_seconds": round(took_seconds, 3),
    })

# ====================== MAIN HOURLY WORKER ==========================

def read_found_records(found_dir: str) -> List[dict]:
    """Load all lines from logs/<custom>/marketfiles/*.jsonl"""
    paths = sorted(glob.glob(os.path.join(found_dir, "*.jsonl")))
    rows = []
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    rows.append(json.loads(line))
                except:
                    continue
    return rows

def resolve_market_identity(rec: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (conditionId, human_name) from the found record or later via Gamma fetch.
    """
    cid = rec.get("conditionId") or rec.get("id") or rec.get("cid")
    name = rec.get("question") or rec.get("name") or rec.get("title")
    return cid, name

def main():
    print("== Hourly closed-market watcher ==")
    custom = ask_str("Custom folder name (under logs/)")
    cap = ask_float("Cap price (e.g. 0.70)", 0.70)
    bet_size = ask_float("Bet size in $ (per market)", 100.0)

    ROOT_DIR = os.path.join("logs", custom)
    FOUND_DIR = os.path.join(ROOT_DIR, "marketfiles")
    ensure_dir(ROOT_DIR)

    CLOSED_PATH = os.path.join(ROOT_DIR, "closed_markets.jsonl")
    SNAPSHOT_PATH = os.path.join(ROOT_DIR, "snapshot_latest.json")
    SUMMARY_PATH  = os.path.join(ROOT_DIR, "run_summary.log")

    print(f"Reading found logs from: {FOUND_DIR}")
    print(f"Outputs: {CLOSED_PATH}, {SNAPSHOT_PATH}, {SUMMARY_PATH}")
    print("Running hourly. Ctrl+C to stop.\n")

    try:
        while True:
            run_started = time.monotonic()
            stats = RunStats()

            # Aggregate snapshot counters
            total_open = 0
            total_closed = 0
            total_under_trades = 0
            total_over_trades = 0

            # Load found market records
            found = read_found_records(FOUND_DIR)
            print(f"{iso_now()} loaded found rows: {len(found)}")

            # De-dup by ID with most recent time_found kept
            latest_by_id: Dict[str, dict] = {}
            for r in found:
                cid, _ = resolve_market_identity(r)
                if not cid:
                    # skip malformed rows
                    continue
                tf = r.get("time_found")
                tf_epoch = parse_iso_to_epoch(str(tf)) if tf else None
                prev = latest_by_id.get(cid)
                if prev is None:
                    latest_by_id[cid] = r
                else:
                    prev_tf = parse_iso_to_epoch(str(prev.get("time_found"))) if prev.get("time_found") else None
                    if (tf_epoch or 0) > (prev_tf or 0):
                        latest_by_id[cid] = r

            ids = list(latest_by_id.keys())

            # Track which cids we've already written to CLOSED_PATH (avoid duplicate rows)
            already_closed: set = set()
            if os.path.exists(CLOSED_PATH):
                with open(CLOSED_PATH, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            j = json.loads(line)
                            if j.get("market_id"):
                                already_closed.add(j["market_id"])
                        except:
                            continue

            newly_closed_rows = 0

            for cid in ids:
                stats.scanned += 1
                rec = latest_by_id[cid]
                time_found_iso = rec.get("time_found")
                tf_epoch = parse_iso_to_epoch(str(time_found_iso)) if time_found_iso else None
                _, name_hint = resolve_market_identity(rec)

                # Pull market meta
                m = fetch_market_by_id_or_cid(cid, stats=stats)
                if not m:
                    print(f"[MISS] market not found for id={cid}")
                    continue

                name = name_hint or m.get("question") or m.get("slug") or m.get("market_slug") or "(unknown)"
                resolved = market_is_closed(m)
                winner = market_winner(m)
                res_ts = market_resolved_ts(m)

                if not resolved:
                    stats.unresolved_seen += 1
                    total_open += 1
                else:
                    total_closed += 1

                # If we have time_found, scan trades after time_found and count under/over vs cap
                if tf_epoch is not None:
                    trades, under, over = fetch_trades_after(m.get("conditionId") or cid, tf_epoch, cap, stats=stats)
                    total_under_trades += under
                    total_over_trades  += over

                # If newly closed and not logged yet → compute P/L and write
                if resolved and (m.get("conditionId") or cid) not in already_closed:
                    # Assume filled at cap
                    pl = pnl_for_no_bet_at_price(bet_size, cap, winner or "YES")
                    wl = 1 if (winner == "NO") else 0

                    row = {
                        "time_logged": iso_now(),
                        "market_id": m.get("conditionId") or cid,
                        "question": name,
                        "winner": winner,                   # "YES"/"NO"/None
                        "resolved_at": res_ts,              # epoch
                        "w_l": wl,                          # 1=win, 0=loss
                        "p_l": round(pl, 2),                # assuming entry at cap
                        "cap": cap,
                        "bet_size": bet_size,
                        "open_markets_seen": total_open,
                        "closed_markets_seen": total_closed,
                        "trades_under_cap_since_found": under if tf_epoch is not None else None,
                        "trades_over_cap_since_found": over if tf_epoch is not None else None,
                    }
                    open_jsonl_write(CLOSED_PATH, row)
                    print(f"[CLOSED] wrote → {name} | winner={winner} | P/L=${row['p_l']:.2f}")
                    newly_closed_rows += 1
                    stats.newly_closed += 1

            # Dollars locked = bet_size * unresolved_seen (your definition)
            snapshot = {
                "time": iso_now(),
                "cap": cap,
                "bet_size": bet_size,
                "open_markets_seen": total_open,
                "closed_markets_seen": total_closed,
                "trades_under_cap_total": total_under_trades,
                "trades_over_cap_total": total_over_trades,
                "unresolved_count": stats.unresolved_seen,
                "dollars_locked_est": stats.est_locked_dollars(bet_size),
                "newly_closed_rows": newly_closed_rows,
            }
            write_json(SNAPSHOT_PATH, snapshot)

            took = time.monotonic() - run_started
            write_run_summary(SUMMARY_PATH, iso_now(), stats, cap, bet_size, took)

            # Sleep to next hour
            # Try to align roughly to 1h cadence while accounting for runtime
            sleep_for = max(0.0, 3600.0 - took)
            print(f"\n{iso_now()} pass done: scanned={stats.scanned}, newly_closed={stats.newly_closed}, "
                  f"unresolved={stats.unresolved_seen}, under_trades_total={total_under_trades}, "
                  f"over_trades_total={total_over_trades}. Sleeping {int(sleep_for)}s.\n")
            time.sleep(sleep_for)

    except KeyboardInterrupt:
        print("\nbye.")

if __name__ == "__main__":
    main()