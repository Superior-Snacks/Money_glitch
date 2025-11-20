import os, sys, json, random, time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ======================= Config / Constants =======================
BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"
LOGS_DIR     = "logs"

TRADES_PAGE_LIMIT = 250

HINT_SPREAD = 0.98
FINAL_GRACE = timedelta(days=2)

# Adaptive rate limit
RPS_TARGET           = 1.0   # conservative
_RPS_SCALE           = 1.0
_RPS_MIN             = 0.3
_RPS_RECOVER_PER_SEC = 0.03

# Caps for maker-style spread
CAPS = [0.001, 0.05, 0.10, 0.15, 0.20,
        0.25, 0.30, 0.35, 0.40, 0.45,
        0.50, 0.55, 0.60, 0.65, 0.70,
        0.75, 0.80, 0.85, 0.90, 0.95, 1.0]

# ======================= HTTP session / pacing ====================
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET","POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=40, pool_maxsize=40)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pm-maker-cap-spread-historical/1.0"})
    return s

SESSION = make_session()
_last_tokens_ts = time.monotonic()
_bucket = RPS_TARGET
_num_429 = 0

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
    global _RPS_SCALE, _num_429
    _num_429 += 1
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
        except requests.RequestException as e:
            print(f"      [HTTP EXC] {e} → sleeping {back:.1f}s")
            time.sleep(back + random.random()*0.4)
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
            ra = _retry_after_seconds(r)
            sleep_s = max(ra, back) + random.random()*0.3
            print(f"      [429] rate limited → sleeping {sleep_s:.1f}s (tries={tries+1})")
            time.sleep(sleep_s)
            back = min(back*1.8, 30.0)
            tries += 1
            if tries >= max_tries:
                r.raise_for_status()
            continue

        if 500 <= r.status_code < 600:
            print(f"      [5xx] status={r.status_code} → sleeping {back:.1f}s")
            time.sleep(back + random.random()*0.2)
            back = min(back*1.7, 20.0)
            tries += 1
            if tries >= max_tries:
                r.raise_for_status()
            continue

        print(f"      [HTTP {r.status_code}] giving up")
        r.raise_for_status()

# ======================= Time / FS helpers ========================
def dt_iso():
    return datetime.now(timezone.utc).isoformat()

def _to_epoch_any(x):
    try:
        f = float(x)
        return int(f/1000) if f > 1e12 else int(f)
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

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def atomic_write_text(path: str, text: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)

# ======================= Market meta / status =====================
MARKET_META_CACHE: Dict[str, dict] = {}

def _parse_dt_any(v):
    if not v:
        return None
    try:
        if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().isdigit()):
            ts = float(v)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        pass
    try:
        s = str(v).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

HINT_SPREAD = 0.98
FINAL_GRACE = timedelta(days=2)

def resolve_status(m: dict):
    uma = (m.get("umaResolutionStatus") or "").strip().lower()
    if uma in {"yes","no"}:
        return True, uma.upper(), "umaResolutionStatus"
    if uma.startswith("resolved_"):
        w = uma.split("_", 1)[1].upper()
        if w in {"YES","NO"}:
            return True, w, "umaResolutionStatus"

    w = (m.get("winningOutcome") or m.get("winner") or "").strip().upper()
    if w in {"YES","NO"}:
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
        prices = json.loads(raw) if isinstance(raw, str) else (raw or ["0", "0"])
        try:
            y, n = float(prices[0]), float(prices[1])
        except Exception:
            y = n = None

        if age_ok and y is not None and n is not None:
            if y >= HINT_SPREAD and n <= 1 - HINT_SPREAD:
                return True, "YES", "terminal_outcomePrices"
            if n >= HINT_SPREAD and y <= 1 - HINT_SPREAD:
                return True, "NO", "terminal_outcomePrices"

        if y is not None and n is not None:
            if y >= 0.90 and n <= 0.10:
                return True, "YES", "closed_price_hint_yes"
            if n >= 0.90 and y <= 0.10:
                return True, "NO", "closed_price_hint_no"

    return False, None, "unresolved"

def current_status(m: dict) -> str:
    resolved, winner, _ = resolve_status(m)
    if resolved and winner in ("YES","NO"):
        return winner
    return "TBD"

# ======================= Trades paging ============================
def trade_ts(trade: dict) -> int:
    for key in ("match_time", "timestamp", "time", "ts", "last_update"):
        v = trade.get(key)
        ts = _to_epoch_any(v) if v is not None else None
        if ts is not None:
            return ts
    return 0

def fetch_trades_page_ms(
    cid: str,
    *,
    limit: int = TRADES_PAGE_LIMIT,
    offset: int = 0,
) -> List[dict]:
    params = {
        "market": cid,
        "limit": int(limit),
        "offset": int(offset),
        "sort": "asc",
    }
    r = http_get_with_backoff(DATA_TRADES, params=params, timeout=20)
    return r.json() or []

def fetch_all_trades_since(
    cid: str,
    since_epoch_s: int,
    page_limit: int = TRADES_PAGE_LIMIT,
    hard_cap_pages: int = 5000,
) -> List[dict]:
    since_epoch_s = int(since_epoch_s)
    offset = 0
    seen_ids: set = set()
    uniq: List[dict] = []

    start_time = time.monotonic()
    print(f"    [FETCH] cid={cid[:12]}… baseline={since_epoch_s} (offset-based)")

    seen_since_region = False

    for page_no in range(1, hard_cap_pages + 1):
        elapsed = time.monotonic() - start_time
        bar_len = min(20, page_no)
        bar = "[" + "#" * bar_len + "-" * (20 - bar_len) + "]"
        print(
            f"      [PAGE {page_no:4d}/{hard_cap_pages}] {bar} "
            f"offset={offset} elapsed={elapsed:5.1f}s"
        )

        page = fetch_trades_page_ms(cid, limit=page_limit, offset=offset)
        if not page:
            print("        → empty page, stopping")
            break

        page_added = 0

        for t in page:
            tid = t.get("id")
            if not tid:
                tid = (
                    f"{t.get('taker_order_id')}-"
                    f"{t.get('market')}-"
                    f"{t.get('price')}-"
                    f"{t.get('size')}-"
                    f"{t.get('side')}-"
                    f"{t.get('outcome')}-"
                    f"{t.get('bucket_index')}"
                )

            if tid in seen_ids:
                continue

            ts = trade_ts(t)
            if ts < since_epoch_s:
                continue

            seen_since_region = True

            seen_ids.add(tid)
            uniq.append(t)
            page_added += 1

        print(f"        → got {len(page)} trades")
        print(f"        → added {page_added}, total_unique={len(uniq)}")

        if seen_since_region and page_added == 0:
            print("        [STOP] no new trades beyond baseline; stopping")
            break

        if len(page) < page_limit:
            print("        [STOP] short page (likely end of history)")
            break

        offset += page_limit

    uniq.sort(key=trade_ts)
    total_elapsed = time.monotonic() - start_time
    print(f"    [DONE] trades_fetched={len(uniq)} in {total_elapsed:.1f}s since={since_epoch_s}")
    return uniq

# ======================= Maker-style cap spread ===================
def collect_cap_spread(
    trades: List[dict],
    caps: List[float],
    since_epoch: int = 0,
    prev_spread: Optional[dict] = None,
) -> dict:
    caps = sorted({round(c, 3) for c in caps})

    if prev_spread is None:
        spread = {
            "last_trade_ts": since_epoch,
            "caps": {
                c: {"shares": 0.0, "dollars": 0.0, "trades": 0}
                for c in caps
            },
        }
    else:
        spread = {
            "last_trade_ts": int(prev_spread.get("last_trade_ts", since_epoch)),
            "caps": {}
        }
        prev_caps = prev_spread.get("caps", {})
        for c in caps:
            state = prev_caps.get(str(c)) or prev_caps.get(c) or {}
            spread["caps"][c] = {
                "shares": float(state.get("shares", 0.0)),
                "dollars": float(state.get("dollars", 0.0)),
                "trades": int(state.get("trades", 0)),
            }

    trades_sorted = sorted(trades, key=trade_ts)
    last_ts = spread["last_trade_ts"] or since_epoch

    for t in trades_sorted:
        ts = trade_ts(t)
        if ts < since_epoch:
            continue
        if ts <= last_ts:
            continue

        outcome = (t.get("outcome") or "").strip().lower()
        if outcome != "no":
            continue

        try:
            p = float(t.get("price") or 0.0)
            s = float(t.get("size")  or 0.0)
        except Exception:
            continue
        if p <= 0 or s <= 0:
            continue

        notional = p * s

        for c in caps:
            if c + 1e-12 >= p:  # cap >= trade price
                st = spread["caps"][c]
                st["shares"]  += s
                st["dollars"] += notional
                st["trades"]  += 1

        if ts > spread["last_trade_ts"]:
            spread["last_trade_ts"] = ts

    json_caps = {}
    for c, st in spread["caps"].items():
        json_caps[f"{c:.3f}"] = {
            "shares":  round(st["shares"], 6),
            "dollars": round(st["dollars"], 2),
            "trades":  int(st["trades"]),
        }

    return {
        "last_trade_ts": spread["last_trade_ts"],
        "caps": json_caps,
    }

# ======================= Fetch markets from Gamma =================
def fetch_markets_since(start_epoch: int, excluded: List[str]) -> Dict[str, dict]:
    """
    Walks ALL Gamma markets via offset until empty page.
    Keeps markets whose createdAt (or similar) >= start_epoch.
    """
    markets: Dict[str, dict] = {}
    offset = 0
    page_size = 500
    total_kept = 0
    sample_shown = False

    print(f"{dt_iso()} Fetching markets from Gamma since {datetime.fromtimestamp(start_epoch, tz=timezone.utc).isoformat()} (epoch={start_epoch})")

    while True:
        params = {"limit": page_size, "offset": offset}
        r = http_get_with_backoff(BASE_GAMMA, params=params, timeout=30)
        rows = r.json() or []
        got = len(rows)
        if got == 0:
            print(f"  [GAMMA] offset={offset} got=0 → done.")
            break

        if not sample_shown and rows:
            sample_shown = True
            print("Sample market keys:", list(rows[0].keys()))
            print("Sample market:", json.dumps(rows[0], indent=2)[:400])

        kept_new = 0
        for m in rows:
            cid = m.get("conditionId")
            if not cid:
                continue
            q = (m.get("question") or "").strip()

            if excluded and any(tok in q.lower() for tok in excluded):
                continue

            created_raw = (
                m.get("createdAt")
                or m.get("created_at")
                or m.get("endDate")
                or m.get("endDateIso")
                or m.get("closedTime")
                or m.get("updatedAt")
            )
            created_ts = _to_epoch_any(created_raw)
            if created_ts is None:
                continue

            if created_ts < start_epoch:
                continue

            prev = markets.get(cid)
            if prev is None or created_ts < prev["created_epoch"]:
                markets[cid] = {
                    "conditionId": cid,
                    "question": q,
                    "createdAt": created_raw,
                    "created_epoch": created_ts,
                }
                kept_new += 1

        total_kept += kept_new
        print(f"  [GAMMA] offset={offset} got={got}, kept_new={kept_new}, total_kept={total_kept}")
        offset += got

    return markets

# ======================= Overview / heartbeat =====================
def print_overview(open_rows, closed_rows, errors):
    print("\n===== HISTORICAL CAP-SPREAD OVERVIEW =====")
    print(f"Open markets saved:   {len(open_rows)}")
    print(f"Closed markets saved: {len(closed_rows)}")
    print(f"Errors (fetch/etc):   {errors}")

# ======================= Main ====================================
def main():
    global _num_429

    days_back_str = input("Days back (e.g. 30): ").strip()
    try:
        days_back = int(days_back_str)
    except ValueError:
        print("Invalid days_back; must be integer.")
        return

    excluded_str = input("Exclude categories/keywords (comma-separated, blank=none): ").strip()
    excluded = [x.strip().lower() for x in excluded_str.split(",") if x.strip()]

    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=days_back)
    start_epoch = int(start_dt.timestamp())

    folder = f"daysback_{days_back}"
    out_dir = os.path.join(LOGS_DIR, folder)
    ensure_dir(out_dir)
    open_path   = os.path.join(out_dir, "log_open_markets_cap_spread.jsonl")
    closed_path = os.path.join(out_dir, "log_closed_markets_cap_spread.jsonl")

    print(f"{dt_iso()} Starting historic cap-spread build… caps={CAPS}")
    print(f"Output folder: {out_dir}")

    _num_429 = 0
    global_trades_fetched = 0
    errors = 0

    # 1) fetch all markets created in the last N days
    markets = fetch_markets_since(start_epoch, excluded)
    total_markets = len(markets)
    print(f"Total markets created in last {days_back} days: {total_markets}")

    open_rows: List[dict] = []
    closed_rows: List[dict] = []

    for i, (cid, meta) in enumerate(markets.items(), 1):
        q = meta["question"]
        created_epoch = meta["created_epoch"]

        print(f"\n[{i}/{total_markets}] {q}  (cid={cid[:10]}…)  createdAt={meta['createdAt']}")

        # Fetch meta & status (again via Gamma, but cheap compared to trades)
        try:
            if cid in MARKET_META_CACHE and MARKET_META_CACHE[cid]:
                m = MARKET_META_CACHE[cid]
            else:
                r = http_get_with_backoff(BASE_GAMMA, params={"condition_ids": cid, "limit": 1}, timeout=15)
                rows = r.json() or []
                m = {}
                for row in rows:
                    if row.get("conditionId") == cid:
                        m = row
                        break
                MARKET_META_CACHE[cid] = m
            status = current_status(m)
        except Exception as e:
            errors += 1
            print(f"  [WARN meta] {e}")
            status = "TBD"

        # Assume we discover the market 5 minutes after creation
        baseline = created_epoch + 5 * 60

        try:
            trades = fetch_all_trades_since(cid, baseline, page_limit=TRADES_PAGE_LIMIT)
        except Exception as e:
            errors += 1
            print(f"  [WARN trades] {e}")
            continue

        global_trades_fetched += len(trades)

        cap_spread = collect_cap_spread(
            trades,
            caps=CAPS,
            since_epoch=baseline,
            prev_spread=None,   # no incremental; this is a one-shot historic build
        )

        parts = []
        for c in CAPS:
            st = cap_spread["caps"].get(f"{c:.3f}", {})
            dollars = int(st.get("dollars", 0))
            shares  = int(st.get("shares", 0))
            trades_n = st.get("trades", 0)
            parts.append(f"{c:0.2f}: ${dollars}/{shares} ({trades_n}t)")
        print("    [CAP SPREAD] " + " | ".join(parts))

        row = {
            "ts": dt_iso(),
            "status": status,
            "conditionId": cid,
            "question": q,
            "createdAt": meta["createdAt"],
            "created_epoch": created_epoch,
            "cap_spread": cap_spread,
        }

        if status in ("YES","NO"):
            closed_rows.append(row)
        else:
            open_rows.append(row)

    open_text = "\n".join(json.dumps(s, ensure_ascii=False) for s in open_rows)
    atomic_write_text(open_path, open_text + ("\n" if open_rows else ""))
    print(f"\n[WRITE] {len(open_rows)} open rows → {open_path} (rewritten)")

    closed_text = "\n".join(json.dumps(s, ensure_ascii=False) for s in closed_rows)
    atomic_write_text(closed_path, closed_text + ("\n" if closed_rows else ""))
    print(f"[WRITE] {len(closed_rows)} closed rows → {closed_path} (rewritten)")

    print_overview(open_rows, closed_rows, errors)
    print(f"Total trades fetched: {global_trades_fetched} | 429_hits={_num_429}")

if __name__ == "__main__":
    main()