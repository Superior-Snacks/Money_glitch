import os, sys, json, random, glob, time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------- Constants -----------------
BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"
LOGS_DIR     = "logs"

TRADES_PAGE_LIMIT = 250
LOOP_DEFAULT_SEC  = 3600  # 0 = single pass

HINT_SPREAD = 0.98                 # how close to 0/1 we require for decisive winner
FINAL_GRACE = timedelta(days=2)    # wait this long after close before trusting price-only finals

# Rate limiting with adaptive scale
RPS_TARGET            = 1.0   # average allowed requests per second for THIS script
_RPS_SCALE            = 1.0
_RPS_MIN              = 0.2   # if we get hammered with 429s, we can drop to 0.2 rps
_RPS_RECOVER_PER_SEC  = 0.01  # slow recovery so we don't bounce back too fast

# ----------------- Session + pacing -----------------
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
    s.headers.update({"User-Agent": "pm-no-bet-scan/cap-spread-cache-1.0"})
    return s

SESSION = make_session()
_last_tokens_ts = time.monotonic()
_bucket = RPS_TARGET

_HTTP_CALLS = 0
_HTTP_429S  = 0
_LAST_HEARTBEAT = time.monotonic()

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
    """
    Very conservative HTTP GET with:
      - token-bucket rate limiting (RPS_TARGET)
      - exponential backoff on errors
      - special handling + logging for 429s
      - heartbeat logging every ~60s
    """
    global _HTTP_CALLS, _HTTP_429S, _LAST_HEARTBEAT

    back = 0.5
    tries = 0
    last_t = time.monotonic()

    while True:
        _HTTP_CALLS += 1

        # Heartbeat every ~60 seconds
        now_hb = time.monotonic()
        if now_hb - _LAST_HEARTBEAT > 60:
            print(
                f"[HEARTBEAT] http_calls={_HTTP_CALLS} 429s={_HTTP_429S} "
                f"rps_target={RPS_TARGET:.2f} rps_scale={_RPS_SCALE:.3f}"
            )
            _LAST_HEARTBEAT = now_hb

        # Global rate limiter
        _rate_limit()

        try:
            r = SESSION.get(url, params=params or {}, timeout=timeout)
        except requests.RequestException as e:
            tries += 1
            print(f"[HTTP ERROR] {type(e).__name__} on {url} try={tries}/{max_tries} back={back:.2f}s")
            time.sleep(back + random.random()*0.4)
            back = min(back * 1.7, 20.0)
            if tries >= max_tries:
                raise
            continue

        status = r.status_code

        # SUCCESS
        if status < 400:
            now = time.monotonic()
            _rps_recover(now - last_t)
            return r

        # 429: Too Many Requests
        if status == 429:
            _HTTP_429S += 1
            _rps_on_429()
            ra = _retry_after_seconds(r)
            sleep_s = max(ra, back) + random.random()*0.3
            print(
                f"[HTTP 429] url={url} try={tries+1}/{max_tries} "
                f"Retry-After={ra:.2f}s sleep={sleep_s:.2f}s "
                f"rps_scale→{_RPS_SCALE:.3f}"
            )
            time.sleep(sleep_s)
            back = min(back * 1.8, 30.0)
            tries += 1
            if tries >= max_tries:
                print("[HTTP 429] max_tries reached, raising.")
                r.raise_for_status()
            continue

        # 5xx: server errors (we backoff, but log)
        if 500 <= status < 600:
            tries += 1
            print(
                f"[HTTP {status}] server error on {url} try={tries}/{max_tries} "
                f"back={back:.2f}s"
            )
            time.sleep(back + random.random()*0.2)
            back = min(back * 1.7, 20.0)
            if tries >= max_tries:
                print(f"[HTTP {status}] max_tries reached, raising.")
                r.raise_for_status()
            continue

        # Other 4xx: log once and bail
        print(f"[HTTP {status}] fatal for {url}, params={params}")
        r.raise_for_status()

# ----------------- Time utils -----------------
def dt_iso():
    return datetime.now(timezone.utc).isoformat()

def _parse_iso_to_epoch(s: Optional[str]) -> Optional[int]:
    if not s: return None
    try:
        return int(datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc).timestamp())
    except Exception:
        return None

def _to_epoch_any(x):
    try:
        f = float(x)
        # if ms
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

def _parse_dt_any(v):
    """Best-effort parse of ISO or epoch-like to aware UTC datetime."""
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

# ----------------- FS helpers -----------------
def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def atomic_write_text(path: str, text: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)

def open_logs(path: str) -> List[dict]:
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                rows.append(rec)
            except Exception:
                continue
    return rows

def load_log_index(path: str) -> Dict[str, dict]:
    """
    Return {conditionId: last_row} for a given JSONL log file.
    If multiple rows exist, the last one in the file wins.
    """
    idx: Dict[str, dict] = {}
    for rec in open_logs(path):
        cid = rec.get("conditionId")
        if cid:
            idx[cid] = rec
    return idx

# ----------------- Read rolling market logs -----------------
def read_unique_markets(folder_name: str) -> Dict[str, dict]:
    """
    Reads all logs/<folder_name>/markets_*.jsonl files,
    returns {conditionId: {"question":..., "time_found":..., "conditionId":...}}
    keeping the EARLIEST time_found per cid.
    """
    out_dir = os.path.join(LOGS_DIR, folder_name)
    paths = sorted(glob.glob(os.path.join(out_dir, "markets_*.jsonl")))
    uniq = {}
    bad = 0
    total = 0
    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    total += 1
                    try:
                        rec = json.loads(line)
                    except Exception:
                        bad += 1
                        continue
                    cid = rec.get("conditionId")
                    q   = rec.get("question")
                    tf  = rec.get("time_found")
                    if not cid or not q or not tf:
                        continue
                    old_tf = _parse_iso_to_epoch(uniq.get(cid, {}).get("time_found"))
                    new_tf = _parse_iso_to_epoch(tf)
                    if cid not in uniq or (new_tf or 10**18) < (old_tf or 10**18):
                        uniq[cid] = {"conditionId": cid, "question": q, "time_found": tf}
        except FileNotFoundError:
            continue
    print(f"[READ] files={len(paths)} rows≈{total} unique_by_cid={len(uniq)} bad_lines={bad}")
    return uniq

# ----------------- Market meta / status -----------------
MARKET_META_CACHE: Dict[str, dict] = {}

def fetch_market_full_by_cid(cid: str) -> dict:
    """Fetch a single market row by conditionId, with correct param name and safety."""
    if not cid:
        return {}
    if cid in MARKET_META_CACHE and MARKET_META_CACHE[cid]:
        return MARKET_META_CACHE[cid]
    try:
        r = http_get_with_backoff(BASE_GAMMA, params={"condition_ids": cid, "limit": 1}, timeout=15)
        rows = r.json() or []
        for m in rows:
            if m.get("conditionId") == cid:
                MARKET_META_CACHE[cid] = m
                return m
        MARKET_META_CACHE[cid] = {}
        return {}
    except Exception as e:
        print(f"[ERROR fetch_market_full_by_cid] {cid}: {e}")
        MARKET_META_CACHE[cid] = {}
        return {}

def resolve_status(m: dict):
    """
    Returns (is_resolved: bool, winner: Optional['YES'|'NO'], source_tag: str)
    """
    q = m.get("question", "(unknown question)")

    uma = (m.get("umaResolutionStatus") or "").strip().lower()
    if uma in {"yes","no"}:
        return True, uma.upper(), "umaResolutionStatus"
    if uma.startswith("resolved_"):
        w = uma.split("_",1)[1].upper()
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

# ----------------- Trades pulling -----------------
def _epoch_from_trade(t) -> Optional[int]:
    """
    Prefer Polymarket 'match_time' (unix seconds string), then 'last_update',
    then any legacy 'timestamp'/'time'/'ts'. Accepts seconds or ms.
    """
    cand = (
        t.get("match_time") or
        t.get("last_update") or
        t.get("timestamp") or
        t.get("time") or
        t.get("ts")
    )
    if cand is None:
        return None
    try:
        x = float(cand)
        if x > 1e12:  # ms -> s
            x /= 1000.0
        return int(x)
    except Exception:
        # last resort: try ISO 8601
        try:
            return int(datetime.fromisoformat(str(cand).replace("Z","+00:00"))
                       .astimezone(timezone.utc).timestamp())
        except Exception:
            return None

def trade_ts(trade: dict) -> int:
    """
    Get best-effort epoch seconds from a Polymarket trade object.
    Prefers match_time (spec), then falls back to other fields.
    """
    for key in ("match_time", "timestamp", "time", "ts"):
        v = trade.get(key)
        ts = _to_epoch_any(v) if v is not None else None
        if ts is not None:
            return ts
    return 0

def fetch_trades_page_ms(
    cid: str,
    *,
    limit: int = TRADES_PAGE_LIMIT,
    after: Optional[int] = None,
    before: Optional[int] = None
) -> List[dict]:
    params = {"market": cid, "limit": int(limit)}
    if after is not None:
        params["after"] = int(after)
    if before is not None:
        params["before"] = int(before)
    r = http_get_with_backoff(DATA_TRADES, params=params, timeout=20)
    return r.json() or []

def fetch_all_trades_since(
    cid: str,
    since_epoch_s: int,
    page_limit: int = TRADES_PAGE_LIMIT,
    hard_cap_pages: int = 5000
) -> List[dict]:
    """
    Forward paginates Polymarket trades with verbose logging so you always
    know whether it's progressing, slow, rate-limited, or stuck.
    """

    print(f"    [FETCH] cid={cid[:12]}… baseline={since_epoch_s}")
    cursor = int(since_epoch_s)
    seen_ids = set()
    uniq = []

    last_total = 0
    stagnant_pages = 0

    for page_i in range(1, hard_cap_pages+1):

        print(f"      [PAGE {page_i}] after={cursor} limit={page_limit}")
        page = fetch_trades_page_ms(cid, limit=page_limit, after=cursor)

        if page is None:
            print("      [WARN] page=None (network or API issue)")
            break

        if not page:
            print("      [END] empty page (no more trades)")
            break

        print(f"         → got {len(page)} trades")

        added_this_page = 0
        max_ts = None

        for t in page:
            tid = t.get("id") or (
                f"{t.get('taker_order_id')}-{t.get('price')}-{t.get('size')}"
            )
            if tid in seen_ids:
                continue

            ts = _epoch_from_trade(t)
            if ts is None:
                continue
            if ts < since_epoch_s:
                continue

            uniq.append(t)
            seen_ids.add(tid)
            added_this_page += 1
            if max_ts is None or ts > max_ts:
                max_ts = ts

        print(f"         → added {added_this_page}, total={len(uniq)}")

        # Advance cursor
        if max_ts is None:
            cursor += 1
        else:
            cursor = max_ts + 1

        # Stagnation detection
        if len(uniq) == last_total:
            stagnant_pages += 1
            print(f"         → stagnant page ({stagnant_pages}/3)")
            if stagnant_pages >= 3:
                print("      [STOP] stagnation threshold reached")
                break
        else:
            stagnant_pages = 0
            last_total = len(uniq)

        # Soft stop
        if len(page) < page_limit:
            print("      [STOP] short page (last page)")
            break

    print(f"    [DONE] total trades fetched={len(uniq)}")
    uniq.sort(key=trade_ts)
    return uniq

# ------------------ collect shares at price (maker-style) -----------------
def collect_cap_spread(
    trades: List[dict],
    caps: List[float],
    since_epoch: int = 0,
    prev_spread: Optional[dict] = None,
) -> dict:
    """
    Build (or update) a maker-style cap spread for a single market.

    Logic:
      - Only considers NO-side trades
      - Only trades with ts >= since_epoch
      - For each trade (price p, size s), it adds liquidity to all caps c where c >= p
        because a maker posting NO at any cap <= c would have been hit.
    """
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
            "last_trade_ts": prev_spread.get("last_trade_ts", since_epoch),
            "caps": {}
        }
        prev_caps = prev_spread.get("caps", {})
        for c in caps:
            state = prev_caps.get(c, {})
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
            if c + 1e-12 >= p:
                st = spread["caps"][c]
                st["shares"]  += s
                st["dollars"] += notional
                st["trades"]  += 1

        if ts > spread["last_trade_ts"]:
            spread["last_trade_ts"] = ts

    return spread

# ----------------- Main -----------------
def main():
    folder = input("Folder name under logs/: ").strip()
    try:
        loop_s = int(input("Loop seconds (0=single pass) [3600]: ").strip() or "3600")
    except:
        loop_s = LOOP_DEFAULT_SEC

    excluded_str = input("Exclude categories/keywords (comma-separated, blank=none): ").strip()
    excluded = [x.strip().lower() for x in excluded_str.split(",") if x.strip()]

    out_dir = os.path.join(LOGS_DIR, folder)
    ensure_dir(out_dir)

    open_fname   = "log_open_markets.jsonl"
    closed_fname = "log_closed_markets.jsonl"

    open_path   = os.path.join(out_dir, open_fname)
    closed_path = os.path.join(out_dir, closed_fname)

    caps = [0.05,0.10,0.15,0.20,0.25,0.30,0.35,0.40,0.45,0.50,
            0.55,0.60,0.65,0.70,0.75,0.80,0.85,0.90,0.95]

    print(f"{dt_iso()} Starting cap-spread scan…")

    while True:
        markets_from_folders = read_unique_markets(folder)

        # load previous open/closed state
        prev_open_idx   = load_log_index(open_path)
        prev_closed_idx = load_log_index(closed_path)

        print(f"[STATE] prev_open={len(prev_open_idx)} prev_closed={len(prev_closed_idx)}")

        open_rows: List[dict] = []
        closed_rows: List[dict] = []
        skipped = 0
        errors = 0

        print(f"\n=== CAP SPREAD (folder={folder}) | markets={len(markets_from_folders)} ===")

        for i, (cid, meta) in enumerate(markets_from_folders.items(), 1):
            q = meta["question"]
            time_found_iso = meta["time_found"]
            tf_epoch = _parse_iso_to_epoch(time_found_iso) or 0

            if excluded and any(tok in q.lower() for tok in excluded):
                skipped += 1
                if i % 50 == 0 or len(markets_from_folders) <= 50:
                    print(f"[{i}/{len(markets_from_folders)}] SKIP → {q}")
                continue

            print(f"[{i}/{len(markets_from_folders)}] {q}  (cid={cid[:10]}…)  time_found={time_found_iso}")

            # previous state (either open or closed)
            prev_state = prev_open_idx.get(cid) or prev_closed_idx.get(cid)
            prev_status = prev_state.get("status") if prev_state else None
            prev_spread = prev_state.get("cap_spread") if prev_state else None

            try:
                m = fetch_market_full_by_cid(cid)
                status = current_status(m)  # 'YES', 'NO', 'TBD'
            except Exception as e:
                errors += 1
                print(f"  [WARN meta] {e}")
                status = "TBD"

            # If already closed and we have a closed row from before, we assume final and skip
            if status in ("YES","NO") and prev_state and prev_status in ("YES","NO"):
                print("    → already closed with cached spread, skipping.")
                if prev_status in ("YES","NO"):
                    closed_rows.append(prev_state)
                else:
                    open_rows.append(prev_state)
                continue

            # Decide baseline + prev_spread usage
            if status in ("YES","NO") and prev_state and prev_status != status:
                # Transition from open->closed: do a full backfill from time_found
                print("    → status changed to CLOSED, doing full backfill from time_found.")
                since_epoch = tf_epoch
                prev_spread_for_calc = None
            else:
                if prev_spread:
                    since_epoch = prev_spread.get("last_trade_ts", tf_epoch)
                    prev_spread_for_calc = prev_spread
                    print(f"    → incremental update since last_trade_ts={prev_spread.get('last_trade_ts')}")
                else:
                    since_epoch = tf_epoch
                    prev_spread_for_calc = None
                    print("    → first run for this market (no prev spread).")

            # Fetch trades since baseline
            try:
                trades = fetch_all_trades_since(cid, since_epoch, page_limit=TRADES_PAGE_LIMIT)
                print(f"    trades_fetched={len(trades)} since={since_epoch}")
            except Exception as e:
                errors += 1
                print(f"  [WARN trades] {e}")
                continue

            # Build / update cap_spread
            cap_spread = collect_cap_spread(
                trades=trades,
                caps=caps,
                since_epoch=since_epoch,
                prev_spread=prev_spread_for_calc,
            )

            # Per-market debug line
            line_parts = []
            for c in [0.30,0.35,0.40,0.45,0.50,0.55,0.60,0.65,0.70]:
                st = cap_spread["caps"].get(round(c,3), {"shares":0.0,"dollars":0.0})
                line_parts.append(f"{c:0.2f}: ${st['dollars']:.0f}/{st['shares']:.0f}")
            print("    [CAP SPREAD] " + " | ".join(line_parts))

            row = {
                "ts": dt_iso(),
                "status": status,
                "conditionId": cid,
                "question": q,
                "time_found": time_found_iso,
                "cap_spread": cap_spread,
            }

            if status in ("YES","NO"):
                closed_rows.append(row)
            else:
                open_rows.append(row)

        # ---------- Rewrite open/closed atomically ----------
        open_text = "\n".join(json.dumps(s, ensure_ascii=False) for s in open_rows)
        atomic_write_text(open_path, open_text + ("\n" if open_rows else ""))
        print(f"\n[WRITE] {len(open_rows)} open rows → {open_path} (rewritten this pass)")

        closed_text = "\n".join(json.dumps(s, ensure_ascii=False) for s in closed_rows)
        atomic_write_text(closed_path, closed_text + ("\n" if closed_rows else ""))
        print(f"[WRITE] {len(closed_rows)} closed rows → {closed_path} (rewritten this pass)")

        print(f"\n[SUMMARY] skipped={skipped} errors={errors} open={len(open_rows)} closed={len(closed_rows)}")

        if loop_s <= 0:
            break
        print(f"\nSleeping {loop_s}s… (Ctrl+C to stop)")
        try:
            time.sleep(loop_s)
        except KeyboardInterrupt:
            print("\nbye.")
            break

if __name__ == "__main__":
    main()
"""
breyta logic fyrir fill I fill á dict, refeer í það til að calck allt
imoprta decision logic fra sw2-3
hafa cap_spread reference log með timestamp fyrir seinasta trade checked, checka trades síðan þá ef of mörg trades, pull back come back later
3 logs open, closed, cap_spread
foundtmsp,conid,spread,spreadtmsp,tradecount,flags:status,list:trades till fill cap
cap_spread={0.3:0.0,0.35:0.0,0.4:0.0,0.45:0.0,0.5:0.0,0.55:0.0,0.6:0.0,0.65:0.0,0.7:0.0}
"""