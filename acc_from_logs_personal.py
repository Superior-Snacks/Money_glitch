import os, sys, json, random, glob, time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import subprocess

# ======================= Config / Constants =======================
BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"
LOGS_DIR     = "logs"

TRADES_PAGE_LIMIT = 250
LOOP_DEFAULT_SEC  = 3600  # 0 = single pass

# For determining closed winners when explicit winner not present
HINT_SPREAD = 0.98
FINAL_GRACE = timedelta(days=2)

# Adaptive rate limit
RPS_TARGET           = 1.0   # slightly conservative
_RPS_SCALE           = 1.0
_RPS_MIN             = 0.3
_RPS_RECOVER_PER_SEC = 0.03

# Caps for maker-style spread
CAPS = [0.001, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1] #full
#CAPS = [0.05, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.95] #for better visibility during tests

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
    s.headers.update({"User-Agent": "pm-maker-cap-spread/1.0"})
    return s

SESSION = make_session()
_last_tokens_ts = time.monotonic()
_bucket = RPS_TARGET
_num_429 = 0   # for heartbeat

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
    """
    Very conservative HTTP GET with backoff + token-bucket RPS limiting,
    and prints when we hit 429.
    """
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

def _parse_iso_to_epoch(s: Optional[str]) -> Optional[int]:
    if not s: return None
    try:
        return int(datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc).timestamp())
    except Exception:
        return None

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

# ======================= Load markets from logs ===================
def read_unique_markets(folder_name: str) -> Dict[str, dict]:
    """
    Reads logs/<folder>/markets_*.jsonl with:
      {"time_found": ISO, "conditionId": str, "question": str}
    Keeps earliest time_found per conditionId.
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

def read_spread_index(path: str) -> Dict[str, dict]:
    """
    Reads a JSONL file of rows that contain at least:
      { "conditionId": ..., "status": ..., "cap_spread": {...} }
    Returns {cid: row}.
    """
    idx = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                cid = row.get("conditionId")
                if cid:
                    idx[cid] = row
    except FileNotFoundError:
        pass
    return idx

# ======================= Market meta / status =====================
MARKET_META_CACHE: Dict[str, dict] = {}

def fetch_market_full_by_cid(cid: str) -> dict:
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
    after: Optional[int] = None,
) -> List[dict]:
    """
    One page of trades using the Polymarket trades API:

      - market: conditionId
      - after:  unix timestamp (sec); only trades strictly after this time
      - limit:  page size
    """
    params = {"market": cid, "limit": int(limit)}
    if after is not None:
        params["after"] = int(after)

    r = http_get_with_backoff(DATA_TRADES, params=params, timeout=20)
    return r.json() or []

def fetch_all_trades_since(
    cid: str,
    since_epoch_s: int,
    page_limit: int = TRADES_PAGE_LIMIT,
    hard_cap_pages: int = 5000,
) -> List[dict]:
    """
    Incremental trade fetch:

      - Only returns trades with ts >= since_epoch_s.
      - Uses `after=<cursor>` where cursor starts at since_epoch_s and
        moves forward to the newest timestamp we've actually *seen*.
      - De-dups by trade id.
      - Stops when:
          * a page is empty, OR
          * page adds 0 new trades, OR
          * page < page_limit, OR
          * hard_cap_pages is reached.
    """
    cursor = int(since_epoch_s)
    seen_ids: set = set()
    uniq: List[dict] = []

    start_time = time.monotonic()
    print(f"    [FETCH] cid={cid[:12]}… baseline={since_epoch_s}")

    for page_no in range(1, hard_cap_pages + 1):
        elapsed = time.monotonic() - start_time
        bar_len = min(20, page_no)
        bar = "[" + "#" * bar_len + "-" * (20 - bar_len) + "]"
        print(f"      [PAGE {page_no:4d}/{hard_cap_pages}] {bar} after={cursor} elapsed={elapsed:5.1f}s")

        page = fetch_trades_page_ms(cid, limit=page_limit, after=cursor)
        if not page:
            print("        → empty page, stopping")
            break

        added = 0
        max_ts_on_page = cursor

        for t in page:
            tid = t.get("id")
            if not tid:
                tid = f"{t.get('taker_order_id')}-{t.get('market')}-{t.get('price')}-{t.get('size')}-{t.get('side')}-{t.get('outcome')}-{t.get('bucket_index')}"

            if tid in seen_ids:
                continue

            ts = trade_ts(t)
            if ts < since_epoch_s:
                continue

            seen_ids.add(tid)
            uniq.append(t)
            added += 1
            if ts > max_ts_on_page:
                max_ts_on_page = ts

        print(f"        → got {len(page)} trades")
        print(f"        → added {added}, total_unique={len(uniq)}")

        if added == 0:
            print("        [STOP] no new trades beyond current cursor; stopping")
            break

        if max_ts_on_page <= cursor:
            print("        [STOP] max_ts_on_page <= cursor (API likely repeating pages); stopping")
            break

        cursor = max_ts_on_page

        if len(page) < page_limit:
            print("        [STOP] short page (likely end of history)")
            break

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
    """
    Build (or update) a maker-style cap spread for a single market.

    Logic:
      - Only considers NO-side trades
      - Only trades with ts >= since_epoch
      - For each trade (price p, size s), it adds liquidity to all caps c where c >= p
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

    # Make JSON-friendly (stringify caps)
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

# ======================= Overview / heartbeat =====================
def print_overview(open_rows, closed_rows, skipped, errors, skipped_closed_final, new_markets):
    print("\n===== PASS OVERVIEW (cap spread builder) =====")
    print(f"Open markets saved:         {len(open_rows)}")
    print(f"Closed markets saved:       {len(closed_rows)}")
    print(f"Skipped (filters):          {skipped}")
    print(f"Skipped (already closed):   {skipped_closed_final}")
    print(f"New markets this pass:      {new_markets}")
    print(f"Errors (fetch/etc):         {errors}")

# ======================= Main ====================================
def main():
    global _num_429

    folder = input("Folder name under logs/: ").strip()
    try:
        loop_s = int(input("Loop seconds (0=single pass) [3600]: ").strip() or "3600")
    except:
        loop_s = LOOP_DEFAULT_SEC

    excluded_str = input("Exclude categories/keywords (comma-separated, blank=none): ").strip()
    excluded = [x.strip().lower() for x in excluded_str.split(",") if x.strip()]

    out_dir = os.path.join(LOGS_DIR, folder)
    ensure_dir(out_dir)
    open_path   = os.path.join(out_dir, "log_open_markets_cap_spread.jsonl")
    closed_path = os.path.join(out_dir, "log_closed_markets_cap_spread.jsonl")

    print(f"{dt_iso()} Starting cap-spread scan… caps={CAPS}")

    while True:
        run_start = time.monotonic()
        _num_429 = 0
        global_trades_fetched = 0

        # Load previous spreads
        prev_open_idx   = read_spread_index(open_path)
        prev_closed_idx = read_spread_index(closed_path)

        # Set of all markets we've seen before in open/closed logs
        prev_any_idx = set(prev_open_idx.keys()) | set(prev_closed_idx.keys())

        markets = read_unique_markets(folder)
        total_markets = len(markets)
        print(f"\n=== MAKER cap spread (folder={folder}) | markets={total_markets} ===")

        open_rows: List[dict] = []
        closed_rows: List[dict] = []
        skipped = 0
        errors  = 0
        skipped_closed_final = 0
        new_markets = 0

        for i, (cid, meta) in enumerate(markets.items(), 1):
            q = meta["question"]
            baseline = _parse_iso_to_epoch(meta["time_found"]) or 0

            # Skip already-final closed markets (we assume spread won't change)
            if cid in prev_closed_idx:
                skipped_closed_final += 1
                print(f"[{i}/{total_markets}] SKIP (already closed final) → {q}")
                continue

            # Skip by filter
            if excluded and any(tok in q.lower() for tok in excluded):
                skipped += 1
                if i % 50 == 0 or total_markets <= 50:
                    print(f"[{i}/{total_markets}] SKIP (filter) → {q}")
                continue

            # Detect new markets
            if cid not in prev_any_idx:
                new_markets += 1
                print(f"\n[{i}/{total_markets}] NEW market → {q}  (cid={cid[:10]}…)  time_found={meta['time_found']}")
            else:
                print(f"\n[{i}/{total_markets}] {q}  (cid={cid[:10]}…)  time_found={meta['time_found']}")

            prev_row    = prev_open_idx.get(cid) or prev_closed_idx.get(cid)
            prev_status = (prev_row or {}).get("status")
            prev_spread = (prev_row or {}).get("cap_spread")

            # Fetch meta & status
            try:
                m = fetch_market_full_by_cid(cid)
                status = current_status(m)   # 'YES','NO','TBD'
            except Exception as e:
                errors += 1
                print(f"  [WARN meta] {e}")
                status = "TBD"

            # Decide baseline for this run
            if prev_spread is None:
                since_epoch = baseline
                print(f"    → first run for this market (no prev spread).")
            else:
                since_epoch = int(prev_spread.get("last_trade_ts", baseline))
                since_epoch = max(since_epoch, baseline)
                print(f"    → incremental from last_trade_ts={since_epoch} (prev_status={prev_status})")

            # Fetch only new trades >= since_epoch
            try:
                trades = fetch_all_trades_since(cid, since_epoch, page_limit=TRADES_PAGE_LIMIT)
            except Exception as e:
                errors += 1
                print(f"  [WARN trades] {e}")
                continue

            global_trades_fetched += len(trades)

            # Update / build cap spread
            cap_spread = collect_cap_spread(
                trades,
                caps=CAPS,
                since_epoch=since_epoch,
                prev_spread=prev_spread,
            )

            # Debug print cap spread summary
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
                "status": status,                     # 'YES' | 'NO' | 'TBD'
                "conditionId": cid,
                "question": q,
                "time_found": meta["time_found"],
                "cap_spread": cap_spread,
            }

            if status in ("YES","NO"):
                closed_rows.append(row)
            else:
                open_rows.append(row)

            # -------- Global heartbeat every 25 markets --------
            if i % 25 == 0 or i == total_markets:
                elapsed = time.monotonic() - run_start
                print(
                    f"\n[HEARTBEAT] markets_done={i}/{total_markets} | "
                    f"new_trades_this_pass={global_trades_fetched} | "
                    f"429_hits={_num_429} | elapsed={elapsed/60:.1f}m"
                )

        # ---------- Rewrite open / closed spreads ----------
        open_text = "\n".join(json.dumps(s, ensure_ascii=False) for s in open_rows)
        atomic_write_text(open_path, open_text + ("\n" if open_rows else ""))
        print(f"\n[WRITE] {len(open_rows)} open rows → {open_path} (rewritten this pass)")

        closed_text = "\n".join(json.dumps(s, ensure_ascii=False) for s in closed_rows)
        atomic_write_text(closed_path, closed_text + ("\n" if closed_rows else ""))
        print(f"[WRITE] {len(closed_rows)} closed rows → {closed_path} (rewritten this pass)")

        # ---------- Overview ----------
        print_overview(
            open_rows,
            closed_rows,
            skipped=skipped,
            errors=errors,
            skipped_closed_final=skipped_closed_final,
            new_markets=new_markets,
        )

        if loop_s <= 0:
            break
        elapsed_run = time.monotonic() - run_start
        print(f"\n[LOOP] pass finished in {elapsed_run/60:.1f}m, sleeping {loop_s}s… (Ctrl+C to stop)")
        try:
            time.sleep(loop_s)
        except KeyboardInterrupt:
            print("\nbye.")
            break

if __name__ == "__main__":
    main()
    subprocess.call(["python", "status_check.py"])