import os, sys, glob, json, time, math, random
from datetime import datetime, timezone
from collections import defaultdict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Config ----------
BASE_GAMMA  = "https://gamma-api.polymarket.com/markets"
DATA_TRADES = "https://data-api.polymarket.com/trades"

LOGS_DIR = "logs"

# RPS limiter + backoff (gentle by default)
RPS_TARGET = 3.0
_last_tokens_ts = time.monotonic()
_bucket = RPS_TARGET

def _rate_limit():
    global _last_tokens_ts, _bucket
    now = time.monotonic()
    _bucket = min(RPS_TARGET, _bucket + (now - _last_tokens_ts) * RPS_TARGET)
    _last_tokens_ts = now
    if _bucket < 1.0:
        need = (1.0 - _bucket) / RPS_TARGET
        time.sleep(need)
        now2 = time.monotonic()
        _bucket = min(RPS_TARGET, _bucket + (now2 - _last_tokens_ts) * RPS_TARGET)
        _last_tokens_ts = now2
    _bucket -= 1.0

def make_session():
    s = requests.Session()
    retry = Retry(
        total=3, connect=3, read=3,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",)
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pm-no-bet-from-logs/1.0"})
    return s

SESSION = make_session()

def dt_iso():
    return datetime.now(timezone.utc).isoformat()

def _parse_iso_to_epoch(s: str):
    return int(datetime.fromisoformat(s.replace("Z","+00:00")).timestamp())

def http_get(url, params=None, timeout=20):
    back = 0.6
    tries = 0
    while True:
        _rate_limit()
        try:
            r = SESSION.get(url, params=params or {}, timeout=timeout)
        except requests.RequestException:
            time.sleep(back + random.random()*0.3)
            back = min(back*1.7, 20.0)
            tries += 1
            if tries >= 6:
                raise
            continue
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            wait = float(ra) if ra and ra.replace(".","",1).isdigit() else back
            time.sleep(wait + random.random()*0.3)
            back = min(back*1.8, 30.0)
            tries += 1
            if tries >= 6:
                r.raise_for_status()
            continue
        if 500 <= r.status_code < 600:
            time.sleep(back + random.random()*0.3)
            back = min(back*1.7, 20.0)
            tries += 1
            if tries >= 6:
                r.raise_for_status()
            continue
        r.raise_for_status()
        return r

# ---------- Trades fetching ----------
def fetch_trades_page(cid: str, limit=250, starting_before_s=None, offset=None):
    """
    DATA_TRADES supports:
      - market=<conditionId>
      - sort=desc
      - limit
      - starting_before (milliseconds)
      - offset
    """
    params = {"market": cid, "sort": "desc", "limit": int(limit)}
    if starting_before_s is not None:
        params["starting_before"] = int(starting_before_s * 1000)  # ms
    if offset is not None:
        params["offset"] = int(offset)
    r = http_get(DATA_TRADES, params=params, timeout=20)
    return r.json() or []

def _to_epoch_any(x):
    try:
        f = float(x)
        # support ms too
        return int(f/1000) if f > 1e12 else int(f)
    except Exception:
        return None

def fetch_all_trades_since(cid: str, since_epoch_s: int, page_limit=250):
    """
    Page trades backward by timestamp; fall back to offset if needed.
    Returns list of trade dicts (possibly large).
    """
    out = []
    starting_before = None
    last_len = None

    # Timestamp paging
    for _ in range(10000):
        data = fetch_trades_page(cid, limit=page_limit, starting_before_s=starting_before)
        if not data:
            break
        out.extend(data)
        oldest_ts = min(_to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in data)
        if oldest_ts <= since_epoch_s:
            break
        starting_before = oldest_ts
        if last_len == len(data):   # no progress
            break
        last_len = len(data)

    # Offset fallback
    if (not out) or (min((_to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0) for t in out) > since_epoch_s):
        offset = 0
        for _ in range(4000):
            data = fetch_trades_page(cid, limit=page_limit, offset=offset)
            if not data:
                break
            out.extend(data)
            oldest_ts = min(_to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in data)
            if oldest_ts <= since_epoch_s:
                break
            offset += len(data)

    # Dedup
    seen, uniq = set(), []
    for t in out:
        key = t.get("id") or (t.get("timestamp"), t.get("price"), t.get("size"), t.get("side"), t.get("outcome"))
        if key in seen:
            continue
        seen.add(key); uniq.append(t)
    return uniq

def should_skip_category(q: str, excluded: list) -> bool:
    """Check if question text matches any excluded keywords."""
    if not excluded:
        return False
    q_low = q.lower()
    for ex in excluded:
        if ex.lower() in q_low:
            return True
    return False

# ---------- Market meta (closed/winner) ----------
def fetch_market_meta(cid: str):
    """
    Pull one market by conditionId. Gamma doesn't have a direct by-cid route
    everywhere, so we scan a few pages and match on conditionId.
    We only need a couple pages (recent-most) for typical runs.
    """
    for closed_flag in (False, True):
        offset = 0
        for _ in range(20):  # ~5k rows max per sweep
            params = {
                "limit": 250,
                "order": "startDate",
                "ascending": False,
                "enableOrderBook": False,
                "closed": closed_flag,
                "offset": offset,
            }
            r = http_get(BASE_GAMMA, params=params, timeout=25)
            data = r.json() or []
            if not data:
                break
            for m in data:
                if (m.get("conditionId") or m.get("id")) == cid or m.get("conditionId") == cid:
                    # Return minimal fields
                    return {
                        "closed": bool(m.get("closed") is True or
                                       (str(m.get("winningOutcome") or "").lower() in ("yes","no"))),
                        "winner": (str(m.get("winningOutcome") or m.get("resolvedOutcome") or
                                       m.get("winner") or (m.get("result") or {}).get("winner") or ""))
                                   .strip().upper() or None
                    }
            if len(data) < 250:
                break
            offset += 250
    return {"closed": False, "winner": None}

# ---------- Core calc for NO side ----------
def calc_no_fill_and_print(trades: list, cap: float, since_epoch: int):
    """
    Iterate trades (all outcomes), keep only NO, and:
      - print every trade under cap (price<=cap) with ts, price, size
      - compute lowest ever NO price and total $ at/below cap
      - compute if bet can fill: return (filled, vwap, shares, dollars_under_cap, lowest_price, dollars_at_lowest)
    """
    # Collect NO trades since since_epoch
    no_trades = []
    for t in trades:
        side = str(t.get("outcome") or t.get("side") or "").lower()
        if side != "no":
            continue
        ts = _to_epoch_any(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0
        if ts < since_epoch:
            continue
        p = float(t.get("price", 0) or 0)
        s = float(t.get("size", 0) or 0)
        if p <= 0 or s <= 0:
            continue
        no_trades.append((ts, p, s))

    if not no_trades:
        return {
            "filled": False, "vwap": None, "shares": 0.0,
            "dollars_under_cap": 0.0, "lowest_price": None, "dollars_at_or_below_lowest": 0.0
        }

    # Lowest ever NO price and dollars at/below cap
    lowest_price = min(p for _, p, _ in no_trades)
    dollars_under_cap = sum(p * s for _, p, s in no_trades if p <= cap)

    # Print trades under cap as we go (sorted by time desc to mirror API)
    # (Limit extremely chatty output by printing at most first 50 under-cap)
    under_cap_trades = [(ts, p, s) for ts, p, s in no_trades if p <= cap]
    under_cap_trades.sort(key=lambda x: x[0], reverse=True)
    to_print = under_cap_trades[:50]
    for ts, p, s in to_print:
        print(f"  NO trade ≤ cap  ts={datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()}  p={p:.4f}  s={s:.4f}  $={p*s:.2f}")
    if len(under_cap_trades) > len(to_print):
        print(f"  ... +{len(under_cap_trades) - len(to_print)} more under-cap trades omitted ...")

    # Decide fill and VWAP for a bet of 'bet_size' (we'll compute later given cap)
    # To compute VWAP for the *cheapest-first* liquidity under cap:
    # sort under-cap trades ascending by price (cheapest first)
    uc_sorted = sorted([(p, s) for _, p, s in under_cap_trades], key=lambda x: x[0])
    return {
        "uc_sorted": uc_sorted,
        "dollars_under_cap": dollars_under_cap,
        "lowest_price": lowest_price,
        # calc below needs bet_size; we’ll finalize in-place later
    }

def take_under_cap_vwap(uc_sorted, bet_size):
    """
    Given under-cap trades sorted by price asc as (p, s),
    try to fill 'bet_size' notional. Return (filled, vwap, shares).
    """
    need = bet_size
    spent = 0.0
    shares = 0.0
    for p, s in uc_sorted:
        can_spend = p * s
        if can_spend >= need:
            # take partial
            take_shares = need / p
            spent += need
            shares += take_shares
            need = 0.0
            break
        else:
            spent += can_spend
            shares += s
            need -= can_spend
    if need > 1e-9:
        return False, None, shares  # not fully filled
    vwap = spent / max(shares, 1e-12)
    return True, vwap, shares

# ---------- File helpers ----------
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def list_market_files(folder):
    # pattern logs/<folder>/markets_*.jsonl (supports daily rolling files)
    return sorted(glob.glob(os.path.join(LOGS_DIR, folder, "markets_*.jsonl")))

def read_unique_markets(folder):
    """
    Read all markets_*.jsonl and return dict {conditionId: {"question":..., "time_found": earliest}}
    """
    files = list_market_files(folder)
    uniq = {}
    bad = 0
    rows = 0
    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                rows += 1
                try:
                    rec = json.loads(line)
                except Exception:
                    bad += 1
                    continue
                cid = rec.get("conditionId")
                q = rec.get("question")
                tf = rec.get("time_found")
                if not cid or not q or not tf:
                    continue
                prev = uniq.get(cid)
                if not prev:
                    uniq[cid] = {"question": q, "time_found": tf}
                else:
                    # keep earliest time_found
                    if _parse_iso_to_epoch(tf) < _parse_iso_to_epoch(prev["time_found"]):
                        uniq[cid]["time_found"] = tf
                        uniq[cid]["question"] = q
    print(f"[READ] files={len(files)} rows≈{rows} unique_by_conditionId={len(uniq)} bad_lines={bad}")
    return uniq

# ---------- Snapshot writer ----------
def append_jsonl(path, record):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

# ---------- Main loop ----------
def main():
    folder = input("Folder name under logs/: ").strip()
    try:
        bet_size = float(input("Bet size $ [10.0]: ").strip() or "10.0")
    except:
        bet_size = 10.0
    try:
        cap = float(input("Price cap [0.5]: ").strip() or "0.5")
    except:
        cap = 0.5
    try:
        loop_s = int(input("Loop seconds (0=single pass) [0]: ").strip() or "0")
    except:
        loop_s = 3600

    excluded_str = input("Exclude categories/keywords (comma-separated, blank=none): ").strip()
    excluded = [x.strip() for x in excluded_str.split(",") if x.strip()]

    out_dir = os.path.join(LOGS_DIR, folder)
    ensure_dir(out_dir)
    snap_path = os.path.join(out_dir, "no_bet_snapshots.jsonl")

    print(f"{dt_iso()} Starting scan…\n")
    while True:
        # Load unique markets from rolling files
        uniq = read_unique_markets(folder)
        print(f"\n=== NO bet backtest (folder={folder}) | markets={len(uniq)} ===")

        for i, (cid, meta) in enumerate(uniq.items(), 1):
            q = meta["question"]
            since_epoch = _parse_iso_to_epoch(meta["time_found"])

            print(f"[{i}/{len(uniq)}] {q}  (cid={cid[:10]}…)")
            # Fetch trades since time_found
            if should_skip_category(meta["question"], excluded):
                print(f"[{i}/{len(uniq)}] SKIP: excluded keyword match → {meta['question']}")
                continue
            try:
                trades = fetch_all_trades_since(cid, since_epoch, page_limit=250)
            except Exception as e:
                print(f"  [WARN trades] {e}")
                continue

            # Calc under-cap / lowest-ever for NO side and print under-cap trades
            res = calc_no_fill_and_print(trades, cap, since_epoch)

            # If there were no trades, store minimal row
            if "uc_sorted" not in res:
                row = {
                    "ts": dt_iso(),
                    "conditionId": cid,
                    "question": q,
                    "time_found": meta["time_found"],
                    "cap": cap,
                    "bet_size": bet_size,
                    "dollars_under_cap": 0.0,
                    "lowest_price_ever": res["lowest_price"],
                    "filled": False,
                    "fill_vwap": None,
                    "fill_shares": 0.0,
                    "winner": None,
                    "closed": False,
                    "pnl": None,
                }
                append_jsonl(snap_path, row)
                continue

            uc_sorted = res["uc_sorted"]
            dollars_under_cap = float(res["dollars_under_cap"])
            lowest_price_ever = float(res["lowest_price"])

            # Determine if bet fills and compute VWAP/shares
            filled, vwap, shares = take_under_cap_vwap(uc_sorted, bet_size)
            print(f"  dollars_under_cap=${dollars_under_cap:.2f}  lowest_NO_price={lowest_price_ever:.4f}")
            if filled:
                print(f"  FILL OK: vwap={vwap:.4f} shares={shares:.6f} (notional ${bet_size:.2f})")
            else:
                print(f"  FILL NO: only ${dollars_under_cap:.2f} ≤ cap (need ${bet_size:.2f})")

            # Fetch market meta (closed/winner) to compute P/L when available
            meta2 = fetch_market_meta(cid)
            closed = bool(meta2.get("closed"))
            winner = meta2.get("winner")
            pnl = None
            if filled and winner in ("YES", "NO"):
                if winner == "NO":
                    pnl = shares * (1.0 - vwap)  # payout $1 per share
                else:
                    pnl = -shares * vwap         # loses full paid price

            row = {
                "ts": dt_iso(),
                "conditionId": cid,
                "question": q,
                "time_found": meta["time_found"],
                "cap": cap,
                "bet_size": bet_size,
                "dollars_under_cap": round(dollars_under_cap, 2),
                "lowest_price_ever": round(lowest_price_ever, 6),
                "filled": filled,
                "fill_vwap": None if vwap is None else round(vwap, 6),
                "fill_shares": round(shares, 6),
                "winner": winner,
                "closed": closed,
                "pnl": None if pnl is None else round(pnl, 2),
            }
            append_jsonl(snap_path, row)

        print(f"\nSnapshots saved to {snap_path}")
        if loop_s <= 0:
            break
        print(f"Sleeping {loop_s}s… (Ctrl+C to stop)")
        try:
            time.sleep(loop_s)
        except KeyboardInterrupt:
            print("\nbye.")
            break

if __name__ == "__main__":
    main()