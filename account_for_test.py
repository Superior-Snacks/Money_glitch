import json
import time
import requests
from datetime import datetime, timezone
import time, json, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import re
import random, time
import os, json, time, math
from datetime import datetime, timedelta, timezone
import gzip
import glob
import os, glob, gzip, json, time

#!/usr/bin/env python3
import os, re, json, gzip, time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Iterable, Tuple, Optional

# ----------------------------
# CONFIG
# ----------------------------
BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_BOOK  = "https://clob.polymarket.com/book"
BASE_PHIST = "https://clob.polymarket.com/prices-history"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "pl-bot/1.0"})
LOG_DIR = "logs"
TRADE_LOG_GLOB = os.path.join(LOG_DIR, "trades_taken_*.jsonl*")   # picks .jsonl and .jsonl.gz
OUTPUT_ALL         = os.path.join(LOG_DIR, "pl_timeseries_all.jsonl")
OUTPUT_NO_CRYPTO   = os.path.join(LOG_DIR, "pl_timeseries_no_crypto.jsonl")
UPDATE_INTERVAL_S  = 3600   # run hourly
BOOK_DEPTH = 1              # only need top of book to MTM at bid
RPS_TARGET = 5.0            # gentle rate limit for /book

BASE_BOOK  = "https://clob.polymarket.com/book"

# same fees as your main watcher to keep apples-to-apples (MTM uses raw bid; P/L uses cost basis)
SETTLE_FEE = 0.01

# crude crypto detector (expand as you like)
CRYPTO_WORDS = {
    "crypto","coin","token","blockchain","defi","stablecoin",
    "bitcoin","btc","ethereum","eth","solana","sol","xrp","ripple","doge","dogecoin",
    "ada","cardano","bnb","ton","shib","litecoin","ltc","avalanche","avax","tron","trx",
    "chainlink","link","polkadot","dot","near","aptos","apt","arbitrum","arb","base",
    "matic","polygon","pepe","sui","kaspa","kas","sei",
}

# ----------------------------
# small helpers
# ----------------------------

# --- simple per-run caches ---
_market_meta_cache: dict[str, dict] = {}
_best_bid_cache: dict[str, float | None] = {}
_last_price_cache: dict[str, float | None] = {}

def fetch_market_meta_cached(condition_id: str) -> dict | None:
    m = _market_meta_cache.get(condition_id)
    if m is not None:
        return m
    m = fetch_market_meta(condition_id)
    _market_meta_cache[condition_id] = m
    return m


def _rate_limit_factory(rps: float):
    last_ts = time.monotonic()
    bucket = rps
    def _consume():
        nonlocal last_ts, bucket
        now = time.monotonic()
        bucket = min(rps, bucket + (now - last_ts) * rps)
        last_ts = now
        if bucket < 1.0:
            need = (1.0 - bucket)/rps
            time.sleep(need)
            now2 = time.monotonic()
            bucket = min(rps, bucket + (now2 - last_ts) * rps)
            last_ts = now2
        bucket -= 1.0
    return _consume

consume_token = _rate_limit_factory(RPS_TARGET)

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET","POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pl-timeseries-reporter/1.0"})
    return s

SESSION = make_session()

def outcome_map_from_market(m: dict):
    outs = m.get("outcomes"); toks = m.get("clobTokenIds")
    if isinstance(outs, str):
        try: outs = json.loads(outs)
        except: outs = None
    if isinstance(toks, str):
        try: toks = json.loads(toks)
        except: toks = None
    if isinstance(outs, list) and isinstance(toks, list) and len(outs)==2 and len(toks)==2:
        o0,o1 = str(outs[0]).strip().upper(), str(outs[1]).strip().upper()
        if (o0,o1)==("YES","NO"):  return {"YES": toks[0], "NO": toks[1]}
        if (o0,o1)==("NO","YES"):  return {"YES": toks[1], "NO": toks[0]}
        if "YES" in o0 and "NO" in o1: return {"YES": toks[0], "NO": toks[1]}
        if "NO"  in o0 and "YES" in o1: return {"YES": toks[1], "NO": toks[0]}
    if isinstance(toks, list) and len(toks)==2:
        return {"YES": toks[0], "NO": toks[1]}
    raise ValueError("cannot resolve YES/NO tokens")


def fetch_market_meta(condition_id: str) -> dict | None:
    try:
        r = SESSION.get(BASE_GAMMA, params={"condition_ids": condition_id, "limit": 1}, timeout=15)
        r.raise_for_status()
        rows = r.json() or []
        for m in rows:
            if m.get("conditionId") == condition_id:
                return m
    except Exception:
        pass
    return None

def fetch_best_bid_for_no_token(token_id: str) -> float | None:
    if token_id in _best_bid_cache:
        return _best_bid_cache[token_id]
    try:
        consume_token()  # rate-limit
        r = SESSION.get(BASE_BOOK, params={"token_id": token_id}, timeout=15)
        if r.status_code == 404:
            _best_bid_cache[token_id] = None
            return None
        r.raise_for_status()
        book = r.json() or {}
        bids = book.get("bids") or []
        best = max((float(b["price"]) for b in bids if "price" in b), default=None)
        _best_bid_cache[token_id] = best
        return best
    except Exception:
        _best_bid_cache[token_id] = None
        return None

def fetch_last_no_trade_price(token_id: str) -> float | None:
    if token_id in _last_price_cache:
        return _last_price_cache[token_id]
    try:
        consume_token()  # rate-limit
        r = SESSION.get(BASE_PHIST, params={"token_id": token_id, "resolution": "1h", "limit": 1}, timeout=15)
        r.raise_for_status()
        rows = r.json() or []
        price = float(rows[-1].get("price", 0)) if rows else 0.0
        val = price or None
        _last_price_cache[token_id] = val
        return val
    except Exception:
        _last_price_cache[token_id] = None
        return None


def parse_dt_iso(s: str) -> datetime:
    # tolerate no tz by assuming UTC; prefer ISO strings from your logs with +00:00
    try:
        return datetime.fromisoformat(s.replace("Z","+00:00"))
    except Exception:
        # last resort: treat as UTC naive
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)

def iter_trade_files() -> List[str]:
    import glob
    return sorted(glob.glob(TRADE_LOG_GLOB))

def open_maybe_gz(path: str, mode: str = "rt"):
    # Always read/write text as UTF-8; tolerate bad bytes.
    if path.endswith(".gz"):
        return gzip.open(path, mode, encoding="utf-8", errors="replace")
    return open(path, mode, encoding="utf-8", errors="replace")

def iter_trade_records():
    for path in iter_trade_files():
        try:
            with open_maybe_gz(path, "rt") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except Exception:
                        # If a single line is corrupted, skip it rather than crash.
                        continue
        except FileNotFoundError:
            continue

def earliest_trade_date_utc(trades: List[dict]) -> Optional[datetime]:
    dts = []
    for r in trades:
        ts = r.get("ts")
        if not ts: continue
        try:
            dts.append(parse_dt_iso(ts))
        except Exception:
            pass
    return min(dts).astimezone(timezone.utc) if dts else None

def is_crypto_market(rec: dict) -> bool:
    # check slug and question fields as your trade log provides both
    q = (rec.get("question") or "").lower()
    s = (rec.get("market_slug") or "").lower()
    text = f"{q} {s}"
    return any(w in text for w in CRYPTO_WORDS)

# ----------------------------
# positions + MTM
# ----------------------------
def build_positions_for_start(trades: List[dict], start_dt_utc: datetime, include_crypto: bool) -> Dict[str, dict]:
    """
    Build NO-only positions from trades occurring at/after start_dt_utc.
    Returns: market_id -> { 'shares': float, 'cost': float, 'side': 'NO', 'no_token_id': str }
    """
    pos: Dict[str, dict] = {}
    for rec in trades:
        if rec.get("side") != "NO":
            continue
        ts = rec.get("ts")
        if not ts: continue
        try:
            t_dt = parse_dt_iso(ts).astimezone(timezone.utc)
        except Exception:
            continue
        if t_dt < start_dt_utc:
            continue
        if not include_crypto and is_crypto_market(rec):
            continue

        mid = rec.get("market_id")
        shares = float(rec.get("shares", 0.0) or 0.0)
        cost   = float(rec.get("spent_after", 0.0) or 0.0)
        tok    = rec.get("token_id")  # NO token in your logs
        if not (mid and tok) or shares <= 0 or cost < 0:
            continue
        st = pos.setdefault(mid, {"shares": 0.0, "cost": 0.0, "side": "NO", "no_token_id": tok})
        st["shares"] += shares
        st["cost"]   += cost
    return pos

SETTLE_FEE = 0.01  # same as your live script

def mtm_no_position(condition_id: str, shares: float) -> tuple[float, str]:
    """
    Returns (mark_value, status_note)
    - If resolved and NO won: value = shares * (1 - SETTLE_FEE)
    - If resolved and YES won: value = 0
    - If open: use best bid on NO; if no book, use last price; if none, 0.
    """
    m = fetch_market_meta(condition_id)
    if not m:
        # No meta: treat as conservative 0 mark
        return 0.0, "no_meta"

    # Resolution path
    resolved = bool(m.get("resolved") or m.get("isResolved"))
    winning = (m.get("winningOutcome") or m.get("winner") or "").strip().upper()
    if resolved:
        if winning == "NO":
            return shares * (1.0 - SETTLE_FEE), "resolved_NO"
        elif winning == "YES":
            return 0.0, "resolved_YES"
        else:
            # Resolved but winner missing? conservative 0
            return 0.0, "resolved_unknown"

    # Still open → get NO token and try book/last
    try:
        om = outcome_map_from_market(m)
        no_tok = om["NO"]
    except Exception:
        return 0.0, "open_tokens_unknown"

    bid = fetch_best_bid_for_no_token(no_tok)
    if bid is not None:
        return shares * bid, "open_bid"

    lastp = fetch_last_no_trade_price(no_tok)
    if lastp is not None:
        return shares * lastp, "open_last"

    return 0.0, "open_unpriced"

def mark_to_market(positions_by_market: dict[str, dict]):
    total_cost = 0.0
    total_val  = 0.0
    nmk = 0
    for mid, p in positions_by_market.items():
        shares = float(p.get("shares", 0.0))
        cost   = float(p.get("cost", 0.0))
        if shares <= 0:
            continue
        nmk += 1
        total_cost += cost
        val, note = mtm_no_position(mid, shares)
        total_val += val
        # (Optional) log per-market MTM notes somewhere if you want:
        # print(f"[MTM] {mid} shares={shares:.2f} cost={cost:.2f} val={val:.2f} note={note}")
    return total_cost, total_val, nmk


def per_market_mark(mid: str, shares: float, cost: float) -> dict:
    """
    Returns a dict describing this position's status and values.
    Keys: status ('open'|'resolved_NO'|'resolved_YES'|'resolved_unknown'|'no_meta'|'open_unpriced'|'open_last'|'open_bid'),
          realized_value, unrealized_value, realized_pl, unrealized_pl
    """
    m = fetch_market_meta_cached(mid)
    if not m:
        return {
            "status": "no_meta",
            "realized_value": 0.0,
            "unrealized_value": 0.0,
            "realized_pl": 0.0,
            "unrealized_pl": -cost,  # conservative (no mark)
        }

    resolved = bool(m.get("resolved") or m.get("isResolved"))
    winner   = (m.get("winningOutcome") or m.get("winner") or "").strip().upper()

    if resolved:
        if winner == "NO":
            rv = shares * (1.0 - SETTLE_FEE)
            return {
                "status": "resolved_NO",
                "realized_value": rv,
                "unrealized_value": 0.0,
                "realized_pl": rv - cost,
                "unrealized_pl": 0.0,
            }
        if winner == "YES":
            return {
                "status": "resolved_YES",
                "realized_value": 0.0,
                "unrealized_value": 0.0,
                "realized_pl": -cost,
                "unrealized_pl": 0.0,
            }
        # resolved but winner missing
        return {
            "status": "resolved_unknown",
            "realized_value": 0.0,
            "unrealized_value": 0.0,
            "realized_pl": -cost,  # safest
            "unrealized_pl": 0.0,
        }

    # OPEN → bid/last/zero fallback
    try:
        om = outcome_map_from_market(m)
        no_tok = om["NO"]
    except Exception:
        return {
            "status": "open_tokens_unknown",
            "realized_value": 0.0,
            "unrealized_value": 0.0,
            "realized_pl": 0.0,
            "unrealized_pl": -cost,
        }

    bid = fetch_best_bid_for_no_token(no_tok)
    if bid is not None:
        uv = shares * bid
        return {
            "status": "open_bid",
            "realized_value": 0.0,
            "unrealized_value": uv,
            "realized_pl": 0.0,
            "unrealized_pl": uv - cost,
        }

    lastp = fetch_last_no_trade_price(no_tok)
    if lastp is not None:
        uv = shares * lastp
        return {
            "status": "open_last",
            "realized_value": 0.0,
            "unrealized_value": uv,
            "realized_pl": 0.0,
            "unrealized_pl": uv - cost,
        }

    # no price
    return {
        "status": "open_unpriced",
        "realized_value": 0.0,
        "unrealized_value": 0.0,
        "realized_pl": 0.0,
        "unrealized_pl": -cost,
    }


def breakdown_pl(positions_by_market: dict[str, dict]) -> dict:
    stats = {
        "positions_count": 0,
        "markets_marked": 0,

        "open_count": 0,
        "open_cost": 0.0,
        "open_mtm_value": 0.0,
        "unrealized_pl": 0.0,

        "closed_count": 0,
        "closed_cost": 0.0,
        "realized_value": 0.0,
        "realized_pl": 0.0,

        "total_cost": 0.0,
        "mtm_value": 0.0,   # realized_value + open_mtm_value
        "pl_total": 0.0,    # realized_pl + unrealized_pl
    }

    for mid, p in positions_by_market.items():
        shares = float(p.get("shares", 0.0))
        cost   = float(p.get("cost", 0.0))
        if shares <= 0 or cost < 0:
            continue

        stats["positions_count"] += 1
        marks = per_market_mark(mid, shares, cost)
        stats["markets_marked"]  += 1

        status = marks["status"]
        rv = float(marks["realized_value"])
        uv = float(marks["unrealized_value"])
        rpl = float(marks["realized_pl"])
        upl = float(marks["unrealized_pl"])

        if status.startswith("resolved"):
            stats["closed_count"] += 1
            stats["closed_cost"]  += cost
            stats["realized_value"] += rv
            stats["realized_pl"] += rpl
        else:
            stats["open_count"] += 1
            stats["open_cost"]  += cost
            stats["open_mtm_value"] += uv
            stats["unrealized_pl"] += upl

    stats["total_cost"] = stats["open_cost"] + stats["closed_cost"]
    stats["mtm_value"]  = stats["realized_value"] + stats["open_mtm_value"]
    stats["pl_total"]   = stats["realized_pl"] + stats["unrealized_pl"]
    return stats


def write_breakdown_line(start_date: str, variant: str, positions: dict, outfile: str):
    as_of = datetime.now(timezone.utc).isoformat()
    stats = breakdown_pl(positions)
    rec = {
        "run_ts": as_of,
        "as_of_ts": as_of,
        "start_date": start_date,
        "variant": variant,

        # counts
        "positions_count": stats["positions_count"],
        "markets_marked":  stats["markets_marked"],
        "open_count":      stats["open_count"],
        "closed_count":    stats["closed_count"],

        # dollars
        "open_cost":        stats["open_cost"],
        "open_mtm_value":   stats["open_mtm_value"],
        "unrealized_pl":    stats["unrealized_pl"],
        "closed_cost":      stats["closed_cost"],
        "realized_value":   stats["realized_value"],
        "realized_pl":      stats["realized_pl"],

        # totals / reconciliation
        "total_cost": stats["total_cost"],
        "mtm_value":  stats["mtm_value"],
        "pl_total":   stats["pl_total"],
    }
    with open(outfile, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

# ----------------------------
# output
# ----------------------------
def append_jsonl(path: str, rec: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

# ----------------------------
# main loop
# ----------------------------
def compute_and_write_once(include_crypto: bool):
    # 1) load all trades into memory once (daily volumes are fine)
    trades = list(iter_trade_records())
    if not trades:
        now = datetime.now(timezone.utc).isoformat()
        print(f"[{now}] no trades found. nothing to do.")
        return

    earliest = earliest_trade_date_utc(trades)
    if earliest is None:
        now = datetime.now(timezone.utc).isoformat()
        print(f"[{now}] could not parse earliest trade date.")
        return

    now_dt = datetime.now(timezone.utc)
    # from earliest day to today, inclusive
    total_days = (now_dt.date() - earliest.date()).days
    start_dates = [datetime.combine(earliest.date() + timedelta(days=i), 
                                    datetime.min.time(), tzinfo=timezone.utc)
                   for i in range(total_days + 1)]

    variant = "all" if include_crypto else "no_crypto"
    out_path = OUTPUT_ALL if include_crypto else OUTPUT_NO_CRYPTO
    print(f"[RUN] {now_dt.isoformat()} variant={variant} days={len(start_dates)}")

    for sd in start_dates:
        # rebuild per-run caches each start-date pass to avoid cross-contamination if you prefer:
        _market_meta_cache.clear()
        _best_bid_cache.clear()
        _last_price_cache.clear()

        positions = build_positions_for_start(trades, sd, include_crypto=include_crypto)

        # write full realized/unrealized breakdown (includes totals & counts)
        write_breakdown_line(sd.date().isoformat(), variant, positions, out_path)

        # optional console line
        print(f"  {variant} | start={sd.date().isoformat()} pos={len(positions)} → wrote breakdown")

def main():
    import argparse
    p = argparse.ArgumentParser(description="Continuous P/L MTM timeseries reporter")
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--include-crypto", action="store_true", help="Include crypto markets (default)")
    g.add_argument("--exclude-crypto", action="store_true", help="Exclude crypto markets")
    p.add_argument("--once", action="store_true", help="Run once and exit (no loop)")
    p.add_argument("--interval", type=int, default=UPDATE_INTERVAL_S, help="Seconds between updates (default hourly)")
    args = p.parse_args()

    include_crypto = not args.exclude_crypto

    if args.once:
        compute_and_write_once(include_crypto)
        return

    while True:
        try:
            compute_and_write_once(include_crypto)
        except Exception as e:
            print(f"[WARN] compute error: {e}")
        time.sleep(max(60, args.interval))

if __name__ == "__main__":
    main()