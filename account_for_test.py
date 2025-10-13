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
from datetime import datetime, timezone
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

#!/usr/bin/env python3
import os, re, json, gzip, time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Iterable, Tuple, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
    try:
        r = SESSION.get(BASE_BOOK, params={"token_id": token_id}, timeout=15)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        book = r.json() or {}
        bids = book.get("bids") or []
        best = max((float(b["price"]) for b in bids if "price" in b), default=None)
        return best
    except Exception:
        return None

def fetch_last_no_trade_price(token_id: str) -> float | None:
    try:
        r = SESSION.get(BASE_PHIST, params={"token_id": token_id, "resolution": "1h", "limit": 1}, timeout=15)
        r.raise_for_status()
        rows = r.json() or []
        if rows:
            # prices-history returns objects with "price" (ask/bid mid). Treat as last traded/quoted price.
            return float(rows[-1].get("price", 0)) or None
    except Exception:
        pass
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

    # cache of token->bid inside this run
    # (We already dedup in mark_to_market; keeping simple.)
    for sd in start_dates:
        positions = build_positions_for_start(trades, sd, include_crypto=include_crypto)
        total_cost, total_val, nmk = mark_to_market(positions)
        pl_mtm = total_val - total_cost

        rec = {
            "run_ts": now_dt.isoformat(),
            "as_of_ts": now_dt.isoformat(),
            "start_date": sd.date().isoformat(),
            "variant": variant,
            "positions_count": len(positions),
            "markets_marked": nmk,
            "total_cost": round(float(total_cost), 6),
            "mtm_value": round(float(total_val), 6),
            "pl_mtm": round(float(pl_mtm), 6),
        }
        append_jsonl(out_path, rec)
        # short print for visibility
        print(f"  {variant} | start={rec['start_date']} pos={rec['positions_count']} "
              f"cost={rec['total_cost']:.2f} mtm={rec['mtm_value']:.2f} P/L={rec['pl_mtm']:.2f}")

    print(f"[DONE] wrote to {out_path}")

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