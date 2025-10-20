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
LOG_DIR = input("name log to check:")
TRADE_LOG_GLOB = os.path.join(LOG_DIR, "trades_taken_*.jsonl*")   # picks .jsonl and .jsonl.gz
OUTPUT_ALL         = os.path.join(LOG_DIR, "pl_timeseries_all.jsonl")
OUTPUT_NO_CRYPTO   = os.path.join(LOG_DIR, "pl_timeseries_no_crypto.jsonl")
UPDATE_INTERVAL_S  = 3600   # run hourly
BOOK_DEPTH = 1              # only need top of book to MTM at bid
RPS_TARGET = 5.0            # gentle rate limit for /book
CLOSED_NO = os.path.join(LOG_DIR, "closed_no.jsonl")
CLOSED_YES = os.path.join(LOG_DIR, "closed_yes.jsonl")

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


def read_last_snapshot_for(outfile: str, variant: str, start_date: str) -> dict | None:
    """
    Read the most recent line in `outfile` for the given (variant, start_date).
    Returns that JSON record or None if not found.
    """
    if not os.path.exists(outfile):
        return None
    last = None
    with open(outfile, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("variant") == variant and rec.get("start_date") == start_date:
                last = rec
    return last


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


# --- YES/NO mix tracking -----------------------------------------------
YESNO_PROGRESS_PATH = os.path.join(LOG_DIR, "yesno_progress.jsonl")

def resolved_timestamp(m: dict) -> datetime | None:
    # best-effort: when did it (likely) finalize?
    for k in ("closedTime", "umaEndDate", "endDate", "updatedAt"):
        dt = parse_dt_any(m.get(k))
        if dt:
            return dt
    return None

def tally_yes_no(positions: dict) -> dict:
    """
    Look at the positions' markets and count resolved YES/NO vs OPEN,
    plus a little extra context (median resolution age).
    """
    from statistics import median
    yes = no = open_ = 0
    res_ages_days = []

    for mid in positions.keys():
        m = fetch_market_meta_cached(mid)
        if not m:
            continue
        finalized, winner, _src = resolve_status(m)
        if finalized:
            if winner == "YES":
                yes += 1
            elif winner == "NO":
                no += 1
            # resolution age for context
            rdt = resolved_timestamp(m)
            if rdt:
                res_ages_days.append((datetime.now(timezone.utc) - rdt).days)
        else:
            open_ += 1

    total_closed = yes + no
    yes_pct = (yes / total_closed) if total_closed else 0.0
    no_pct  = (no  / total_closed) if total_closed else 0.0

    return {
        "closed_count": total_closed,
        "open_count": open_,
        "yes": yes,
        "no": no,
        "yes_pct": yes_pct,
        "no_pct": no_pct,
        "median_days_since_resolution": (median(res_ages_days) if res_ages_days else None),
    }

def write_yesno_progress_line(start_date: str, variant: str, positions: dict):
    now_iso = datetime.now(timezone.utc).isoformat()
    stats = tally_yes_no(positions)
    rec = {
        "run_ts": now_iso,
        "as_of_ts": now_iso,
        "start_date": start_date,
        "variant": variant,
        **stats
    }
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(YESNO_PROGRESS_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    # quick console summary
    cc = stats["closed_count"]
    if cc:
        print(f"[YES/NO] {variant} | start={start_date} closed={cc} "
              f"YES={stats['yes']} ({stats['yes_pct']:.1%}) "
              f"NO={stats['no']} ({stats['no_pct']:.1%}) "
              f"open={stats['open_count']}")
    else:
        print(f"[YES/NO] {variant} | start={start_date} closed=0 open={stats['open_count']}")

# ----------------------------
# positions + MTM
# ----------------------------
def build_positions_for_start(trades, start_dt_utc, include_crypto: bool):
    pos = {}
    for rec in trades:
        if rec.get("side") != "NO":
            continue
        ts = rec.get("ts")
        if not ts:
            continue
        try:
            t_dt = parse_dt_iso(ts).astimezone(timezone.utc)
        except Exception:
            continue
        if t_dt < start_dt_utc:
            continue
        if not include_crypto and is_crypto_market(rec):
            continue

        mid    = rec.get("market_id")
        shares = float(rec.get("shares", 0.0) or 0.0)
        cost   = float(rec.get("spent_after", 0.0) or 0.0)
        tok    = rec.get("token_id")

        if not (mid and tok) or shares <= 0 or cost < 0:
            continue

        st = pos.setdefault(mid, {
            "question": None,
            "shares": 0.0,
            "cost": 0.0,
            "side": "NO",
            "no_token_id": tok,
            # keep 'price' for backwards-compat (we'll set later to entry_avg)
            "price": 0.0,
        })
        st["question"] = rec.get("question")
        st["shares"]  += shares
        st["cost"]    += cost

    # finalize per-position averages for convenience & compat
    for st in pos.values():
        if st["shares"] > 0:
            entry_avg = st["cost"] / st["shares"]
        else:
            entry_avg = 0.0
        st["entry_avg"] = entry_avg
        st["price"] = entry_avg   # <-- back-compat: if any code reads st["price"], it gets VWAP

    return pos


def sanity_check_resolution_sample(positions: dict, sample_n: int = 50):
    """Quick sample to verify we see some resolved markets when there should be."""
    from random import sample
    mids = list(positions.keys())
    if not mids:
        print("[SANITY] no positions")
        return
    mids = sample(mids, min(sample_n, len(mids)))
    counts = {"resolved_NO": 0, "resolved_YES": 0, "open": 0}
    for mid in mids:
        m = fetch_market_meta_cached(mid)
        if not m:
            continue
        r, w, _ = resolve_status(m)
        if r:
            if w == "NO":
                counts["resolved_NO"] += 1
            elif w == "YES":
                counts["resolved_YES"] += 1
        else:
            counts["open"] += 1
    print(f"[SANITY] sample={len(mids)} → resolved_NO={counts['resolved_NO']} "
          f"resolved_YES={counts['resolved_YES']} open={counts['open']}")


def _normalize_uma_status(val: str | None) -> str:
    if not val:
        return ""
    s = str(val).strip().lower()
    # Common variants you’ll see:
    # "yes", "no", "resolved_yes", "resolved_no", "finalized_yes", "finalized_no"
    if s in {"yes", "resolved_yes", "finalized_yes"}:
        return "YES"
    if s in {"no", "resolved_no", "finalized_no"}:
        return "NO"
    return ""  # unknown/in-flight


def parse_dt_any(v):
    """Best-effort parse: ISO (with/without Z) or epoch seconds/millis → aware UTC datetime."""
    if not v:
        return None
    # numeric epoch?
    try:
        if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().isdigit()):
            ts = float(v)
            if ts > 1e12:  # millis → seconds
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        pass
    # ISO-ish
    try:
        s = str(v).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

HINT_SPREAD = 0.98           # how close to 0/1 we require for a decisive winner
FINAL_GRACE = timedelta(days=2)   # wait this long after close before trusting price-only finals

def resolve_status(m: dict) -> tuple[bool, str | None, str]:
    # 1) explicit winner fields
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

    # 2) price-based finalization ONLY if closed long enough
    if m.get("closed"):
        end_dt = (
            parse_dt_any(m.get("closedTime"))
            or parse_dt_any(m.get("umaEndDate"))
            or parse_dt_any(m.get("endDate"))
            or parse_dt_any(m.get("updatedAt"))
        )
        age_ok = True if end_dt is None else (datetime.now(timezone.utc) - end_dt) >= FINAL_GRACE

        raw = m.get("outcomePrices", ["0","0"])
        prices = json.loads(raw) if isinstance(raw, str) else (raw or ["0","0"])
        try:
            y = float(prices[0]); n = float(prices[1])
        except Exception:
            y = n = None

        if age_ok and y is not None and n is not None:
            # outcomes are ["Yes","No"] on Gamma for yes/no markets
            if y >= HINT_SPREAD and n <= 1 - HINT_SPREAD:
                return True, "YES", "terminal_outcomePrices"
            if n >= HINT_SPREAD and y <= 1 - HINT_SPREAD:
                return True, "NO",  "terminal_outcomePrices"

        # Optional “strong hint” (do NOT finalize, just hint)
        if y is not None and n is not None:
            if y >= 0.90 and n <= 0.10:
                print(f"YES | {m["question"]}")
                return True, "YES", "closed_price_hint_yes"
            if n >= 0.90 and y <= 0.10:
                print(f"NO | {m["question"]}")
                return True, "NO",  "closed_price_hint_no"

    return False, None, "unresolved"


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
        return 0.0, "no_meta"

    is_resolved, winner, note = resolve_status(m)
    if is_resolved:
        if winner == "NO":
            return shares * (1.0 - SETTLE_FEE), "resolved_NO"
        if winner == "YES":
            return 0.0, "resolved_YES"
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

    is_resolved, winner, note = resolve_status(m)
    if is_resolved:
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
        return {
            "status": "resolved_unknown",
            "realized_value": 0.0,
            "unrealized_value": 0.0,
            "realized_pl": -cost,
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
        "no":0,
        "yes":0,
        "closed_cost": 0.0,
        "realized_value": 0.0,
        "realized_pl": 0.0,

        "total_cost": 0.0,
        "mtm_value": 0.0,   # realized_value + open_mtm_value
        "pl_total": 0.0,    # realized_pl + unrealized_pl
    }
    no_count = 0
    yes_count = 0
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
        if status.startswith("resolved"):
            if marks["status"] == "resolved_NO":
                no_count += 1
                closed_no(p)
            else:
                yes_count += 1
                closed_yes(p)

    stats["no"] = no_count
    stats["yes"] = yes_count
    stats["total_cost"] = stats["open_cost"] + stats["closed_cost"]
    stats["mtm_value"]  = stats["realized_value"] + stats["open_mtm_value"]
    stats["pl_total"]   = stats["realized_pl"] + stats["unrealized_pl"]
    return stats


def write_breakdown_line(start_date: str, variant: str, positions: dict, outfile: str):
    as_of = datetime.now(timezone.utc).isoformat()
    stats = breakdown_pl(positions)

    # Base totals
    total_cost = float(stats["total_cost"])
    mtm_value  = float(stats["mtm_value"])
    pl_total   = float(stats["pl_total"])

    # Ratios / returns (guard divide-by-zero)
    return_pct           = (pl_total / total_cost) if total_cost > 0 else 0.0
    mtm_to_cost_pct      = (mtm_value / total_cost - 1.0) if total_cost > 0 else 0.0
    realized_return_pct  = (stats["realized_pl"] / total_cost) if total_cost > 0 else 0.0
    unrealized_return_pct= (stats["unrealized_pl"] / total_cost) if total_cost > 0 else 0.0
    open_exposure_pct    = (stats["open_cost"] / total_cost) if total_cost > 0 else 0.0
    closed_exposure_pct  = (stats["closed_cost"] / total_cost) if total_cost > 0 else 0.0

    # Previous snapshot (for deltas)
    prev = read_last_snapshot_for(outfile, variant, start_date) or {}

    def delta(curr: float, key: str) -> float:
        try:
            return curr - float(prev.get(key, 0.0))
        except Exception:
            return 0.0

    rec = {
        "run_ts": as_of,
        "as_of_ts": as_of,
        "start_date": start_date,
        "variant": variant,

        # counts
        "positions_count": int(stats["positions_count"]),
        "markets_marked":  int(stats["markets_marked"]),
        "open_count":      int(stats["open_count"]),
        "closed_count":    int(stats["closed_count"]),
        "no":              int(stats["no"]),
        "yes":             int(stats["yes"]),

        # dollars
        "open_cost":        float(stats["open_cost"]),
        "open_mtm_value":   float(stats["open_mtm_value"]),
        "unrealized_pl":    float(stats["unrealized_pl"]),
        "closed_cost":      float(stats["closed_cost"]),
        "realized_value":   float(stats["realized_value"]),
        "realized_pl":      float(stats["realized_pl"]),

        # totals / reconciliation
        "total_cost": total_cost,
        "mtm_value":  mtm_value,
        "pl_total":   pl_total,

        # ratios
        "return_pct":            return_pct,            # pl_total / total_cost
        "mtm_to_cost_pct":       mtm_to_cost_pct,       # mtm_value / total_cost - 1
        "realized_return_pct":   realized_return_pct,   # realized_pl / total_cost
        "unrealized_return_pct": unrealized_return_pct, # unrealized_pl / total_cost
        "open_exposure_pct":     open_exposure_pct,     # open_cost / total_cost
        "closed_exposure_pct":   closed_exposure_pct,   # closed_cost / total_cost

        # deltas vs previous line for this (variant,start_date)
        "delta_pl_total":        delta(pl_total,  "pl_total"),
        "delta_unrealized_pl":   delta(stats["unrealized_pl"], "unrealized_pl"),
        "delta_realized_pl":     delta(stats["realized_pl"],   "realized_pl"),
        "delta_mtm_value":       delta(mtm_value, "mtm_value"),
        "delta_open_cost":       delta(stats["open_cost"], "open_cost"),
        "delta_closed_cost":     delta(stats["closed_cost"], "closed_cost"),
        "delta_open_count":      delta(stats["open_count"], "open_count"),
        "delta_closed_count":    delta(stats["closed_count"], "closed_count"),
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
    _debug_check_closed_page(n=250) ####rmrmr
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

        sanity_check_resolution_sample(positions, sample_n=40)

        # NEW: log YES/NO mix for this cohort (cumulative, as of now)
        write_yesno_progress_line(sd.date().isoformat(), variant, positions)

        # write full realized/unrealized breakdown (includes totals & counts)
        write_breakdown_line(sd.date().isoformat(), variant, positions, out_path)

        # optional console line
        print(f"  {variant} | start={sd.date().isoformat()} pos={len(positions)} → wrote breakdown")


def _debug_check_closed_page(n=250):
    # Pull one page of closed markets and tally the UMA winner
    r = SESSION.get(
        BASE_GAMMA,
        params={"limit": n, "offset": 0, "closed": True, "order": "startDate", "ascending": False},
        timeout=20
    )
    r.raise_for_status()
    rows = r.json() or []
    from collections import Counter
    c = Counter()
    for m in rows:
        isr, w, _ = resolve_status(m)
        if isr and w in ("YES", "NO"):
            c[w] += 1
        else:
            c["open_or_unfinalized"] += 1
    print(f"[DEBUG CLOSED PAGE] {dict(c)}")


def test_single_market(mid):
    r = SESSION.get(BASE_GAMMA, params={"ids": mid}, timeout=10)
    r.raise_for_status()
    m = r.json()[0]
    print(m["question"], m.get("umaResolutionStatus"), m.get("umaResolutionStatuses"))
    print(resolve_status(m))


def compute_pl(cost, shares, price, eps=1e-6, ndp=4):
    pnl = price * float(shares) - float(cost)
    if abs(pnl) < eps:
        pnl = 0.0
    return round(pnl, ndp)

def closed_no(market):
    # de-dup
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(CLOSED_NO, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    i = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if i.get("market_id") == market["no_token_id"]:
                    return
    except FileNotFoundError:
        pass

    shares    = float(market.get("shares", 0.0))
    cost      = float(market.get("cost", 0.0))
    entry_avg = float(market.get("entry_avg", cost / shares if shares else 0.0))

    exit_price = 1.0 - SETTLE_FEE              # settlement per share when NO wins
    pl         = compute_pl(cost, shares, exit_price)

    data = {
        "cost": cost,
        "shares": shares,
        "entry_avg": entry_avg,
        "pl": pl,
        "exit_price": exit_price,
        "proceeds": round(exit_price * shares, 6),
        "market_id": market["no_token_id"],
        "market": market.get("question")
    }
    with open(CLOSED_NO, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


def closed_yes(market):
    # de-dup
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(CLOSED_YES, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    i = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if i.get("market_id") == market["no_token_id"]:
                    return
    except FileNotFoundError:
        pass

    shares    = float(market.get("shares", 0.0))
    cost      = float(market.get("cost", 0.0))
    entry_avg = float(market.get("entry_avg", cost / shares if shares else 0.0))

    exit_price = 0.0                           # settlement per share when YES wins
    pl         = compute_pl(cost, shares, exit_price)  # = -cost

    data = {
        "cost": cost,
        "shares": shares,
        "entry_avg": entry_avg,
        "pl": pl,
        "exit_price": exit_price,
        "proceeds": 0.0,
        "market_id": market["no_token_id"],
        "market": market.get("question")
    }
    with open(CLOSED_YES, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


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