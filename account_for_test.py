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

LOG_DIR = "logs"
TRADE_LOG_GLOB = os.path.join(LOG_DIR, "trades_taken_*.jsonl*")

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_BOOK  = "https://clob.polymarket.com/book"

FEE_BPS   = 600      # same as live script
SLIP_BPS  = 200
SETTLE_FEE = 0.01    # winner fee (same assumption as your main script)

RPS_TARGET = 3.0     # keep gentle
SLEEP_BETWEEN_MARKETS = 0.05
LOOP_SECONDS = 3600  # 1 hour when run with --loop

# -------------------------------------------------------------------
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
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=30, pool_maxsize=30)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pl-watcher/1.0"})
    return s

SESSION = make_session()

# ---- Helpers from your live code (robust to strings/lists) ----------
def outcome_map_from_market(market: dict) -> dict:
    outs = market.get("outcomes")
    toks = market.get("clobTokenIds")
    if isinstance(outs, str):
        try: outs = json.loads(outs)
        except: outs = None
    if isinstance(toks, str):
        try: toks = json.loads(toks)
        except: toks = None
    if isinstance(outs, list) and isinstance(toks, list) and len(outs) == 2 and len(toks) == 2:
        o0, o1 = str(outs[0]).strip().upper(), str(outs[1]).strip().upper()
        if (o0, o1) == ("YES", "NO"):
            return {"YES": toks[0], "NO": toks[1]}
        if (o0, o1) == ("NO", "YES"):
            return {"YES": toks[1], "NO": toks[0]}
        if "NO" in o0 and "YES" in o1:
            return {"YES": toks[1], "NO": toks[0]}
        if "YES" in o0 and "NO" in o1:
            return {"YES": toks[0], "NO": toks[1]}
    if isinstance(toks, list) and len(toks) == 2:
        return {"YES": toks[0], "NO": toks[1]}
    raise ValueError(f"Cannot map YES/NO for market {market.get('id') or market.get('conditionId')}")

def get_yes_no_token_ids(market: dict):
    om = outcome_map_from_market(market)
    return om["YES"], om["NO"]

# ---- Log reading ----------------------------------------------------
def iter_trade_records():
    paths = sorted(glob.glob(TRADE_LOG_GLOB))
    for path in paths:
        opener = gzip.open if path.endswith(".gz") else open
        try:
            with opener(path, "rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    yield rec
        except FileNotFoundError:
            continue

def load_positions_from_logs():
    """
    Returns dict market_id -> {"shares": float, "cost": float, "side": "NO"}
    Ignores YES (you’re only opening NO in your current strategy).
    """
    pos = {}
    for rec in iter_trade_records():
        if rec.get("side") != "NO":
            continue
        mid = rec.get("market_id")
        shares = float(rec.get("shares", 0.0))
        cost   = float(rec.get("spent_after", 0.0))
        if not mid or shares <= 0 or cost < 0:
            continue
        p = pos.setdefault(mid, {"shares": 0.0, "cost": 0.0, "side": "NO"})
        p["shares"] += shares
        p["cost"]   += cost
    return pos

# ---- Market & book fetching ----------------------------------------
def fetch_markets_by_ids(ids):
    """
    Try to fetch Gamma rows by IDs in chunks. This endpoint commonly supports a CSV in 'ids'.
    Falls back per-id if needed.
    """
    rows = {}
    chunk = 50
    id_list = list(ids)
    for i in range(0, len(id_list), chunk):
        slab = id_list[i:i+chunk]
        _rate_limit()
        r = SESSION.get(BASE_GAMMA, params={"ids": ",".join(slab)}, timeout=20)
        if r.status_code == 200:
            arr = r.json() or []
            for m in arr:
                mid = m.get("conditionId") or m.get("id")
                if mid:
                    rows[mid] = m
        else:
            # fallback one by one
            for mid in slab:
                _rate_limit()
                r2 = SESSION.get(BASE_GAMMA, params={"ids": mid}, timeout=20)
                if r2.status_code == 200:
                    arr2 = r2.json() or []
                    for m in arr2:
                        key = m.get("conditionId") or m.get("id")
                        if key:
                            rows[key] = m
        time.sleep(0.02)
    return rows

def fetch_book(token_id, depth=10):
    _rate_limit()
    r = SESSION.get(BASE_BOOK, params={"token_id": token_id}, timeout=15)
    if r.status_code != 200:
        return {}
    try:
        b = r.json() or {}
    except Exception:
        return {}
    # normalize/truncate
    b["bids"] = (b.get("bids") or [])[:depth]
    b["asks"] = (b.get("asks") or [])[:depth]
    return b

def best_bid(book):
    bids = book.get("bids") or []
    bb = None
    for b in bids:
        try:
            px = float(b.get("price"))
            if bb is None or px > bb:
                bb = px
        except Exception:
            pass
    return bb

# ---- Resolution detection ------------------------------------------
def market_resolution_info(m):
    """
    Returns (is_resolved, winning_str) with winning_str in {"YES","NO",None}.
    Tries several likely fields since Gamma schemas vary.
    """
    # typical hints
    resolved = bool(m.get("resolved") or m.get("isResolved") or m.get("closed"))
    winning = m.get("winningOutcome") or m.get("winning_outcome") or m.get("resolution")
    # Normalize strings
    win_norm = None
    if isinstance(winning, str):
        w = winning.strip().upper()
        if "YES" in w:
            win_norm = "YES"
        elif "NO" in w:
            win_norm = "NO"
        else:
            # sometimes resolution text is like "Resolved: No" or "Cancel"
            if "RESOLV" in w and "NO" in w:
                win_norm = "NO"
            elif "RESOLV" in w and "YES" in w:
                win_norm = "YES"
    # Another pattern: list of winning outcomes or booleans
    if win_norm is None:
        wo = m.get("winningOutcomes") or m.get("winning_outcomes")
        if isinstance(wo, list) and len(wo) == 1:
            s = str(wo[0]).strip().upper()
            if s in ("YES","NO"):
                win_norm = s
    return resolved, win_norm

# ---- P&L calc ------------------------------------------------------
def compute_pl():
    positions = load_positions_from_logs()
    if not positions:
        return {
            "asof": datetime.now(timezone.utc).isoformat(),
            "positions": 0,
            "realized_pl": 0.0,
            "unrealized_pl": 0.0,
            "total_cost": 0.0,
            "mtm_value": 0.0,
            "closed_count": 0,
            "open_count": 0,
            "by_market": {}
        }

    market_ids = list(positions.keys())
    gamma = fetch_markets_by_ids(market_ids)

    fee_mult = 1.0 + (FEE_BPS/10000.0) + (SLIP_BPS/10000.0)

    realized_pl = 0.0
    unrealized_pl = 0.0
    total_cost = 0.0
    mtm_value = 0.0
    closed_count = 0
    open_count = 0

    by_market = {}

    for mid, pos in positions.items():
        cost   = float(pos["cost"])
        shares = float(pos["shares"])
        total_cost += cost

        mrow = gamma.get(mid) or {}
        resolved, winning = market_resolution_info(mrow)

        try:
            yes_tok, no_tok = get_yes_no_token_ids(mrow)
        except Exception:
            yes_tok = no_tok = None

        status = "open"
        pnl = 0.0
        mval = 0.0
        close_proceeds = None

        if resolved and winning in ("YES","NO"):
            status = "closed"
            closed_count += 1
            if winning == "NO":
                close_proceeds = shares * (1.0 - SETTLE_FEE)
            else:
                close_proceeds = 0.0
            pnl = (close_proceeds - cost)
            realized_pl += pnl
            mval = close_proceeds
        else:
            open_count += 1
            # mark NO at best bid (conservative)
            bb = None
            if no_tok:
                time.sleep(SLEEP_BETWEEN_MARKETS)
                book = fetch_book(no_tok, depth=10)
                bb = best_bid(book)
            if bb is None:
                bb = 0.0
            # (optional) include exit fees/slip here; many people don't for MTM.
            mval = shares * bb
            pnl = mval - cost
            unrealized_pl += pnl

        mtm_value += mval

        by_market[mid] = {
            "question": mrow.get("question"),
            "status": status,
            "winning": winning,
            "shares_no": shares,
            "cost": round(cost, 6),
            "mtm_value": round(mval, 6),
            "pnl": round(pnl, 6),
        }

    snap = {
        "asof": datetime.now(timezone.utc).isoformat(),
        "positions": len(positions),
        "realized_pl": round(realized_pl, 6),
        "unrealized_pl": round(unrealized_pl, 6),
        "total_cost": round(total_cost, 6),
        "mtm_value": round(mtm_value, 6),
        "closed_count": closed_count,
        "open_count": open_count,
        "by_market": by_market,
    }
    return snap

def print_summary(snap):
    print(f"\n=== P/L Snapshot @ {snap['asof']} ===")
    print(f"Positions: {snap['positions']} | Open: {snap['open_count']} | Closed: {snap['closed_count']}")
    print(f"Total cost:       {snap['total_cost']:.2f}")
    print(f"MTM value:        {snap['mtm_value']:.2f}")
    print(f"Realized P/L:     {snap['realized_pl']:.2f}")
    print(f"Unrealized P/L:   {snap['unrealized_pl']:.2f}")
    print(f"Total P/L:        {(snap['realized_pl']+snap['unrealized_pl']):.2f}")
    # Show a few lines
    shown = 0
    for mid, row in list(snap["by_market"].items())[:8]:
        print(f" - {row['status'].upper():6s} | {row['pnl']:>10.2f} | cost {row['cost']:>9.2f} | mtm {row['mtm_value']:>9.2f} | {row.get('question') or mid}")
        shown += 1
    if snap["positions"] > shown:
        print(f"   ... (+{snap['positions']-shown} more)")

def save_snapshot(snap, path=os.path.join(LOG_DIR, "pl_snapshot.json")):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(snap, f, ensure_ascii=False, indent=2)
        print(f"[PL] wrote {path}")
    except Exception as e:
        print(f"[PL WARN] failed to write snapshot: {e}")

def main(loop=False):
    while True:
        try:
            snap = compute_pl()
            print_summary(snap)
            save_snapshot(snap)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"[PL WARN] {e}")
        if not loop:
            break
        time.sleep(LOOP_SECONDS)

if __name__ == "__main__":
    import sys
    loop = ("--loop" in sys.argv)
    main(loop=loop)