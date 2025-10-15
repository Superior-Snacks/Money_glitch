#!/usr/bin/env python3
import os, re, json, gzip, time, math
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Iterable, Tuple, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------------------
# CONFIG
# ----------------------------
LOG_DIR = "logs_run_1760549852"
TRADE_LOG_GLOB = os.path.join(LOG_DIR, "trades_taken_*.jsonl*")  # both .jsonl and .jsonl.gz

OUTPUT_ALL       = os.path.join(LOG_DIR, "projection_all.jsonl")
OUTPUT_NOCRYPTO  = os.path.join(LOG_DIR, "projection_no_crypto.jsonl")

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"

# Fees consistent with your main bot (used only to compute NO payout at resolution)
SETTLE_FEE = 0.01

# crude crypto detector (extend as you like)
CRYPTO_WORDS = {
    "crypto","coin","token","blockchain","defi","stablecoin",
    "bitcoin","btc","ethereum","eth","solana","sol","xrp","ripple","doge","dogecoin",
    "ada","cardano","bnb","ton","shib","litecoin","ltc","avalanche","avax","tron","trx",
    "chainlink","link","polkadot","dot","near","aptos","apt","arbitrum","arb","base",
    "matic","polygon","pepe","sui","kaspa","kas","sei",
}

# ------------- HTTP session (robust, gentle) -----------------
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
    s.headers.update({"User-Agent": "pl-scenario-projection/1.0"})
    return s

SESSION = make_session()

# ----------------------------
# helpers
# ----------------------------
def parse_dt_iso(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        # last resort: very permissive (your logs are ISO already)
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)

def iter_trade_files() -> List[str]:
    import glob
    return sorted(glob.glob(TRADE_LOG_GLOB))

def open_maybe_gz(path: str, mode: str = "rt"):
    if path.endswith(".gz"):
        return gzip.open(path, mode, encoding="utf-8", errors="replace")
    return open(path, mode, encoding="utf-8", errors="replace")

def iter_trade_records() -> Iterable[dict]:
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
                        # skip a bad line rather than crash
                        continue
        except FileNotFoundError:
            continue

def earliest_trade_date_utc(trades: List[dict]) -> Optional[datetime]:
    dts = []
    for r in trades:
        ts = r.get("ts")
        if not ts: 
            continue
        try:
            dts.append(parse_dt_iso(ts).astimezone(timezone.utc))
        except Exception:
            pass
    return min(dts) if dts else None

def is_crypto_text(text: str) -> bool:
    t = (text or "").lower()
    return any(w in t for w in CRYPTO_WORDS)

def is_crypto_trade(rec: dict) -> bool:
    q = rec.get("question") or ""
    s = rec.get("market_slug") or ""
    return is_crypto_text(q) or is_crypto_text(s)

# ----------------------------
# position builder (NO-only, like your bot)
# ----------------------------
def build_positions_for_start(trades: List[dict], start_dt_utc: datetime, include_crypto: bool) -> Dict[str, dict]:
    """
    Returns: market_id -> {"shares": float, "cost": float, "side":"NO"}
    Only includes trades at or after start_dt_utc.
    """
    pos: Dict[str, dict] = {}
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
        if not include_crypto and is_crypto_trade(rec):
            continue

        mid = rec.get("market_id")
        shares = float(rec.get("shares", 0.0) or 0.0)
        cost   = float(rec.get("spent_after", 0.0) or 0.0)
        if not mid or shares <= 0 or cost < 0:
            continue
        st = pos.setdefault(mid, {"shares": 0.0, "cost": 0.0, "side": "NO"})
        st["shares"] += shares
        st["cost"]   += cost
    return pos

# ----------------------------
# resolution metadata (Gamma)
# ----------------------------
def fetch_markets_meta_bulk(condition_ids: List[str]) -> Dict[str, dict]:
    """
    Batches lookups by condition_ids. Returns {condition_id: market_meta}
    """
    out: Dict[str, dict] = {}
    CHUNK = 50  # be gentle; Gamma handles batching fine
    for i in range(0, len(condition_ids), CHUNK):
        chunk = condition_ids[i:i+CHUNK]
        try:
            r = SESSION.get(BASE_GAMMA, params={"condition_ids": ",".join(chunk), "limit": len(chunk)}, timeout=15)
            r.raise_for_status()
            rows = r.json() or []
            for m in rows:
                cid = m.get("conditionId")
                if cid:
                    out[cid] = m
        except Exception:
            # if one chunk fails, skip it (projection continues)
            continue
        time.sleep(0.05)  # tiny pause
    return out

def market_resolution_status(m: dict) -> Tuple[bool, Optional[str]]:
    """
    Returns (resolved, winner) where winner is "YES"|"NO"|None
    """
    if not m:
        return False, None
    resolved = bool(m.get("resolved") or m.get("isResolved"))
    winner = (m.get("winningOutcome") or m.get("winner") or "")
    winner = winner.strip().upper() if isinstance(winner, str) else None
    return resolved, winner

# ----------------------------
# projection logic
# ----------------------------
def project_pl_for_positions(positions: Dict[str, dict], metas: Dict[str, dict], win_rates: List[float]) -> dict:
    """
    Compute realized P/L from resolved markets and projection for unresolved
    under a set of win rates (applied to unresolved NOs).
    Returns a dict with:
      - counts & dollars
      - break_even_win_rate
      - projections: [{win_rate, projected_pl, unresolved_ev, realized_pl} ...]
    """
    # accumulators
    closed_cost = 0.0
    realized_value = 0.0
    realized_pl = 0.0

    open_cost = 0.0
    open_total_payout_if_win = 0.0  # sum(shares * (1 - fee)) across unresolved

    closed_count = 0
    open_count = 0

    for mid, p in positions.items():
        shares = float(p.get("shares", 0.0))
        cost   = float(p.get("cost", 0.0))
        if shares <= 0 or cost < 0:
            continue

        meta = metas.get(mid)
        resolved, winner = market_resolution_status(meta)

        if resolved:
            closed_count += 1
            closed_cost += cost
            if winner == "NO":
                cash = shares * (1.0 - SETTLE_FEE)
                realized_value += cash
                realized_pl += (cash - cost)
            elif winner == "YES":
                realized_value += 0.0
                realized_pl += (0.0 - cost)
            else:
                # unknown winner → conservative
                realized_value += 0.0
                realized_pl += (0.0 - cost)
        else:
            open_count += 1
            open_cost += cost
            open_total_payout_if_win += shares * (1.0 - SETTLE_FEE)

    # break-even win rate for unresolved only:
    # find w such that: realized_pl + (w * open_total_payout_if_win - open_cost) = 0
    be_den = open_total_payout_if_win
    if be_den <= 1e-12:
        w_break_even = None  # no unresolved exposure
    else:
        w_break_even = max(0.0, min(1.0, open_cost / be_den))

    # grid projections
    projections = []
    for w in win_rates:
        w = max(0.0, min(1.0, float(w)))
        unresolved_ev = w * open_total_payout_if_win - open_cost
        projected_pl = realized_pl + unresolved_ev
        projections.append({
            "win_rate": w,
            "unresolved_ev": round(float(unresolved_ev), 6),
            "realized_pl": round(float(realized_pl), 6),
            "projected_pl": round(float(projected_pl), 6),
        })

    return {
        "open_count": open_count,
        "closed_count": closed_count,
        "open_cost": round(float(open_cost), 6),
        "closed_cost": round(float(closed_cost), 6),
        "realized_value": round(float(realized_value), 6),
        "realized_pl": round(float(realized_pl), 6),
        "open_total_payout_if_win": round(float(open_total_payout_if_win), 6),
        "break_even_win_rate": w_break_even if w_break_even is None else round(float(w_break_even), 6),
        "projections": projections,
    }

# ----------------------------
# IO helpers
# ----------------------------
def append_jsonl(path: str, rec: dict):
    if not path:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

def parse_win_grid(arg: Optional[str]) -> List[float]:
    if not arg:
        # default: 40%..80% step 5%
        return [round(x, 2) for x in [0.40,0.45,0.50,0.55,0.60,0.65,0.70,0.75,0.80]]
    try:
        vals = [float(x.strip()) for x in arg.split(",") if x.strip()]
        vals = [v for v in vals if 0.0 <= v <= 1.0]
        return sorted(set(round(v,4) for v in vals))
    except Exception:
        return [0.40,0.50,0.60,0.70,0.80]

# ----------------------------
# main
# ----------------------------
def run_once(include_crypto: bool, win_grid: List[float], out_path: Optional[str], no_write: bool):
    trades = list(iter_trade_records())
    now_dt = datetime.now(timezone.utc)
    if not trades:
        print(f"[{now_dt.isoformat()}] no trades found.")
        return

    earliest = earliest_trade_date_utc(trades)
    if not earliest:
        print(f"[{now_dt.isoformat()}] could not parse earliest trade date.")
        return

    # build all daily start dates
    total_days = (now_dt.date() - earliest.date()).days
    start_dates = [datetime.combine(earliest.date() + timedelta(days=i), 
                                    datetime.min.time(), tzinfo=timezone.utc)
                   for i in range(total_days + 1)]

    variant = "all" if include_crypto else "no_crypto"
    if out_path is None:
        out_path = OUTPUT_ALL if include_crypto else OUTPUT_NOCRYPTO

    print(f"[RUN] {now_dt.isoformat()} variant={variant} days={len(start_dates)} grid={win_grid}")

    # collect union of market_ids across all starts once (to batch metadata)
    all_mids = set()
    for sd in start_dates:
        pos = build_positions_for_start(trades, sd, include_crypto)
        all_mids.update(pos.keys())

    metas = fetch_markets_meta_bulk(list(all_mids))

    # process each start date
    for sd in start_dates:
        pos = build_positions_for_start(trades, sd, include_crypto)
        proj = project_pl_for_positions(pos, metas, win_grid)

        rec = {
            "run_ts": now_dt.isoformat(),
            "as_of_ts": now_dt.isoformat(),
            "start_date": sd.date().isoformat(),
            "variant": variant,

            "positions_count": len(pos),
            "open_count": proj["open_count"],
            "closed_count": proj["closed_count"],

            "open_cost": proj["open_cost"],
            "closed_cost": proj["closed_cost"],
            "realized_value": proj["realized_value"],
            "realized_pl": proj["realized_pl"],

            "open_total_payout_if_win": proj["open_total_payout_if_win"],
            "break_even_win_rate": proj["break_even_win_rate"],
            "projections": proj["projections"],
        }

        # print compact console line
        be = rec["break_even_win_rate"]
        be_s = "n/a" if be is None else f"{be:.2%}"
        print(
            f"{rec['start_date']} | pos={rec['positions_count']} "
            f"open_cost={rec['open_cost']:.2f} closed_pl={rec['realized_pl']:.2f} "
            f"open_if_all_win={rec['open_total_payout_if_win']:.2f} "
            f"BE_win_rate={be_s}"
        )

        if not no_write:
            append_jsonl(out_path, rec)

    if not no_write:
        print(f"[DONE] wrote to {out_path}")

def main():
    import argparse
    p = argparse.ArgumentParser(description="NO-portfolio scenario projection by start date (runs once)")
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--include-crypto", action="store_true", help="Include crypto markets (default)")
    g.add_argument("--exclude-crypto", action="store_true", help="Exclude crypto markets")

    p.add_argument("--win-grid", type=str, default=None,
                   help="Comma list of win rates (0..1). Example: 0.35,0.45,0.55,0.65,0.75")
    p.add_argument("--out", type=str, default=None, help="Output JSONL path (defaults to logs/projection_*.jsonl)")
    p.add_argument("--no-write", action="store_true", help="Print only; do not write output file")

    args = p.parse_args()
    include_crypto = not args.exclude_crypto
    win_grid = parse_win_grid(args.win_grid)
    run_once(include_crypto, win_grid, args.out, args.no_write)

if __name__ == "__main__":
    main()