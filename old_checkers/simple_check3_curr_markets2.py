import json
import time
import requests
from datetime import datetime, timezone, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import re
import glob

# ======================================================
#  CONFIG: tweak these in ONE place
# ======================================================
BET_SIZE     = 100.0      # dollars per market
CAP_NO       = 0.40      # maker NO cap
FEE_BPS      = 0         # 600 = 6.00%
SLIP_BPS     = 0         # 200 = 2.00%
START_BANK   = 5000.0
BATCH_LIMIT  = 50        # markets per batch
MIN_BANK     = 10.0      # stop if bank below this

BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"

LOGS_DIR             = "logs"
STATUS_REPORT_PATH   = "status_report_saved_markets.txt"
RUN_LOG_PATH         = "look_saved_markets.txt"

# ======================================================
#  GLOBAL STATUS SNAPSHOT (for soft interrupt)
# ======================================================

STATUS = {
    "bank": START_BANK,
    "idx": 0,
    "total_pl": 0.0,
    "total_markets_done": 0,
    "total_spent": 0.0,
    "total_batches": 0,
    "last_batch_pl": 0.0,
    "last_batch_markets": 0,
    "last_market_cid": None,
    "last_market_question": None,
    "last_time_found": None,
    "bet_size": BET_SIZE,
    "cap_no": CAP_NO,
    "fee_bps": FEE_BPS,
    "slip_bps": SLIP_BPS,
    "last_update_ts": None,
}


def update_status(
    *,
    bank=None,
    idx=None,
    total_pl=None,
    total_markets_done=None,
    total_spent=None,
    total_batches=None,
    last_batch_pl=None,
    last_batch_markets=None,
    last_market_cid=None,
    last_market_question=None,
    last_time_found=None,
):
    if bank is not None:
        STATUS["bank"] = bank
    if idx is not None:
        STATUS["idx"] = idx
    if total_pl is not None:
        STATUS["total_pl"] = total_pl
    if total_markets_done is not None:
        STATUS["total_markets_done"] = total_markets_done
    if total_spent is not None:
        STATUS["total_spent"] = total_spent
    if total_batches is not None:
        STATUS["total_batches"] = total_batches
    if last_batch_pl is not None:
        STATUS["last_batch_pl"] = last_batch_pl
    if last_batch_markets is not None:
        STATUS["last_batch_markets"] = last_batch_markets
    if last_market_cid is not None:
        STATUS["last_market_cid"] = last_market_cid
    if last_market_question is not None:
        STATUS["last_market_question"] = last_market_question
    if last_time_found is not None:
        STATUS["last_time_found"] = last_time_found

    STATUS["last_update_ts"] = datetime.now(timezone.utc).isoformat()


def write_status_report(path: str = STATUS_REPORT_PATH):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    s = STATUS.copy()
    lines = [
        "================= STATUS SNAPSHOT (saved markets) =================",
        f"timestamp           : {datetime.now(timezone.utc).isoformat()}",
        "",
        "CONFIG:",
        f"  BET_SIZE          : {s['bet_size']}",
        f"  CAP_NO            : {s['cap_no']}",
        f"  FEE_BPS           : {s['fee_bps']}",
        f"  SLIP_BPS          : {s['slip_bps']}",
        "",
        "ACCOUNT / P&L:",
        f"  bank              : {s['bank']:.2f}",
        f"  total P/L         : {s['total_pl']:.2f}",
        f"  total spent       : {s['total_spent']:.2f}",
        "",
        "MARKET PROGRESS:",
        f"  index             : {s['idx']}",
        f"  total done        : {s['total_markets_done']}",
        f"  total batches     : {s['total_batches']}",
        "",
        "LAST MARKET:",
        f"  last cid          : {s['last_market_cid']}",
        f"  last question     : {s['last_market_question']}",
        f"  last time_found   : {s['last_time_found']}",
        "",
        "LAST BATCH:",
        f"  last batch P/L    : {s['last_batch_pl']:.2f}",
        f"  last batch markets: {s['last_batch_markets']}",
        "===================================================================",
        "",
    ]
    text = "\n".join(lines)
    with open(path, "a", encoding="utf-8") as f:
        f.write(text)
    print(f"[SOFT INTERRUPT] Status snapshot appended to {path}")

# ======================================================
#  SESSION
# ======================================================

def make_session():
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "research-bot/1.0"})
    return s

SESSION = make_session()

def safe_get(url, *, params=None, timeout=(5, 20)):
    return SESSION.get(url, params=params, timeout=timeout)

# ======================================================
#  SimMarket: maker-at-cap NO (same logic as before)
# ======================================================

class SimMarket:
    """
    blocks: list of dicts with keys:
      time, side ("yes"|"no"), price_yes, price_no,
      notional_yes, notional_no, shares
    """
    def __init__(self, blocks, fee_bps=0, slip_bps=20):
        self.blocks = sorted(blocks, key=lambda b: b["time"])
        self.fee  = fee_bps / 10000.0
        self.slip = slip_bps / 10000.0

    def take_first_no(self, t_from, dollars=100.0, max_no_price=None):
        """
        MAKER-at-cap NO:

        - Look at all NO-side blocks with price_no <= cap and time >= t_from
        - Convert those to available SHARES
        - You place a limit NO at cap; you can buy up to dollars/cap shares
        - Cost is shares * cap
        """
        if isinstance(t_from, datetime):
            t_from_ts = int(t_from.replace(tzinfo=timezone.utc).timestamp())
        else:
            t_from_ts = int(t_from)

        cap = 1.0 if max_no_price is None else float(max_no_price)

        eligible = []
        total_shares = 0.0

        for b in self.blocks:
            if int(b.get("time", 0)) < t_from_ts:
                continue
            if b.get("side") != "no":
                continue

            p_no = float(b.get("price_no", 0.0))
            sh   = float(b.get("shares", 0.0))
            if sh <= 0.0:
                continue
            if p_no > cap:
                continue

            eligible.append(b)
            total_shares += sh

        if total_shares <= 0.0:
            return 0.0, 0.0, 0.0, []

        max_shares_by_bank = dollars / cap
        target_shares = min(total_shares, max_shares_by_bank)

        spent_pre   = target_shares * cap
        spent_after = spent_pre * (1.0 + self.fee + self.slip)
        avg_no      = spent_after / target_shares

        fills = []
        remaining = target_shares

        for b in eligible:
            if remaining <= 0:
                break
            block_sh = float(b.get("shares", 0.0))
            if block_sh <= 0:
                continue

            take_sh = min(block_sh, remaining)
            if take_sh <= 0:
                continue

            p_no  = float(b.get("price_no", 0.0))
            p_yes = float(b.get("price_yes", 0.0))

            fills.append({
                "time": b["time"],
                "side": "no",
                "price_no": p_no,
                "price_yes": p_yes,
                "take_shares": take_sh,
                "take_notional_at_cap": take_sh * cap,
                "block": b,
            })

            remaining -= take_sh

        return target_shares, spent_after, avg_no, fills

# ======================================================
#  Read markets from folder: markets_YYYY-MM-DD.jsonl
# ======================================================

def parse_iso(value):
    if not value:
        return None
    s = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def read_unique_markets_from_folder(folder_name: str):
    """
    Reads logs/<folder_name>/markets_*.jsonl
    Lines look like:
      {"time_found": "...", "conditionId": "...", "question": "..."}

    Returns a list of dicts:
      {"conditionId", "question", "time_found", "time_found_epoch"}
    deduped by conditionId, keeping earliest time_found.
    """
    base_dir = os.path.join(LOGS_DIR, folder_name)
    pattern = os.path.join(base_dir, "markets_*.jsonl")
    paths = sorted(glob.glob(pattern))

    uniq = {}
    total_lines = 0
    bad_lines = 0

    if not paths:
        print(f"[read_unique_markets_from_folder] No files found at {pattern}")

    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    total_lines += 1
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        bad_lines += 1
                        continue

                    cid = rec.get("conditionId")
                    q   = rec.get("question")
                    tf  = rec.get("time_found")
                    if not cid or not q or not tf:
                        continue

                    dt = parse_iso(tf)
                    if not dt:
                        continue
                    ts = int(dt.timestamp())

                    old = uniq.get(cid)
                    if (old is None) or (ts < old["time_found_epoch"]):
                        uniq[cid] = {
                            "conditionId": cid,
                            "question": q,
                            "time_found": tf,
                            "time_found_epoch": ts,
                        }
        except FileNotFoundError:
            continue

    markets = list(uniq.values())
    markets.sort(key=lambda x: x["time_found_epoch"])

    print(f"[READ] files={len(paths)} lines={total_lines} unique_markets={len(markets)} bad_lines={bad_lines}")
    return markets

# ======================================================
#  Market meta + trades
# ======================================================

MARKET_META_CACHE = {}

def fetch_market_meta(cid: str):
    if cid in MARKET_META_CACHE:
        return MARKET_META_CACHE[cid]
    try:
        r = safe_get(
            BASE_GAMMA,
            params={"condition_ids": cid, "limit": 1},
            timeout=(5, 20),
        )
        r.raise_for_status()
        rows = r.json() or []
        m = {}
        for row in rows:
            if row.get("conditionId") == cid:
                m = row
                break
        MARKET_META_CACHE[cid] = m
        return m
    except Exception:
        MARKET_META_CACHE[cid] = {}
        return {}


def fetch_trades(market_dict):
    """Simple full trade history (same as old script, 100 limit, no pagination)."""
    cid = market_dict["conditionId"]
    try:
        resp = safe_get(
            DATA_TRADES,
            params={"market": cid, "sort": "asc", "limit": 100},
            timeout=(5, 20),
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload
    except Exception:
        return None

# ======================================================
#  Robust market resolution (copied from Script B style)
# ======================================================

HINT_SPREAD = 0.98
FINAL_GRACE = timedelta(days=2)


def _parse_dt_any(v):
    if not v:
        return None
    try:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(float(v), tz=timezone.utc)
        if isinstance(v, str) and v.strip().isdigit():
            return datetime.fromtimestamp(float(v), tz=timezone.utc)
    except Exception:
        pass

    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def resolve_status(m: dict):
    """
    Return (resolved_bool, winner_str_or_None, source_tag)
    winner_str is "YES" or "NO" if resolved_bool is True.
    """
    # 1) UMA resolution
    uma = (m.get("umaResolutionStatus") or "").lower()
    if uma in ("yes", "no"):
        return True, uma.upper(), "umaResolutionStatus"
    if uma.startswith("resolved_"):
        w = uma.split("_", 1)[1].upper()
        if w in ("YES", "NO"):
            return True, w, "umaResolutionStatus"

    # 2) Direct winningOutcome / winner
    w = (m.get("winningOutcome") or m.get("winner") or "").upper()
    if w in ("YES", "NO"):
        return True, w, "winningOutcome"

    # 3) Heuristic from prices for closed markets
    if m.get("closed"):
        end_dt = (
            _parse_dt_any(m.get("closedTime"))
            or _parse_dt_any(m.get("umaEndDate"))
            or _parse_dt_any(m.get("endDate"))
            or _parse_dt_any(m.get("updatedAt"))
        )
        age_ok = True if end_dt is None else (datetime.now(timezone.utc) - end_dt) >= FINAL_GRACE

        raw = m.get("outcomePrices", ["0", "0"])
        prices = json.loads(raw) if isinstance(raw, str) else raw
        try:
            y, n = float(prices[0]), float(prices[1])
        except Exception:
            y, n = None, None

        if age_ok and y is not None and n is not None:
            if y >= HINT_SPREAD and n <= 1 - HINT_SPREAD:
                return True, "YES", "terminal_prices"
            if n >= HINT_SPREAD and y <= 1 - HINT_SPREAD:
                return True, "NO", "terminal_prices"

        if y is not None and n is not None:
            if y >= 0.90 and n <= 0.10:
                return True, "YES", "closed_hint_yes"
            if n >= 0.90 and y <= 0.10:
                return True, "NO", "closed_hint_no"

    return False, None, "unresolved"


def current_status(m: dict) -> str:
    resolved, winner, _ = resolve_status(m)
    if resolved and winner in ("YES", "NO"):
        return winner
    return "TBD"

# ======================================================
#  Time + trade normalization (SIMPLE, no aggregation)
# ======================================================

def normalize_time(value, default=None):
    if value is None or value == "":
        return default

    if isinstance(value, (int, float)) or re.match(r"^\d{10,13}$", str(value)):
        try:
            ts = float(value)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return default

    val = str(value).strip()
    val = val.replace("Z", "+00:00")
    val = re.sub(r"\s+", "T", val)

    if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
        val += "T00:00:00+00:00"

    try:
        dt = datetime.fromisoformat(val)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return default


def clamp01(x, eps=1e-6):
    return min(1.0 - eps, max(eps, float(x)))


def snap_price(p, tick=0.01):
    return round(round(float(p) / tick) * tick, 2)


def take_yes(trade):
    out = trade["outcome"].lower()
    side = trade["side"].lower()
    return (out == "no" and side == "sell") or (out == "yes" and side == "buy")


def take_no(trade):
    out = trade["outcome"].lower()
    side = trade["side"].lower()
    return (out == "yes" and side == "sell") or (out == "no" and side == "buy")


def notion_yes(trade):
    return float(trade["size"]) * float(trade["price"])


def notion_no(trade):
    return float(trade["size"]) * (1 - float(trade["price"]))


def valid_trade(trade, min_spend=2, extreme_price=0.05, min_extreme_notional=20.0):
    if not trade:
        return False

    price = float(trade["price"])
    size = float(trade["size"])
    outcome = str(trade.get("outcome", "")).strip().lower()

    if outcome == "yes":
        cost = price * size
    elif outcome == "no":
        cost = (1 - price) * size
    else:
        return False

    if cost < min_spend:
        return False

    if price < extreme_price or price > 1.0 - extreme_price:
        return cost >= min_extreme_notional

    return True


def normalize_trades(trades):
    if not trades:
        print("ERROR NO TRADES AVAILABLE")
        return []

    trades = sorted(trades, key=lambda t: t["timestamp"])

    blocks = []
    for tr in trades:
        if not valid_trade(tr):
            continue

        try:
            ts = int(float(tr["timestamp"]))
        except Exception:
            continue

        p_yes_raw = snap_price(tr["price"], 0.01)
        p_no_raw  = round(1.0 - p_yes_raw, 6)

        if take_yes(tr):
            side = "yes"
        elif take_no(tr):
            side = "no"
        else:
            continue

        size = float(tr["size"])
        if size <= 0:
            continue

        if side == "yes":
            notional_yes = size * p_yes_raw
            notional_no  = 0.0
        else:
            notional_no  = size * p_no_raw
            notional_yes = 0.0

        blocks.append({
            "time": ts,
            "side": side,
            "price_yes": round(p_yes_raw, 6),
            "price_no":  round(p_no_raw, 6),
            "shares": size,
            "notional_yes": round(notional_yes, 6),
            "notional_no":  round(notional_no, 6),
            "compressed": 1,
        })

    return sorted(blocks, key=lambda b: b["time"])

# ======================================================
#  Misc helpers
# ======================================================

def write_to_file(filename, data):
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
    with open(filename, "a", encoding="utf-8") as file:
        file.seek(0, os.SEEK_END)
        if file.tell() > 0:
            file.write("\n")
        file.write(data + "\n")

# ======================================================
#  MAIN SIM LOGIC (from saved markets)
# ======================================================

def run_from_saved_markets():
    folder = input("Folder name under logs/ (e.g. curr): ").strip()
    if not folder:
        print("No folder given, exiting.")
        return

    markets = read_unique_markets_from_folder(folder)
    if not markets:
        print("No markets found in that folder, exiting.")
        return

    total_markets = len(markets)
    print(f"Loaded {total_markets} unique markets from logs/{folder}")

    bank = START_BANK
    idx = 0
    all_pl = 0.0
    all_done = 0          # total markets *visited*
    spent = 0.0
    batch_idx = 0

    update_status(
        bank=bank,
        idx=idx,
        total_pl=all_pl,
        total_markets_done=all_done,
        total_spent=spent,
        total_batches=batch_idx,
    )

    print(f"START: bank={bank:.2f}, BET_SIZE={BET_SIZE}, CAP_NO={CAP_NO}")
    print(f"Total markets to process: {total_markets}")

    while idx < total_markets:
        if bank < MIN_BANK:
            print("Bank below MIN_BANK, stopping.")
            break

        batch_idx += 1
        pnl_batch = 0.0
        visited_this_batch = 0   # how many markets we *looked at*
        bets_this_batch = 0      # how many markets we actually *bet* in
        last_time_found = None

        try:
            for _ in range(BATCH_LIMIT):
                if idx >= total_markets:
                    break

                m_meta = markets[idx]
                cid = m_meta["conditionId"]
                q   = m_meta["question"]
                tf  = m_meta["time_found"]
                tf_epoch = m_meta["time_found_epoch"]

                print(f"\n[#{idx+1}/{total_markets}] {q} (cid={cid[:10]}…) time_found={tf}")

                market_obj = {
                    "conditionId": cid,
                    "question": q,
                }

                visited_this_batch += 1
                all_done += 1
                last_time_found = tf

                trades_raw = fetch_trades(market_obj)
                trades = normalize_trades(trades_raw)
                if not trades:
                    idx += 1
                    continue

                # Maker-at-cap: start from first trade
                t_from = trades[0]["time"]

                if bank >= BET_SIZE:
                    bet = BET_SIZE
                elif bank >= MIN_BANK:
                    bet = float(bank)
                else:
                    print("Out of money / below min bank, breaking batch.")
                    break

                sim = SimMarket(trades, fee_bps=FEE_BPS, slip_bps=SLIP_BPS)
                shares, spent_after, avg_, fills = sim.take_first_no(
                    t_from, dollars=bet, max_no_price=CAP_NO
                )

                # no valid fill under cap
                if shares == 0.0 or spent_after == 0.0:
                    idx += 1
                    continue

                # Get full meta and robust status
                full_meta = fetch_market_meta(cid)
                status = current_status(full_meta)

                if status not in ("YES", "NO"):
                    print(f"    [SKIP] unresolved/TBD status={status} → no P/L counted for this market.")
                    idx += 1
                    continue

                # We always simulate BUY NO; profit depends on whether NO actually won
                if status == "NO":
                    won = True
                else:  # status == "YES"
                    won = False

                pnl = (shares - spent_after) if won else (-spent_after)

                bank += pnl
                pnl_batch += pnl
                spent += spent_after
                bets_this_batch += 1

                running_pl_global = all_pl + pnl_batch

                print(q)
                print(
                    f"shares={shares:.2f} | spent={spent_after:.2f} | cap={CAP_NO:.3f} "
                    f"| avg={avg_:.4f} | status={status} ({'WON' if won else 'LOST'}) "
                    f"| pnl={pnl:.2f} | running_PL={running_pl_global:.2f} | bank={bank:.2f}"
                )

                update_status(
                    bank=bank,
                    idx=idx,
                    total_pl=running_pl_global,
                    total_markets_done=all_done,
                    total_spent=spent,
                    total_batches=batch_idx,
                    last_batch_pl=pnl_batch,
                    last_batch_markets=bets_this_batch,
                    last_market_cid=cid,
                    last_market_question=q,
                    last_time_found=tf,
                )

                idx += 1
                time.sleep(0.2)

        except KeyboardInterrupt:
            print("\n[SOFT INTERRUPT] Ctrl+C detected → writing status report (continuing afterwards)…")
            write_status_report()
            continue  # go back to while loop, same idx/bank/etc

        all_pl += pnl_batch

        print("-" * 61)
        print(
            f"batch #{batch_idx} | visited_this_batch: {visited_this_batch} "
            f"| bets_this_batch: {bets_this_batch} "
            f"| total_markets_done: {all_done} | batch P/L: {pnl_batch:.2f} "
            f"| total P/L: {all_pl:.2f} | bank: {bank:.2f}"
        )
        print("-" * 61)

        write_to_file(
            RUN_LOG_PATH,
            f"batch:{batch_idx} | visited_total:{all_done} | bets_total:{bets_this_batch} "
            f"| total spent {spent:.2f} | batch P/L: {pnl_batch:.2f} | total P/L: {all_pl:.2f} "
            f"| bank: {bank:.2f} | last_time_found: {last_time_found}"
        )

    print("\n=== DONE processing saved markets ===")
    print(f"Final bank: {bank:.2f}, total P/L: {all_pl:.2f}, markets_done: {all_done}")


def main():
    run_from_saved_markets()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[HARD INTERRUPT] Ctrl+C at top-level → writing final status and exiting.")
        write_status_report()
