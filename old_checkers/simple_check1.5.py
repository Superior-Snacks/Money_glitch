import json
import time
import requests
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import re

# ======================================================
#  CONFIG: tweak these in ONE place
# ======================================================
BET_SIZE     = 10.0      # dollars per market
CAP_NO       = 0.40      # maker NO cap
FEE_BPS      = 0         # 600 = 6.00%
SLIP_BPS     = 0         # 200 = 2.00%
START_BANK   = 5000.0
START_OFFSET = 70000     # Gamma offset to start from
BATCH_LIMIT  = 50        # markets per batch
MAX_BATCHES  = 100       # max batches to run
MIN_BANK     = 10.0      # stop if bank below this

BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_BOOK    = "https://clob.polymarket.com/book"
DATA_TRADES  = "https://data-api.polymarket.com/trades"

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

# ======================================================
#  SimMarket: maker-at-cap NO
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
        Interpret as MAKER-at-cap:

        - Look at all NO-side blocks with price_no <= cap and time >= t_from
        - Convert those to available SHARES
        - You place a limit NO at cap; you can buy up to dollars/cap shares
        - Cost is shares * cap (not historical trade price)
        """
        # normalize t_from to epoch seconds
        if isinstance(t_from, datetime):
            t_from_ts = int(t_from.replace(tzinfo=timezone.utc).timestamp())
        else:
            t_from_ts = int(t_from)

        # Treat max_no_price as our cap. If None, assume 1.0 (effectively uncapped).
        cap = 1.0 if max_no_price is None else float(max_no_price)

        # Gather eligible blocks (NO trades at or below cap, after t_from)
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
            # No liquidity under cap
            return 0.0, 0.0, 0.0, []

        # How many shares can our bankroll buy at the cap?
        max_shares_by_bank = dollars / cap
        target_shares = min(total_shares, max_shares_by_bank)

        # Cost is at cap, not at historical trade prices
        spent_pre = target_shares * cap
        spent_after = spent_pre * (1.0 + self.fee + self.slip)
        avg_no = spent_after / target_shares

        # Optional: build a “fills” breakdown by shares
        fills = []
        remaining = target_shares

        for b in eligible:
            if remaining <= 0:
                break
            block_sh = float(b.get("shares", 0.0))
            if block_sh <= 0.0:
                continue

            take_sh = min(block_sh, remaining)
            if take_sh <= 0.0:
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

    # (You can keep take_first_yes if you want, but for this sanity check it's not needed.)

# ======================================================
#  Old main wrapper helpers
# ======================================================

def rolling_markets(bank, check, limit=BATCH_LIMIT, offset=START_OFFSET,
                    max_price_cap=CAP_NO, fee_bps=FEE_BPS, slip_bps=SLIP_BPS):
    """
    Runs through up to `limit` markets starting at `offset`, placing a NO bet
    per market as a MAKER at `max_price_cap`.

    Returns (pnl_sum, bank, next_offset, num_markets, last_createdAt, total_spent)
    """
    pnl_sum = 0.0
    markets = filter_markets(fetch_markets(limit, offset))
    if not markets:
        return 0.0, bank, offset, 0, None, 0.0

    next_offset = offset + len(markets)
    spent = 0.0

    for market in markets:
        try:
            trades_raw = fetch_trades(market)
            trades = normalize_trades(trades_raw)
            if not trades:
                continue

            # maker-at-cap sim start time: first trade
            t_from = trades[0]["time"]

            # fixed bet from global
            if bank >= BET_SIZE:
                bet = BET_SIZE
            elif bank >= MIN_BANK:
                bet = float(bank)  # go all-in if between MIN_BANK and BET_SIZE
            else:
                print("Out of money / below min bank, breaking.")
                break

            sim = SimMarket(trades, fee_bps=fee_bps, slip_bps=slip_bps)

            if check == "no":
                shares, spent_after, avg_, fills = sim.take_first_no(
                    t_from, dollars=bet, max_no_price=max_price_cap
                )
            else:
                # We’re only using NO in this sanity script
                continue

            # skip if no fill
            if shares == 0.0 or spent_after == 0.0:
                continue

            # outcome from outcomePrices (still heuristic)
            outcome_raw = market.get("outcomePrices", ["0", "0"])
            outcome = json.loads(outcome_raw) if isinstance(outcome_raw, str) else outcome_raw
            yes_p, no_p = float(outcome[0]), float(outcome[1])

            if check == "no":
                won = (no_p > yes_p)
            else:
                won = False

            pnl = (shares - spent_after) if won else (-spent_after)

            bank += pnl
            pnl_sum += pnl
            spent += spent_after

            print(market["question"])
            print(
                f"shares={shares:.2f} | spent={spent_after:.2f} | cap={max_price_cap:.3f} "
                f"| avg={avg_:.4f} | outcome={'WON' if won else 'LOST'} "
                f"| pnl={pnl:.2f} | running_PL={pnl_sum:.2f} | bank={bank:.2f}"
            )

            time.sleep(0.5)  # slow it a bit

            if bank < MIN_BANK:
                print("bank below MIN_BANK; stopping batch.")
                break

        except Exception as e:
            print(f"[skip] {market.get('question','<no title>')}: {e}")

    last_created = markets[0].get("createdAt") if markets else None
    return pnl_sum, bank, next_offset, len(markets), last_created, spent

def main():
    run_simple()

def run_simple():
    bank = START_BANK
    offset = START_OFFSET
    all_pl = 0.0
    all_bets = 0
    spent = 0.0

    for batch_idx in range(MAX_BATCHES):
        if bank < MIN_BANK:
            print("Bank below MIN_BANK, stopping.")
            break

        time.sleep(1)
        pnl_batch, bank, offset, bets, createdAt, sp = rolling_markets(
            bank,
            check="no",
            limit=BATCH_LIMIT,
            offset=offset,
            max_price_cap=CAP_NO,
            fee_bps=FEE_BPS,
            slip_bps=SLIP_BPS,
        )
        all_pl += pnl_batch
        all_bets += bets
        spent += sp

        print("-" * 61)
        print(
            f"bets_this_batch: {bets} | total_bets: {all_bets} | "
            f"batch P/L: {pnl_batch:.2f} | total P/L: {all_pl:.2f} | "
            f"bank: {bank:.2f} | next offset: {offset}"
        )
        print("-" * 61)
        write_to_file(
            "look.txt",
            f"bets_total:{all_bets} | total spent {spent:.2f} "
            f"| batch P/L: {pnl_batch:.2f} | total P/L: {all_pl:.2f} "
            f"| bank: {bank:.2f} | next offset: {offset} | timestamp {createdAt}"
        )

        if bets == 0:
            print("No markets processed in this batch; stopping.")
            break


# ======================================================
#  HTTP helpers
# ======================================================

def safe_get(url, *, params=None, timeout=(5, 20)):  # 5s connect, 20s read
    return SESSION.get(url, params=params, timeout=timeout)


def fetch_markets(limit=20, offset=4811):
    params = {
        "limit": limit,
        "offset": offset,
        "outcomes": ["YES", "NO"],
        "sortBy": "startDate",
    }
    r = requests.get(BASE_GAMMA, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    return payload


def filter_markets(markets):
    """
    Only keep clean yes/no markets with a startDate.
    """
    if not markets:
        print("ERROR: no markets in page")
        return []
    cleaned = []
    for mk in markets:
        outcomes = mk.get("outcomes")
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except Exception:
                outcomes = None
        try:
            if outcomes == ["Yes", "No"] and mk.get("startDate"):
                cleaned.append(mk)
        except Exception:
            continue

    print(f"valid markets {len(cleaned)}")
    cleaned = sorted(cleaned, key=lambda x: normalize_time(x["startDate"]))
    return cleaned


def fetch_trades(market_dict):
    """Pull full trade history with retries+timeouts and a hard time budget."""
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
#  Time + trade normalization (SIMPLE, no aggregation)
# ======================================================

def normalize_time(value, default=None):
    """
    Converts various Polymarket-style date/time formats into a UTC datetime.
    """
    if value is None or value == "":
        return default

    # numeric timestamp (epoch seconds / ms)
    if isinstance(value, (int, float)) or re.match(r"^\d{10,13}$", str(value)):
        try:
            ts = float(value)
            if ts > 1e12:  # milliseconds
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return default

    # string normalization
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
    # same logic you had before
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
    """
    Same validity filter as before.
    """
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
    """
    SIMPLE VERSION (no aggregation):

    - Sort trades by timestamp ascending
    - Each valid trade becomes a single "block"
    """
    if not trades:
        print("ERROR NO TRADES AVAILABLE")
        return []

    # timestamp might be ms; your old code treated it as seconds-ish
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
            "compressed": 1,  # kept just for compatibility
        })

    return sorted(blocks, key=lambda b: b["time"])

# ======================================================
#  Misc helpers
# ======================================================

def ls_print(li):
    for i in li:
        print(i)


def write_to_file(filename, data):
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
    with open(filename, "a", encoding="utf-8") as file:
        file.seek(0, os.SEEK_END)
        if file.tell() > 0:
            file.write("\n")
        file.write(data + "\n")

# ======================================================
if __name__ == "__main__":
    main()
