import json
import time
import requests
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import re
# 1) One session for the whole script
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,  # 0.5, 1.0, 2.0, ...
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

class SimMarket:
    def __init__(self, blocks, fee_bps=0, slip_bps=20):
        # blocks: list of dicts with keys:
        #  time, side ("yes"|"no"), price_yes, price_no, notional_yes, notional_no, shares
        self.blocks = sorted(blocks, key=lambda b: b["time"])
        self.fee = fee_bps/10000.0
        self.slip = slip_bps/10000.0

    def take_first_no(self, t_from, dollars=100.0, max_no_price=None):
        # normalize t_from to epoch seconds
        if isinstance(t_from, datetime):
            t_from_ts = int(t_from.replace(tzinfo=timezone.utc).timestamp())
        else:
            t_from_ts = int(t_from)

        spent_pre = 0.0
        shares = 0.0
        fills = []

        for b in self.blocks:
            # time + side filter
            if b.get("side") != "no" or int(b.get("time", 0)) < t_from_ts:
                continue

            p_no  = float(b.get("price_no", 0.0))
            p_yes = float(b.get("price_yes", 0.0))
            avail = float(b.get("notional_no", 0.0))   # available notional $ for NO in this block
            blk_sh = float(b.get("shares", 0.0))       # total shares in this block (for this side)

            # optional price cap (skip too-expensive NO)
            if max_no_price is not None and p_no > max_no_price:
                continue

            # robust guards
            if avail <= 0.0 or blk_sh <= 0.0:
                continue

            need = dollars - spent_pre
            if need <= 0.0:
                break

            # proportional allocation: no division by p_no
            take = min(need, avail)                # $ notional we take from this block
            ratio = take / avail                   # fraction of block taken
            add_shares = blk_sh * ratio            # shares corresponding to that fraction

            fills.append({
                "time": b["time"],
                "side": "no",
                "price_no": p_no,
                "price_yes": p_yes,
                "take_notional_pre_fee": take,
                "take_shares": add_shares,
                "block": b,
            })

            spent_pre += take
            shares    += add_shares

            if spent_pre >= dollars:
                break

        if shares == 0.0:
            return 0.0, 0.0, 0.0, []   # no fill

        # apply frictions once on total notional
        spent_after = spent_pre * (1.0 + self.fee + self.slip)
        avg_no = spent_after / shares
        return shares, spent_after, avg_no, fills

    def take_first_yes(self, t_from, dollars=100.0, max_yes_price=None):
        # normalize t_from to epoch seconds
        if isinstance(t_from, datetime):
            t_from_ts = int(t_from.replace(tzinfo=timezone.utc).timestamp())
        else:
            t_from_ts = int(t_from)

        spent_pre = 0.0
        shares = 0.0
        fills = []

        for b in self.blocks:
            # time + side filter
            if b.get("side") != "yes" or int(b.get("time", 0)) < t_from_ts:
                continue

            p_yes = float(b.get("price_yes", 0.0))
            p_no  = float(b.get("price_no", 0.0))
            avail = float(b.get("notional_yes", 0.0))  # <-- YES notional
            blk_sh = float(b.get("shares", 0.0))

            # price cap should be on YES price here
            if max_yes_price is not None and p_yes > max_yes_price:
                continue

            # guards
            if avail <= 0.0 or blk_sh <= 0.0:
                continue

            need = dollars - spent_pre
            if need <= 0.0:
                break

            take = min(need, avail)       # $ notional taken from this YES block
            ratio = take / avail
            add_shares = blk_sh * ratio

            fills.append({
                "time": b["time"],
                "side": "yes",            # <-- correct side label
                "price_yes": p_yes,
                "price_no":  p_no,
                "take_notional_pre_fee": take,
                "take_shares": add_shares,
                "block": b,
            })

            spent_pre += take
            shares    += add_shares

            if spent_pre >= dollars:
                break

        if shares == 0.0:
            return 0.0, 0.0, 0.0, []

        spent_after = spent_pre * (1.0 + self.fee + self.slip)
        avg_yes = spent_after / shares
        return shares, spent_after, avg_yes, fills


def rolling_markets(bank, check, limit=50, offset=4811, max_price_cap=None, fee_bps=600, slip_bps=200):
    """
    Runs through up to `limit` markets starting at `offset`, placing a NO bet per market.
    Returns (pnl_sum, bank, next_offset).
    """
    pnl_sum = 0.0
    markets = filter_markets(fetch_markets(limit, offset))
    next_offset = offset + len(markets)
    spent = 0.0

    for market in markets:
        try:
            trades = normalize_trades(fetch_trades(market))
            if not trades:
                continue
            print(datetime.fromtimestamp(int(trades[0]["time"]), tz=timezone.utc))
            # sizing
            if bank >= 20.0:
                bet = 10.0
            elif bank >= 10.0:
                bet = float(bank)          # go all-in if small
            else:
                print("out of money")
                break

            sim = SimMarket(trades, fee_bps=fee_bps, slip_bps=slip_bps)
            t_from = trades[0]["time"]
            if check == "no":
                shares, spent_after, avg_, fills = sim.take_first_no(
                    t_from, dollars=bet, max_no_price=max_price_cap
                )
            elif check == "yes":
                shares, spent_after, avg_, fills = sim.take_first_yes(
                    t_from, dollars=bet, max_yes_price=max_price_cap
                )
            # skip if no fill
            if shares == 0.0 or spent_after == 0.0:
                continue

            # parse outcome robustly
            outcome_raw = market.get("outcomePrices", ["0", "0"])
            outcome = json.loads(outcome_raw) if isinstance(outcome_raw, str) else outcome_raw
            yes_p, no_p = float(outcome[0]), float(outcome[1])
            if check == "no":
                won = (no_p > yes_p)
            elif check == "yes":
                won = (no_p < yes_p)

            pnl = (shares - spent_after) if won else (-spent_after)
            #if avg_ < 0.09 and won: #might not need to !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            #    pnl = 0

            # update account & totals
            bank += pnl
            pnl_sum += pnl
            spent += spent_after

            print(market["question"])
            print(
                f"fills={len(fills)} | shares={shares:.2f} | spent(after)={spent_after:.2f} "
                f"| avg={avg_:.4f} | outcome={'WON' if won else 'LOST'} "
                f"| pnl={pnl:.2f} | running_PL={pnl_sum:.2f} | bank={bank:.2f}"
            )

            time.sleep(2)  # optional

            if bank < 10.0:
                print("bank below min bet; stopping batch.")
                break

        except Exception as e:
            print(f"[skip] {market.get('question','<no title>')}: {e}")

    return pnl_sum, bank, next_offset, len(markets), markets[0]["createdAt"], spent


BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_BOOK = "https://clob.polymarket.com/book"
DATA_TRADES = "https://data-api.polymarket.com/trades"

def main():
    run_simple()

def run_simple():
    bank = 5000.0
    offset = 4811 + 5900 #pressent 21186
    all_pl = 0.0
    all_bets = 0
    spent = 0.0

    # stop when bank < $10 or when you decide to cap batches
    for _ in range(100):  # up to 100 * 50 = 5000 markets
        time.sleep(1)
        pnl_batch, bank, offset, bets, createdAt, sp = rolling_markets(
            bank, check="no",
            limit=50, offset=offset,
            max_price_cap=0.4,  # e.g., 0.40 to avoid expensive NO
            fee_bps=600, slip_bps=200
        )
        all_pl += pnl_batch
        all_bets += bets
        spent += sp
        print("-" * 61)
        print(f"amount of bets:{all_bets} | batch P/L: {pnl_batch:.2f} | total P/L: {all_pl:.2f} | bank: {bank:.2f} | next offset: {offset}")
        print("-" * 61)
        write_to_file("look.txt", f"amount of bets:{all_bets} | total spent {spent:.2f} | batch P/L: {pnl_batch:.2f} | total P/L: {all_pl:.2f} | bank: {bank:.2f} | next offset: {offset} | timestamp{createdAt}")

        if bank < 10.0:
            break


def safe_get(url, *, params=None, timeout=(5, 20)):  # 5s connect, 20s read
    return SESSION.get(url, params=params, timeout=timeout)


def fetch_markets(limit=20, offset=4811):
    params = {
        "limit": limit,
        "offset": offset,
        "outcomes": ["YES", "NO"],
        "sortBy": "startDate"}
    r = requests.get(BASE_GAMMA, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    return payload

def filter_markets(markets):
    """
    making sure to only check the weird bets
    """
    if not markets:
        print("ERROR")
        return None
    cleaned = []
    for mk in markets:
        outcomes = mk["outcomes"]
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        try:
            if outcomes == ["Yes", "No"] and mk["startDate"]:
                cleaned.append(mk)
        except:
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
            timeout=(5, 20)
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload
    except:
        return None
    
def normalize_time(value, default=None):
    """
    Converts various Polymarket-style date/time formats into a UTC datetime.

    Accepts:
      - ISO strings with or without 'Z'
      - 'YYYY-MM-DD' (no time)
      - timestamps (int, float, or numeric strings)
      - None or invalid → returns `default` (or None)

    Returns:
      datetime object (UTC timezone)
    """
    if value is None or value == "":
        return default

    # numeric timestamp (epoch seconds)
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

    # Replace common ISO variants
    val = val.replace("Z", "+00:00")  # Z → UTC
    val = re.sub(r"\s+", "T", val)    # space → T

    # Add missing time or timezone if needed
    if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
        val += "T00:00:00+00:00"

    try:
        dt = datetime.fromisoformat(val)
        # ensure UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return default


def normalize_trades(trades, time_block=10):
    if not trades:
        print("ERROR NO TRADES AVAILABLE")
        return []
    trades = sorted(trades, key=lambda t: t["timestamp"])
    
    j = 0
    i = 0
    side = None
    blocks = []
    while i < len(trades):
        tr1 = trades[i]
        if not valid_trade(tr1):
                i += 1
                continue
        p_yes = snap_price(tr1["price"], 0.01)
        p_no = round(1 - p_yes, 2)
        time0 = tr1["timestamp"]
        if take_yes(tr1):
             side = "yes"
        elif take_no(tr1):
            side = "no"
        else:
            side = None

        notional = 0.0
        shares = 0.0
        compressed = 0
        j = i    
        while j < len(trades):
            tr = trades[j]
            if int(tr["timestamp"]) - time0 > time_block:
                break
            if snap_price(tr["price"], 0.01) != p_yes:
                break
            if not valid_trade(tr):
                j += 1
                continue
            if side == "yes":
                if not take_yes(tr):
                    break
                if not valid_trade(tr):
                    j += 1
                    continue
                shares += float(tr["size"])
                notional += notion_yes(tr)
                compressed += 1
                
            elif side == "no":
                if not take_no(tr):
                    break
                if not valid_trade(tr):
                    j +=1
                    continue
                shares += float(tr["size"])
                notional += notion_no(tr)
                compressed += 1

            j += 1

        if side == "yes":
            price_yes = clamp01((notional or 0.0) / shares) if shares > 0 else 0.5
            price_no  = clamp01(1.0 - price_yes)
            notional_yes = shares * price_yes
            notional_no  = 0.0
        elif side == "no":
                price_no  = clamp01((notional or 0.0) / shares) if shares > 0 else 0.5
                price_yes = clamp01(1.0 - price_no)
                notional_no  = shares * price_no
                notional_yes = 0.0
        if shares > 0 and notional > 0:
            blocks.append({"time": time0,
                        "side": side,  # "yes" or "no"
                        "price_yes": round(price_yes, 6),
                        "price_no":  round(price_no, 6),
                        "shares": shares,
                        "notional_yes": round(notional_yes, 6),
                        "notional_no":  round(notional_no, 6),
                        "compressed": compressed})
        i = max(j, i+1) 
    return sorted(blocks, key=lambda b: b["time"])

def clamp01(x, eps=1e-6):
    return min(1.0 - eps, max(eps, float(x)))

def snap_price(p, tick=0.01):
    # snap to exchange tick, then round nicely
    return round(round(float(p) / tick) * tick, 2)

def take_yes(trade):
    if (trade["outcome"].lower() =="no" and trade["side"].lower() == "sell") or (trade["outcome"].lower() == "yes" and trade["side"].lower() == "buy"):
        return True
    else:
        return False
def take_no(trade):
    if (trade["outcome"].lower() == "yes" and trade["side"].lower() == "sell") or (trade["outcome"].lower() == "no" and trade["side"].lower() == "buy"):
        return True
    else:
        return False

def notion_yes(trade):
    return float(trade["size"]) * float(trade["price"])
def notion_no(trade):
    return float(trade["size"]) * (1 - float(trade["price"]))


"""
if trade is too small with too good odds
"""
def valid_trade(trade, min_spend=2, extreme_price=0.05 ,min_extreme_notional=20.0):
    if not trade:
        return False
    
    price = float(trade["price"])
    size = float(trade["size"])
    outcome = str(trade.get("outcome","")).strip().lower()

    if outcome == "yes":
        cost = price * size
    elif outcome == "no":
        cost = (1 - price) * size
    else:
        return False

    if cost < min_spend:
        return False
    # Extreme prices allowed only for big enough notional
    if price < extreme_price or price > 1.0 - extreme_price:
        return cost >= min_extreme_notional
    #valid
    return True


def ls_print(li):
    for i in li:
        print(i)


def write_to_file(filename, data):
    # ensure parent folder exists
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)

    with open(filename, "a", encoding="utf-8") as file:
        # if file already has content, add newline first
        file.seek(0, os.SEEK_END)
        if file.tell() > 0:
            file.write("\n")
        file.write(data + "\n")


if __name__ == "__main__":
    main()