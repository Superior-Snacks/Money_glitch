import json
import time
import requests
from datetime import datetime, timezone
import pandas as pd
import matplotlib.pyplot as plt
import time, json, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import bisect
import re
import heapq


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

            # price cap (skip too-expensive NO)
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

            p_no  = float(b.get("price_no", 0.0))
            p_yes = float(b.get("price_yes", 0.0))
            avail = float(b.get("notional_no", 0.0))   # available notional $ for NO in this block
            blk_sh = float(b.get("shares", 0.0))       # total shares in this block (for this side)

            # optional price cap (skip too-expensive NO)
            if max_yes_price is not None and p_no > max_yes_price:
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
            return 0.0, 0.0, 0.0, []

        spent_after = spent_pre * (1.0 + self.fee + self.slip)
        avg_yes = spent_after / shares
        print(shares, spent_after, avg_yes, fills)
        return shares, spent_after, avg_yes, fills
    

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_TRADES = "http://data-api.polymarket.com/trades"
BASE_BOOK = "https://clob.polymarket.com/book"
DATA_TRADES = "https://data-api.polymarket.com/trades"
    
# --- locked-capital tracking ---
locked_now = 0.0              # current locked capital ($)
peak_locked = 0.0             # highest locked capital ever reached
peak_locked_time = None       # when the peak happened
first_trade = None
last_settle = None
mk_by_id_global = {}
SETTLE_FEE = 0.01  # 1% on winnings (only when you win)

# --- position store ---
positions_by_id = {}         # id -> position dict
settle_heap = []             # (time1, id) min-heap for next resolution

def main():
    bank = 5000.0
    offset = 4811 + 5900
    spent = 0.0
    desired_bet = 100

    global mk_by_id_global
    mk_by_id_global = {}

    for _ in range(100):
        time.sleep(1)
        limit = 100
        markets = filter_markets(fetch_markets(limit, offset))
        offset += len(markets)

        # make outcomes visible across ALL batches
        for m in markets:
            mk_by_id_global[m["conditionId"]] = m

        # prepare entries once, sort by actual first trade time
        entries = []
        for m in markets:
            blocks = normalize_trades(fetch_trades(m))
            if not blocks:
                continue
            entry_t = normalize_time(blocks[0]["time"])
            entries.append((entry_t, m, blocks))

        entries.sort(key=lambda x: x[0])

        for entry_t, market, blocks in entries:
            # 1) settle everything due up to this entry time
            bank, settled = settle_due_positions(bank, entry_t, outcome_lookup=my_outcome_func)
            # (optional) pretty-print settlements here
            for i in settled:
                print(f'SETTLED ${i["proceeds"]:.2f} | {i["entry_time"]} || {i["settle_time"]} || {i["question"]}')

            # 2) skip if we already opened this market
            pid = market["conditionId"]
            if pid in positions_by_id:
                continue

            # 3) size bet
            bet = desired_bet if bank >= desired_bet else (float(bank) if bank >= 10.0 else 0.0)
            if bet <= 0.0:
                print("Bank too low; stopping.")
                return

            # 4) simulate fills directly with these blocks (no second fetch)
            sim = SimMarket(blocks, fee_bps=600, slip_bps=200)
            shares, spent_after, avg_, fills = sim.take_first_no(
                entry_t, dollars=bet, max_no_price=0.4
            )
            if shares == 0.0 or spent_after == 0.0:
                continue

            # 5) compute robust settle time
            settle_t = (normalize_time(market.get("umaEndDate"))
                        or normalize_time(market.get("closedTime"))
                        or normalize_time(market.get("endDate"))
                        or entry_t)

            # 6) open exactly once
            bank = open_position(
                bank, market, fills, spent_after, shares, entry_t, settle_t, side="NO"
            )
            spent += spent_after

            print(market["question"])
            print(f"fills={len(fills)} | shares={shares:.2f} | spent(after)={spent_after:.2f} | avg={avg_:.4f} | bank={bank:.2f}")
            print(f"LOCKED NOW: ${locked_now:.2f} | PEAK LOCKED: ${peak_locked:.2f} at {peak_locked_time}")
        
        # optional: settle up to the last entry time of the batch
        if entries:
            last_entry = entries[-1][0]
            bank, _ = settle_due_positions(bank, last_entry, outcome_lookup=my_outcome_func)

        if bank < 10.0:
            print("Bank below $10; stopping.")
            break


def prepare_market_entry(market):
    blocks = normalize_trades(fetch_trades(market))
    if not blocks:
        return None
    entry_t = normalize_time(blocks[0]["time"])
    return entry_t, blocks

def my_outcome_func(market_id: str) -> str:
    mk = mk_by_id_global.get(market_id)
    if not mk:
        return "NO"  # conservative fallback
    raw = mk.get("outcomePrices")
    arr = json.loads(raw) if isinstance(raw, str) else raw
    if not arr or len(arr) < 2:
        return "NO"
    y, n = float(arr[0]), float(arr[1])
    return "YES" if y > n else "NO"

def sanity_check_locked():
    global locked_now
    s = sum(p["spent_after"] for p in positions_by_id.values())
    if abs(s - locked_now) > 1e-6:
        print(f"[WARN] locked_now drift: ledger={locked_now:.2f} vs recomputed={s:.2f}. Resetting to recomputed.")
        locked_now = s

def open_position(bank, market, fills, spent_after, shares, entry_time, est_settle_time, side):
    """
    Lock capital immediately. No P&L yet.
    """
    global locked_now, peak_locked, peak_locked_time

    pid = market["conditionId"]
    bank -= spent_after  # cash leaves the bank and is now locked

    pos = {
        "id": pid,
        "question": market["question"],
        "entry_time": entry_time,          # datetime (UTC)
        "settle_time": est_settle_time,    # datetime (UTC)
        "spent_after": float(spent_after), # includes fees/slippage
        "shares": float(shares),
        "side": side,                      # "NO" or "YES"
        "fills": fills,
    }

    positions_by_id[pid] = pos
    heapq.heappush(settle_heap, (est_settle_time, pid))

    # ---- locked capital accounting ----
    locked_now += pos["spent_after"]
    if locked_now > peak_locked:
        peak_locked = locked_now
        peak_locked_time = entry_time

    return bank

def settle_due_positions(bank, now_utc, outcome_lookup):
    """
    outcome_lookup(id) -> ('YES' or 'NO') or (yes_p, no_p)
    Only settle positions whose settle_time <= now_utc.
    Returns updated bank and a list of settlements with P&L.
    """
    global locked_now

    settlements = []
    while settle_heap and settle_heap[0][0] <= now_utc:
        _, pid = heapq.heappop(settle_heap)
        pos = positions_by_id.pop(pid, None)
        if not pos:
            continue

        # This position is no longer locked after settlement
        locked_now -= pos["spent_after"]
        if locked_now < 0:
            locked_now = 0.0  # safety

        # decide winner
        outcome = outcome_lookup(pid)
        if isinstance(outcome, tuple) and len(outcome) == 2 and all(isinstance(x, (int, float)) for x in outcome):
            yes_p, no_p = map(float, outcome)
            won = (no_p > yes_p) if pos["side"] == "NO" else (yes_p > no_p)
        else:
            won = (outcome == pos["side"])

        spent = pos["spent_after"]
        if won:
            proceeds = pos["shares"] * 1.0
            proceeds_after = proceeds * (1.0 - SETTLE_FEE)
            pnl = proceeds_after - spent
            bank += proceeds_after
        else:
            proceeds_after = 0.0
            pnl = -spent

        settlements.append({
            "id": pid, "question": pos["question"],
            "won": won, "spent": spent, "proceeds": proceeds_after, "pnl": pnl,
            "entry_time": pos["entry_time"], "settle_time": pos["settle_time"]
        })
    return bank, settlements


def recompute_locked_from_positions():
    # authoritative recompute if you ever get out of sync
    return sum(p["spent_after"] for p in positions_by_id.values())


# 2) Safe GET with strict timeouts (connect, read)
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
    """Pull 100 first trades"""
    cid = market_dict["conditionId"]
    time.sleep(1)
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
def valid_trade(trade, min_spend=2, extreme_price=0.01 ,min_extreme_notional=20.0):
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

if __name__ == "__main__":
    main()