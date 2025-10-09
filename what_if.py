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
    desired_bet = 1000

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