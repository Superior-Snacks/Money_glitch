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
            if bank >= 100.0:
                bet = 100.0
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
            if avg_ < 0.09 and won: #might not need to !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                pnl = 0

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

def main():
    run_simple()

def run_simple():
    bank = 5000.0
    offset = 4811 + 5900 #pressent 21186
    all_pl = 0.0
    all_bets = 0
    spent = 0.0
    pending = []
    end = []

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