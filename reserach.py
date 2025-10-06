import json
import time
import requests
from datetime import datetime, timezone
import pandas as pd
import matplotlib.pyplot as plt

class SimMarket:
    def __init__(self, blocks, fee_bps=150, slip_bps=20):
        self.blocks = sorted(blocks, key=lambda b: b["time"])
        self.fee = fee_bps/10000.0
        self.slip = slip_bps/10000.0

    def take_first_no(self, t_from, dollars=100, max_no_price=None):
        """
        take first 100 dollar no shares available
        """
        spent = 0.0
        trades_taken = []
        for trade in self.blocks:
            if trade["side"] != "no" or trade["time"] < t_from:
                continue
            p_no = trade["price_no"]
            if max_no_price is not None and p_no > max_no_price:
                continue
            available = trade["notional_no"]
            if available <= 0:
                continue
            need = dollars - spent
            if need <= 0:
                break
            take = min(need, available)
            add_shares = take / p_no
            shares += add_shares
            spent  += take
            trades_taken.append((trade["time"], p_no, add_shares, take))
            if spent >= dollars: break

        if shares == 0: 
            return 0.0, 0.0, 0.0, []

        gross = spent * (1 + self.fee + self.slip)
        return shares, gross, gross/shares, trades_taken
    def take_first_yes():
        """
        take first 100 dollar yes shares available
        should be bad if my theory is correct
        """



#start with a few 50 markets, then test rolling continuous
def main():
    m = filter_markets(fetch_markets(limit=1, offset=51005))
    for i in m:
        print(i["question"])
        print("normalized trades")
        k = fetch_trades(i)
        n = normalize_trades(k)
        ls_print(n)
        print(len(n))


    """ plotting
    df = blocks_to_df(n)
    print(df.head())

    plot_yes_price(df)
    plot_no_price(df)
    plot_notional_flow(df, freq="2min")
    plot_depth_scatter(df)"""

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_TRADES = "http://data-api.polymarket.com/trades"
BASE_BOOK = "https://clob.polymarket.com/book"

def fetch_markets(limit=20, offset=4811):
    params = {
        "limit": limit,
        "offset": offset,
        "outcomes": ["YES", "NO"],
        "sortBy": "creationTime"}
    r = requests.get(BASE_GAMMA, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    #print(payload[0].keys())
    return payload

def filter_markets(markets):
    """
    making sure to only check the weird bets
    """
    print("filtering markets")
    if not markets:
        print("ERROR")
        return None
    cleaned = []
    for mk in markets:
        outcomes = mk["outcomes"]
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if outcomes == ["Yes", "No"]:
            cleaned.append(mk)
    print(f"valid markets {len(cleaned)}")
    return cleaned


def fetch_trades(market_dict):
    params = {
    "market": market_dict["conditionId"],
    "sort": "asc",
    "limit": "max"
    }
    r = requests.get(BASE_TRADES, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    #print(payload[0].keys())
    return payload


"""
ignore low share high value trades, condence no and yes shares together by time block
Taking YES: (outcome=="Yes" and side=="BUY") or (outcome=="No" and side=="SELL")
Taking NO: (outcome=="No" and side=="BUY") or (outcome=="Yes" and side=="SELL")
notional(YES) = shares * price
notional(NO) = shares * (1 - price)

Window: group consecutive trades within ≤ 10s of the first print.
Same snapped price only; stop the block on any trade (any side) at a different snapped price.
Build two streams: one for taking YES, one for taking NO.
Store per block:
{
"time": t0,                # first fill time in block
"price_yes": p_yes,        # snapped YES price in [0,1]
"price_no":  1 - p_yes,
"side": "TAKE_NO" | "TAKE_YES",
"shares": cumulative_shares_in_block,
"notional_yes": shares * p_yes   if TAKE_YES else 0,
"notional_no":  shares * (1-p_yes) if TAKE_NO else 0,
}
"""
def normalize_trades(trades, time_block=10):
    #print("new market")
    if not trades:
        print("ERROR NO TRADES AVAILABLE")
        return []
    #print(trades[0].keys())
    trades = sorted(trades, key=lambda t: t["timestamp"])
    
    j = 0
    i = 0
    side = None
    blocks = []
    while i < len(trades):
        tr1 = trades[i]
        if not valid_trade(tr1):
                #print("not valid?")
                i += 1
                continue
        p_yes = snap_price(tr1["price"], 0.01)
        p_no = round(1 - p_yes, 2)
        time0 = tr1["timestamp"]
        side = "TAKE_YES" 
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
                #print("broke time")
                break
            if snap_price(tr["price"], 0.01) != p_yes:
                #print("broke price")
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
            notional_yes = notional
            notional_no = 0.0
        elif side == "no":
            notional_no = notional
            notional_yes = 0.0
        if shares > 0 and notional > 0:
            blocks.append({"time":time0, 
                           "side": side, 
                           "price_yes":p_yes, 
                           "price_no":p_no, 
                           "shares":shares, 
                           "notional_yes":notional_yes, 
                           "notional_no":notional_no,
                           "compressed": compressed})
        i = max(j, i+1) 
    return sorted(blocks, key=lambda b: b["time"])

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

def ls_print(li):
    for i in li:
        print(i)

# ---------- Core helpers ----------

def blocks_to_df(blocks):
    """
    Convert your list of block dicts into a tidy DataFrame.
    Required keys per block:
      - time (epoch seconds, int)
      - side ("TAKE_NO" | "TAKE_YES")
      - price_yes (float in [0,1])
      - price_no  (float in [0,1]) [optional; computed if missing]
      - shares (float)
      - EITHER: notional (float)
        OR:     notional_yes / notional_no (floats; we’ll pick the right one by side)
    """
    if not blocks:
        return pd.DataFrame(columns=["time","side","price_yes","price_no","shares","notional"])

    rows = []
    for b in blocks:
        ts = datetime.fromtimestamp(int(b["time"]), tz=timezone.utc)
        side = b["side"]
        p_yes = float(b["price_yes"])
        p_no  = float(b.get("price_no", 1.0 - p_yes))
        shares = float(b.get("shares", 0.0))

        if "notional" in b:
            notional = float(b["notional"])
        else:
            if side == "TAKE_NO":
                notional = float(b.get("notional_no", 0.0))
            else:
                notional = float(b.get("notional_yes", 0.0))

        rows.append({
            "time": ts,
            "side": side,
            "price_yes": p_yes,
            "price_no": p_no,
            "shares": shares,
            "notional": notional,
        })

    df = pd.DataFrame(rows).sort_values("time").reset_index(drop=True)
    return df

# ---------- Plots ----------

def plot_yes_price(df):
    """Line chart of YES price over time (per block)."""
    if df.empty:
        print("No data to plot.")
        return
    plt.figure(figsize=(9,4))
    plt.plot(df["time"], df["price_yes"])
    plt.title("YES Price Over Time (per block)")
    plt.xlabel("Time (UTC)")
    plt.ylabel("YES Price (0–1)")
    plt.tight_layout()
    plt.show()

def plot_no_price(df):
    """Line chart of NO price over time (per block)."""
    if df.empty:
        print("No data to plot.")
        return
    plt.figure(figsize=(9,4))
    plt.plot(df["time"], df["price_no"])
    plt.title("NO Price Over Time (per block)")
    plt.xlabel("Time (UTC)")
    plt.ylabel("NO Price (0–1)")
    plt.tight_layout()
    plt.show()

def plot_notional_flow(df, freq="5min"):
    """
    Bar chart of notional flow by side in time buckets.
    freq: pandas offset alias (e.g., '1min','5min','15min','1H')
    """
    if df.empty:
        print("No data to plot.")
        return
    g = (df.set_index("time")
           .groupby([pd.Grouper(freq=freq), "side"])["notional"]
           .sum()
           .unstack(fill_value=0.0))
    plt.figure(figsize=(10,4))
    plt.bar(g.index, g.get("TAKE_YES", pd.Series(index=g.index, dtype=float)), width=0.01)
    plt.bar(g.index, g.get("TAKE_NO", pd.Series(index=g.index, dtype=float)))
    plt.title(f"Notional Flow by Side (bucket={freq})")
    plt.xlabel("Time (UTC)")
    plt.ylabel("Notional ($)")
    plt.tight_layout()
    plt.show()

def plot_depth_scatter(df):
    """
    Scatter of per-block 'available' shares vs price.
    Uses YES price for TAKE_YES points and NO price for TAKE_NO.
    """
    if df.empty:
        print("No data to plot.")
        return
    yes_pts = df[df["side"]=="TAKE_YES"]
    no_pts  = df[df["side"]=="TAKE_NO"]
    plt.figure(figsize=(7,5))
    plt.scatter(yes_pts["price_yes"], yes_pts["shares"])
    plt.scatter(no_pts["price_no"],  no_pts["shares"])
    plt.title("Per-block Lower-bound Shares vs Price")
    plt.xlabel("Price (YES for TAKE_YES, NO for TAKE_NO)")
    plt.ylabel("Shares (lower bound)")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()

"""
first idea
import json
import time
import requests
from datetime import datetime, timezone

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_TRADES = "http://data-api.polymarket.com/trades"
BASE_BOOK = "https://clob.polymarket.com/book"

def fetch_markets(limit=20, offset=4811):
    params = {
        "limit": limit,
        "offset": offset,
        "outcomes": ["YES", "NO"],
        "sortBy": "creationTime"}
    r = requests.get(BASE_GAMMA, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def filter_markets(markets):
    making sure to only check the weird bets
    print("filtering markets")
    if not markets:
        print("ERROR")
        return None
    cleaned = []
    for mk in markets:
        outcomes = mk["outcomes"]
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if outcomes == ["Yes", "No"]:
            cleaned.append(mk)
    return cleaned


def get_trade_for_market(marked_dict):
    params = {"market": marked_dict["conditionId"],
              "sort": "asc",
              "limit": "max"}
    r = requests.get(BASE_TRADES, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    #print(payload[0].keys())
    return payload

def filter_no_trades(trades):
    only interested in the no position rn
    #print("started filtering out correct trades")
    if not trades:
        print("ERROR")
        return None
    bucket = []
    for tr in trades:
        if tr["side"] == "BUY" and tr["outcome"] == "No":
            bucket.append(tr)
            #print("got no")
        elif tr["side"] == "SELL" and tr["outcome"] == "Yes":
            bucket.append(tr)
            #print("got YES")
    return bucket

def compress_trades(trades):
    adds up trades that are the same giving a lower bound on how much was able to be bought
    needs to be filterd for the types I want to look at
    if not trades:
        print("ERROR")
        return None
    sections = []
    trades = sorted(trades, key=lambda t: t["timestamp"])
    curr_price = trades[0]["price"]
    curr_time = trades[0]["timestamp"]
    curr_size = 0 #first trade will always be true

    for trade in trades:
        if trade["price"] == curr_price:
            curr_size += trade["size"]
        else:
            sections.append({"time":curr_time, "size":curr_size, "price":curr_price})
            curr_price = trade["price"]
            curr_size = trade["size"]
            curr_time = trade["timestamp"]
    sections.append({"time":curr_time, "size":curr_size, "price":curr_price})
    return sections

def print_list(li):
    for i in li:
        print(i)

def calc_dollar_value(yes_price, shares):
    Dollar cost of buying `shares` NO at given YES price.
    NO price = 1 - YES price.
    return (1 - yes_price) * shares

def basic_buy_no_algo(trades, target=100):
    Try to buy `target` dollars worth of NO shares.
    Assumes trades are chronological and all at same or better price
    until price changes.
    Returns (shares_bought, avg_price_paid).
    if not trades:
        return 0,0
    spent = 0.0
    bought_shares = 0.0
    for tr in trades:
        price = float(tr["price_yes"])
        size = float(tr["shares"])
        available_value = calc_dollar_value(price, size)

        if available_value + spent <= target:
            spent += available_value
            bought_shares += size
        else:
            remaining = target - spent
            partial_shares = remaining / (1 - price)
            bought_shares += partial_shares
            spent += remaining
            print(f"Filled at NO={1-price:.3f} (YES={price:.3f})")
            return bought_shares, spent

    return bought_shares, spent


        
def go_through_it_all():
    offset_trade = 4811


def main():
    offset_trade = 4811 + 40000
    result = []
    pl = 0
    markets = fetch_markets(50, offset_trade)
    filterd_markets = filter_markets(markets)
    for market in filterd_markets:
        trades = get_trade_for_market(market)
        corrected_trade = filter_no_trades(trades)
        compress = compress_blocks_conservative(corrected_trade)
        print(compress)
        decision = basic_buy_no_algo(compress)
        outcome = json.loads(market["outcomePrices"])
        shares, spent = decision
        if spent > 0:
            if outcome == ["0","1"]:
                pl += (shares - spent)
                print("won")
            else:
                pl -= spent
                print("lost")
        result.append([market["question"], outcome, decision, pl])
        time.sleep(1)
    print_list(result)
    print(f"profit / loss {pl}")


TICK = 0.01
def snap(p): return round(round(float(p)/TICK)*TICK, 2)

def is_take_no(tr):
    o = str(tr.get("outcome","")).strip().lower()
    s = str(tr.get("side","")).strip().upper()
    return (o == "no" and s == "BUY") or (o == "yes" and s == "SELL")

def notional_no(tr):
    p = float(tr["price"])
    q = float(tr["size"])
    return q * (1.0 - p)

def compress_blocks_conservative(trades, window_s=5, min_trade_notional=5.0, min_price_no=0.02):
    Build lower-bound blocks for 'taking NO' with hard stops:
      - same snapped price
      - same direction (taking NO)
      - no intervening trade at a different price (any side)
      - time-bounded window (<= window_s from block start)
      - ignore dust trades (< min_trade_notional)
      - ignore extreme NO prices (< min_price_no) unless cumulative notional >= min_trade_notional within window
    Returns blocks: [{time, price_yes, shares, spent_no}]
    if not trades: return []
    trades = sorted(trades, key=lambda t: t["timestamp"])

    blocks = []
    i = 0
    while i < len(trades):
        tr = trades[i]
        p_yes = snap(tr["price"])
        p_no  = round(1.0 - p_yes, 2)
        t0    = tr["timestamp"]

        # Only start a block if this trade is actually 'taking NO' and not dust/too extreme
        if not is_take_no(tr):
            i += 1
            continue
        if notional_no(tr) < min_trade_notional:
            i += 1
            continue
        if p_no < min_price_no:
            # allow starting only if we'll accumulate >= min_trade_notional within the window
            # we'll re-check after accumulation; skip for now
            pass

        shares = 0.0
        spent  = 0.0
        j = i
        ok = True
        while j < len(trades):
            tj = trades[j]
            pj = snap(tj["price"])
            # stop if time window exceeded
            if tj["timestamp"] - t0 > window_s: break
            # stop if any trade at a different price (ANY side) appears
            if pj != p_yes:
                break
            # only count 'taking NO' at exactly this price
            if is_take_no(tj):
                val = notional_no(tj)
                if val >= min_trade_notional:  # ignore dust prints
                    shares += float(tj["size"])
                    spent  += val
            j += 1

        # enforce extreme-price guard
        if p_no < min_price_no and spent < min_trade_notional:
            ok = False

        if ok and shares > 0:
            blocks.append({"time": t0, "price_yes": p_yes, "shares": shares, "spent_no": spent})

        i = max(j, i+1)

    return blocks

if __name__ == "__main__":
    main()"""