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
    payload = r.json()
    #print(payload[0].keys())
    return payload

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
def normalize_trades(trades):
    if not trades:
        print("ERROR NO TRADES AVAILABLE")
        return None
    #print(trades[0].keys())
    
    for tr in trades:
        print(valid_trade(tr))
        if not valid_trade(tr):
            continue




"""
if trade is too small with too good odds
"""
def valid_trade(trade, min_spend=5):
    if not trade:
        return False
    price = trade["price"]
    size = trade["size"]

    if trade["outcome"].lower() == "yes":
        cost = price * size
    if trade["outcome"].lower() == "no":
        cost = (1 - price) * size



    if (trade["side"] == "BUY") and (trade["size"] < 5) or (trade["price"] < 0.05):
        return False
    elif (trade["side"] == "SELL") and (trade["size"] < 5) or (trade["price"] < 0.95):
        return False
    else:
        return True


    

def valid_yes(trade):
    ...



def simulate_market():
    """
    pretty much just print the trade blocks in time order? or gives a "book best price"
    add fees here
    """
    ...

def take_first_no():
    """
    take first 100 dollar no shares available
    """

def take_first_yes():
    """
    take first 100 dollar yes shares available
    should be bad if my theory is correct
    """

#start with a few 50 markets, then test rolling continuous
def main():
    m = fetch_markets(limit=20, offset=4811)
    t = fetch_trades(m[0])
    normalize_trades(t)


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