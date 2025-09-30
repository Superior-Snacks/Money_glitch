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
    """
    only interested in the no position rn
    """
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
    """
    adds up trades that are the same giving a lower bound on how much was able to be bought
    needs to be filterd for the types I want to look at
    """
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
    """
    Dollar cost of buying `shares` NO at given YES price.
    NO price = 1 - YES price.
    """
    return (1 - yes_price) * shares

def basic_buy_no_algo(trades, target=100):
    """
    Try to buy `target` dollars worth of NO shares.
    Assumes trades are chronological and all at same or better price
    until price changes.
    Returns (shares_bought, avg_price_paid).
    """
    if not trades:
        return 0,0
    spent = 0.0
    bought_shares = 0.0
    for tr in trades:
        price = float(tr["price"])
        size = float(tr["size"])
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
    offset_trade = 4811 + 69999
    result = []
    pl = 0
    markets = fetch_markets(50, offset_trade)
    filterd_markets = filter_markets(markets)
    for market in filterd_markets:
        trades = get_trade_for_market(market)
        corrected_trade = filter_no_trades(trades)
        compress = compress_trades(corrected_trade)
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
    
    # Start near your discovered first-with-history offset
    #offset_history = 74669      #history start
    #offset_trade = 4811 + 39999       #trade start
    #markets = fetch_markets(20, offset_trade)
    #markets = markets[0]  # pass a single dict, not the list
    #look = filter_markets(markets)
    #for i in look:
    #    print(i["question"], i["outcomes"])
"""    print(markets)
    n = get_trade_for_market(markets)
    #p = calculate_price(n)
    filt = n
    filt = filter_no_trades(n)
    print(n[0])
    c = compress_trades(filt)
    print_list(c)
    print(f"size of filterd market {len(c)}")
    print(f"size of unfliterd market {len(markets)}")
"""
if __name__ == "__main__":
    main()