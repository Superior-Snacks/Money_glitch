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
        "sortBy": "creationTime"}
    r = requests.get(BASE_GAMMA, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def get_trade_for_market(marked_dict):
    params = {"market": marked_dict["conditionId"],
              "sort": "asc",
              "limit": "max"}
    r = requests.get(BASE_TRADES, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    return payload

def calculate_price(trades):
    """
    calculate the price for a given time of trades
    """

        
def calculate_market(market, trades):
    """
    give a list with sets that give the time and price available at the time
    """
    if not trades:
        return "ERROR"
    history = []
    for period in range(0,len(trades), 4):
        history.append(trades[period:period+4])

# EXAMPLE USAGE
def main():
    # Start near your discovered first-with-history offset
    offset_history = 74669      #history start
    offset_trade = 4811 + 30000       #trade start
    markets = fetch_markets(30, offset_trade)
    #m = markets[0]  # pass a single dict, not the list
    #n = get_trade_for_market(m)
    #p = calculate_price(n)
    for trades in markets:
        single = get_trade_for_market(trades)
        print(calculate_price(single))

    # If you want to loop multiple markets:
    # batch = fetch_markets(limit=50, offset=73983)
    # for m in batch:
    #     try:
    #         get_history_for_market(m, "Yes", "max")
    #     except Exception as e:
    #         print("[skip]", m.get("id"), e)
    #     time.sleep(0.15)  # be polite

def run_algo(market, trades):
    outcome = market["outcome"]
    for i in trades:
        ...

if __name__ == "__main__":
    main()