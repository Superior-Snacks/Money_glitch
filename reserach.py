import json
import time
import requests
from datetime import datetime, timezone

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_TRADES = "http://data-api.polymarket.com/trades"

def fetch_markets(limit=20, offset=73983):
    params = {
        "limit": limit,
        "offset": offset,
        "sortBy": "creationTime"
    }
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
    print(payload)
    for trader in payload:
        print(trader["name"], trader["title"], trader["side"], trader["price"], time.asctime(time.localtime(trader["timestamp"])))

def run_algo(trades):
    ...





# EXAMPLE USAGE
def main():
    # Start near your discovered first-with-history offset
    offset_history = 74669      #history start
    offset_trade = 4811       #trade start
    markets = fetch_markets(1, offset_trade)
    m = markets[0]  # pass a single dict, not the list
    get_trade_for_market(m)

    # If you want to loop multiple markets:
    # batch = fetch_markets(limit=50, offset=73983)
    # for m in batch:
    #     try:
    #         get_history_for_market(m, "Yes", "max")
    #     except Exception as e:
    #         print("[skip]", m.get("id"), e)
    #     time.sleep(0.15)  # be polite

if __name__ == "__main__":
    main()