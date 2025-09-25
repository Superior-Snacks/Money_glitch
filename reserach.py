import requests
import sys
import time
import json

def get_history(market):
    market = market[0]
# 1) Parse outcomes and token ids (they're JSON strings in your object)
    outcomes = json.loads(market["outcomes"])            # ["Yes", "No"]
    token_ids = json.loads(market["clobTokenIds"])       # ["3905...78175", "8965...48801"]

    # Map outcomes -> token_ids
    outcome_to_token = dict(zip(outcomes, token_ids))

    print("Market:", market["question"])
    print("Outcome -> Token mapping:", outcome_to_token)

    # 2) Pick which outcome you want the history for:
    token_id = outcome_to_token["Yes"]   # or "No"

    # 3) Fetch price history
    # Tip: start with interval="max" to see if any data exists
    params = {
        "market": token_id,   # yes, the param is named 'market' and takes a token_id
        "interval": "max"     # or '1h', '1d'; you can also use startTs/endTs (UNIX seconds)
    }
    resp = requests.get("https://clob.polymarket.com/prices-history", params=params)
    resp.raise_for_status()
    payload = resp.json()

    # Some responses nest under 'history'; others return a bare list—handle both:
    history = payload.get("history", payload)

    print("Points:", len(history))
    if history[:3]:
        print("Sample:", history[:3])

def get_market_data(ammount=1, offset=0):
    url = "https://gamma-api.polymarket.com/markets"
    params = {
        "limit": ammount,
        "offset": offset,
        "sortBy": "creationTime"
    }

    resp = requests.get(url, params=params)
    data = resp.json()
    for m in data:
        print(m)
        print(m["question"], m["createdAt"])
    return data

def save_to_history():
    ...

def run_test_algo():
    ...


def main():
    get_history(get_market_data(1, 73983))


if __name__ == "__main__":
    main()