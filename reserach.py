import requests
import sys
import time

def get_history():
    # 1) pick a market and get its outcome token IDs
    mkts = requests.get("https://clob.polymarket.com/markets", params={"cursor": ""}).json()
    first = mkts["data"][0]
    tokens = first["tokens"]            # each has token_id and outcome name
    tok_yes = next(t for t in tokens if t["outcome"].lower() in ("yes", "y", "1"))

    # 2) fetch price history for that token (full range, hourly bars)
    hist = requests.get(
        "https://clob.polymarket.com/prices-history",
        params={"market": tok_yes["token_id"], "interval": "1h"}
    ).json().get("history", [])

    print(first["question"])
    print(tok_yes["token_id"], len(hist))
    print(hist[:3])

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
        print(m["question"], m["createdAt"])

def save_to_history():
    ...

def run_test_algo():
    ...


def main():
    get_market_data()
    get_history()


if __name__ == "__main__":
    main()