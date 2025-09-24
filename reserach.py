import requests
import sys
import time

url = "https://gamma-api.polymarket.com/markets"
params = {
    "limit": 1,
    "offset": 1000,
    "sortBy": "creationTime"
}

resp = requests.get(url, params=params)
data = resp.json()

for m in data:
    print(m["question"], m["fee"])
    #print(m.keys())


def get_history():
    m = requests.get("https://clob.polymarket.com/markets", params={"next_cursor": ""}).json()
    token_id = m["data"][0]["tokens"][0]["token_id"]  # pick the outcome you want

    # 2) get price history (1h bars, full range)
    hist = requests.get(
        "https://clob.polymarket.com/prices-history",
        params={"market": token_id, "interval": "1h"},
    ).json()["history"]

    print(hist[:3])

get_history()

def get_market_data():
    ...

def save_to_history():
    ...