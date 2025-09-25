import json
import time
import requests
from datetime import datetime, timezone

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_TRADES = "http://data-api.polymarket.com/trades"

def _coerce_json_field(val):
    """Gamma sometimes returns JSON-encoded strings; sometimes real lists."""
    if isinstance(val, str):
        return json.loads(val)
    return val

def fetch_markets(limit=20, offset=73983):
    params = {
        "limit": limit,
        "offset": offset,
        "sortBy": "creationTime"    # <-- important to get oldest first
    }
    r = requests.get(BASE_GAMMA, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def price_history_for_token(token_id, interval="1h", start=None, end=None):
    params = {"market": token_id}
    if start and end:
        params.update({"startTs": int(start), "endTs": int(end)})
    else:
        params["interval"] = interval
    print(params)
    r = requests.get(BASE_HISTORY, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    print(payload)
    history = payload.get("history", payload)
    return history  # list of {t, p}

def get_history_for_market(market_dict, outcome="Yes", interval="1h"):
    # outcomes & clobTokenIds can be JSON strings; coerce to lists
    outcomes = _coerce_json_field(market_dict["outcomes"])
    token_ids = _coerce_json_field(market_dict["clobTokenIds"])
    mapping = dict(zip(outcomes, token_ids))

    if outcome not in mapping:
        # handle categorical or different casing
        print(f"[warn] Outcome '{outcome}' not in {outcomes}; using first outcome.")
        token_id = token_ids[0]
        outcome = outcomes[0]
    else:
        token_id = mapping[outcome]

    hist = price_history_for_token(token_id, interval=interval)
    print(f"Market: {market_dict.get('question') or market_dict.get('title')}")
    print("Outcome -> Token:", mapping)
    print("Points:", len(hist))
    if hist[:3]:
        print("Sample:", hist[:3])
    return hist

def get_trade_for_market(marked_dict):
    params = {"market": marked_dict["conditionId"],
              "sort": "asc",
              "limit": 100}

    r = requests.get(BASE_TRADES, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    print(payload[0].keys())
    for trader in payload:
        print(trader["title"], trader["side"], trader["price"], trader["timestamp"])






# EXAMPLE USAGE
def main():
    # Start near your discovered first-with-history offset
    offset = 74669
    markets = fetch_markets(1, offset)
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