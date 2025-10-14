import json
import time
import requests
from datetime import datetime, timezone
import time, json, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import re
import random, time
import os, json, time, math
from datetime import datetime, timedelta, timezone
import gzip
import glob

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_BOOK = "https://clob.polymarket.com/book"
DATA_TRADES = "https://data-api.polymarket.com/trades"

def main():
    yes_won = 0
    no_won = 0
    offset = 4811
    while True:
        new_markets = fetch_markets(limit=100, offset=offset)
        print(new_markets[0].keys())
        offset += 100
        valid_markets = filter_markets(new_markets)

def check_winners(markets):
    nos = 0
    yess = 0
    for market in markets:
        ...
    return nos, yess

def fetch_markets(limit=20, offset=4811):
    params = {
        "limit": limit,
        "offset": offset,
        "outcomes": ["YES", "NO"],
        "sortBy": "startDate"}
    r = requests.get(BASE_GAMMA, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    return payload

def filter_markets(markets):
    """
    making sure to only check the weird bets
    """
    if not markets:
        print("ERROR")
        return None
    cleaned = []
    for mk in markets:
        outcomes = mk["outcomes"]
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        try:
            if outcomes == ["Yes", "No"] and mk["startDate"]:
                cleaned.append(mk)
        except:
            continue
    print(f"valid markets {len(cleaned)}")
    cleaned = sorted(cleaned, key=lambda x: normalize_time(x["startDate"]))
    return cleaned

def normalize_time(value, default=None):
    """
    Converts various Polymarket-style date/time formats into a UTC datetime.

    Accepts:
      - ISO strings with or without 'Z'
      - 'YYYY-MM-DD' (no time)
      - timestamps (int, float, or numeric strings)
      - None or invalid → returns `default` (or None)

    Returns:
      datetime object (UTC timezone)
    """
    if value is None or value == "":
        return default

    # numeric timestamp (epoch seconds)
    if isinstance(value, (int, float)) or re.match(r"^\d{10,13}$", str(value)):
        try:
            ts = float(value)
            if ts > 1e12:  # milliseconds
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return default

    # string normalization
    val = str(value).strip()

    # Replace common ISO variants
    val = val.replace("Z", "+00:00")  # Z → UTC
    val = re.sub(r"\s+", "T", val)    # space → T

    # Add missing time or timezone if needed
    if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
        val += "T00:00:00+00:00"

    try:
        dt = datetime.fromisoformat(val)
        # ensure UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return default
    
if __name__ == "__main__":
    main()
