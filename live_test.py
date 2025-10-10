import json
import time
import requests
from datetime import datetime, timezone
import pandas as pd
import matplotlib.pyplot as plt
import time, json, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import bisect
import re
import heapq

"""
just start slow on new markets
trding current open not neccisarry, but not bad to check
"""

"""
fyrst sækja alla active markaði, síðan fara yfir book hjáhverjum og einum og sim hvort/hversu mikið er keypt,
savea kaupið og potential winnings og allt info i kringum það í file, síðan á 10 min fresti kanski 1 min sækja alla nýja markaði og ef það er liquidity kaupa,
annars bíða þar til það er liquidity, hafa sér pending file, svo 1 klst fresti checka hvort markaðir hafa klárast.
ef það eru mikið af markets þá byrja þannig ég hef efni á continuous trades á nýjum mörkuðum, (eldri markaðir ekki lokaðir eru oftast "settled")
hafa sterk guards á eldri mörkuðum þannig tap er ekki of hátt.
3 files, bought, pending, log(bottom shows p/l, active, done, bank, lockedup, etmifallsold)
"""
BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_BOOK = "https://clob.polymarket.com/book"
DATA_TRADES = "https://data-api.polymarket.com/trades"


def main():
    open_markets = fetch_all_open_yesno_markets(limit=100, sleep_between=0.3, verbose=True)
    print(len(open_markets))
    for i in open_markets:
        print(f"{i["question"]} || {i["startdate"]}")
    #print(f)


def make_session():
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,  # 0.5, 1.0, 2.0, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "research-bot/1.0"})
    return s
SESSION = make_session()


def fetch_markets_page(offset=0, limit=100, order_field="startDate", ascending=True):
    params = {
        "limit": limit,
        "offset": offset,
        "order": order_field,        # e.g., "startDate" or "createdAt"
        "ascending": str(ascending).lower(),  # "true" | "false"
        # optional server-side filter examples:
        # "closed": False,
        # "start_date_min": "2024-01-01T00:00:00Z",
        # "start_date_max": "2025-01-01T00:00:00Z",
    }
    r = SESSION.get(BASE_GAMMA, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_all_open_yesno(limit=100, order_field="startDate", ascending=True, sleep_between=0.25):
    all_rows, offset = [], 0
    while True:
        try:
            page = fetch_markets_page(offset=offset, limit=limit, order_field=order_field, ascending=ascending)
        except requests.HTTPError as e:
            print(f"[WARN] Gamma fetch failed at offset {offset} with {order_field=}, {ascending=}: {e}")
            # gentle fallback: drop ordering if server complains
            r = SESSION.get(BASE_GAMMA, params={"limit": limit, "offset": offset}, timeout=20)
            r.raise_for_status()
            page = r.json()

        if not page:
            break

        all_rows.extend(page)
        if len(page) < limit:
            break
        offset += limit
        if sleep_between:
            time.sleep(sleep_between)

    # client-side filter: open & exact Yes/No
    def is_yesno_open(m):
        outs = m.get("outcomes")
        if isinstance(outs, str):
            try:
                outs = json.loads(outs)
            except Exception:
                outs = None
        is_open = (m.get("closed") is False) or (m.get("acceptingOrders") is True) or (m.get("active") is True)
        return outs == ["Yes", "No"] and is_open

    cleaned = [m for m in all_rows if is_yesno_open(m)]
    # ensure deterministic order locally too
    cleaned.sort(key=lambda m: m.get("startDate") or m.get("createdAt") or "")
    return cleaned

def filter_markets(markets):
    """
    filter markets by date and type
    """
    if not markets:
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

def fetch_book(market_id):
    """
    fetch book for a single market
    """

def value_book(book):
    """
    calculate how much value can be taken
    """

def decide_book(bookvalue):
    """
    given value book, decide 
    """


def save_to_file(filename, data):
    # ensure parent folder exists
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)

    with open(filename, "a", encoding="utf-8") as file:
        # if file already has content, add newline first
        file.seek(0, os.SEEK_END)
        if file.tell() > 0:
            file.write("\n")
        file.write(data + "\n")

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