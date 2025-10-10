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
BASE_BOOK = "https://clob.polymarket.com/book"

import os, json, time, math
from datetime import datetime, timedelta, timezone

# --------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------
BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_BOOK = "https://clob.polymarket.com/book"
DATA_TRADES = "https://data-api.polymarket.com/trades"

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

# --------------------------------------------------------------------
# Book fetch
# --------------------------------------------------------------------
def fetch_book(token_id: str, depth: int = 10, session=SESSION):
    """Fetch the CLOB book for one token (YES or NO)."""
    r = session.get(BASE_BOOK, params={"token_id": token_id}, timeout=15)
    r.raise_for_status()
    book = r.json() or {}
    # normalize & trim
    book["bids"] = (book.get("bids") or [])[:depth]
    book["asks"] = (book.get("asks") or [])[:depth]
    return book

def get_no_token_id(market: dict) -> str | None:
    """Return the NO token id for a Yes/No market, else None."""
    outs = market.get("outcomes")
    if isinstance(outs, str):
        try: outs = json.loads(outs)
        except: outs = None
    toks = market.get("clobTokenIds") or []
    if isinstance(toks, str):
        try: toks = json.loads(toks)
        except: toks = []
    if outs == ["Yes", "No"] and len(toks) >= 2:
        return toks[1]  # index 1 is "No" when outcomes == ["Yes","No"]
    return None

# --------------------------------------------------------------------
# Watchlist Manager
# --------------------------------------------------------------------
class WatchlistManager:
    def __init__(self,
                 max_no_price=0.40,
                 min_notional=50.0,
                 fee_bps=600, slip_bps=200,
                 dust_price=0.02, dust_min_notional=20.0,
                 poll_every=60, backoff_base=120, backoff_max=3600):
        self.max_no_price = max_no_price
        self.min_notional = min_notional
        self.fee = fee_bps / 10_000.0
        self.slip = slip_bps / 10_000.0
        self.dust_price = dust_price
        self.dust_min_notional = dust_min_notional
        self.poll_every = poll_every
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max

        self.watch = {}       # conditionId -> {m, no_token, next_check, fails, last_quote}
        self.entered = set()  # conditionIds already entered

    def seed_from_gamma(self, markets: list[dict]):
        for m in markets:
            cid = m.get("conditionId")
            if not cid or cid in self.watch or cid in self.entered:
                continue
            no_token = get_no_token_id(m)
            if not no_token:
                continue
            self.watch[cid] = {
                "m": m,
                "no_token": no_token,
                "next_check": 0,
                "fails": 0,
                "last_quote": None,
            }

    def due_ids(self, now_ts: int):
        return [cid for cid, st in self.watch.items() if st["next_check"] <= now_ts and cid not in self.entered]

    def _backoff(self, fails: int) -> int:
        return min(self.backoff_base * (2 ** fails), self.backoff_max)

    def _effective_price(self, p: float) -> float:
        return p * (1.0 + self.fee + self.slip)

    def _valid_no_from_book(self, book: dict):
        """
        Walk asks from best up until our effective price cap.
        Return (takeable_notional, best_ask_price, shares_at_cap).
        """
        asks = book.get("asks") or []
        if not asks:
            return 0.0, None, 0.0

        takeable = 0.0
        shares = 0.0
        best_price = float(asks[0]["price"])

        for a in asks:
            p = float(a["price"])
            s = float(a["size"])
            if self._effective_price(p) > self.max_no_price:
                break
            # notional for NO = p * shares
            takeable += p * s
            shares += s

        if takeable <= 0:
            return 0.0, best_price, 0.0

        # “cheap dust” guard
        if best_price <= self.dust_price and takeable < self.dust_min_notional:
            return 0.0, best_price, shares

        if takeable >= self.min_notional:
            return takeable, best_price, shares
        return 0.0, best_price, shares

    def step(self, now_ts: int, fetch_book_fn, open_position_fn, bet_size_fn):
        """
        Poll due markets once; open positions if valid NO opportunity found.
        - fetch_book_fn(token_id) -> book
        - open_position_fn(cid, market, side, dollars, best_ask, book) -> bool (opened?)
        - bet_size_fn(takeable_notional) -> dollars_to_spend
        """
        opened = 0
        for cid in self.due_ids(now_ts):
            st = self.watch.get(cid)
            if not st:
                continue
            try:
                book = fetch_book_fn(st["no_token"])
                st["last_quote"] = book
                takeable, best_ask, shares = self._valid_no_from_book(book)
                if takeable > 0:
                    dollars = bet_size_fn(takeable)
                    ok = open_position_fn(cid, st["m"], "NO", dollars, best_ask, book)
                    if ok:
                        self.entered.add(cid)
                        self.watch.pop(cid, None)
                        opened += 1
                        continue  # next market

                # not valid → backoff
                st["fails"] += 1
                st["next_check"] = now_ts + self._backoff(st["fails"])

            except Exception:
                st["fails"] += 1
                st["next_check"] = now_ts + self._backoff(st["fails"])
        return opened

# --------------------------------------------------------------------
# Fast open-market fetch you already had (kept here for completeness)
# --------------------------------------------------------------------
def fetch_open_yesno_fast(limit=250, max_pages=10, days_back=90,
                          require_clob=True, min_liquidity=None, min_volume=None,
                          session=SESSION, verbose=True):
    params = {
        "limit": limit,
        "order": "startDate",
        "ascending": False,
        "closed": False,
    }
    if days_back:
        params["start_date_min"] = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
    if require_clob:
        params["enableOrderBook"] = True
    if min_liquidity is not None:
        params["liquidity_num_min"] = float(min_liquidity)
    if min_volume is not None:
        params["volume_num_min"] = float(min_volume)

    all_rows, seen_ids = [], set()
    offset, pages = 0, 0

    while pages < max_pages:
        q = dict(params, offset=offset)
        try:
            r = session.get(BASE_GAMMA, params=q, timeout=20)
            r.raise_for_status()
        except Exception as e:
            if verbose: print(f"[WARN] fetch failed at offset {offset} with {q}: {e}")
            break

        page = r.json() or []
        if not page:
            if verbose: print("[INFO] empty page; done.")
            break

        added = 0
        for m in page:
            outs = m.get("outcomes")
            if isinstance(outs, str):
                try: outs = json.loads(outs)
                except: outs = None
            if outs == ["Yes","No"]:
                mid = m.get("id")
                if mid not in seen_ids:
                    all_rows.append(m)
                    seen_ids.add(mid)
                    added += 1

        if verbose:
            print(f"[PAGE {pages}] got {len(page)} raw, added {added} new, total {len(all_rows)}")

        if len(page) < limit:
            if verbose: print("[INFO] last page (short).")
            break
        pages += 1
        offset += limit

    # sort newest → oldest by startDate or createdAt
    all_rows.sort(key=lambda m: (m.get("startDate") or m.get("createdAt") or ""), reverse=True)
    if verbose:
        print(f"✅ Total open Yes/No markets: {len(all_rows)}")
    return all_rows

def is_actively_tradable(m):
    if not m.get("enableOrderBook"):
        return False
    toks = m.get("clobTokenIds") or []
    if isinstance(toks, str):
        try: toks = json.loads(toks)
        except: toks = []
    has_quote = (m.get("bestBid") is not None) or (m.get("bestAsk") is not None)
    return bool(toks) and has_quote

# --------------------------------------------------------------------
# Minimal open_position “simulation” for live watcher (replace with yours)
# --------------------------------------------------------------------
def simulate_take_from_asks(book: dict, dollars: float, fee: float, slip: float, price_cap: float):
    """
    Simulate taking from top-of-book NO asks up to dollars (pre-fee budget).
    Returns (spent_after, shares, avg_fill_price) or (0,0,0) if nothing.
    """
    asks = book.get("asks") or []
    if not asks: return 0.0, 0.0, 0.0

    spent_pre = 0.0
    shares = 0.0
    for a in asks:
        p = float(a["price"])
        s = float(a["size"])
        if p * (1.0 + fee + slip) > price_cap:
            break
        # take as much as fits remaining budget
        max_shares_here = (dollars - spent_pre) / p
        take_shares = min(s, max_shares_here)
        if take_shares <= 0:
            break
        spent_pre += take_shares * p
        shares += take_shares
        if spent_pre >= dollars - 1e-9:
            break

    if shares <= 0:
        return 0.0, 0.0, 0.0

    spent_after = spent_pre * (1.0 + fee + slip)
    avg_price = spent_after / shares
    return spent_after, shares, avg_price

# --------------------------------------------------------------------
# MAIN: run the watcher
# --------------------------------------------------------------------
def main():
    # bankroll & config
    bank = 5000.0
    desired_bet = 100.0

    # 1) fetch open Yes/No markets quickly
    open_markets = fetch_open_yesno_fast(limit=250, max_pages=3, days_back=60, verbose=True)
    markets = [m for m in open_markets if is_actively_tradable(m)]
    print(f"Tradable Yes/No with quotes: {len(markets)}")

    # 2) seed watchlist
    mgr = WatchlistManager(
        max_no_price=0.40,    # cap on NO effective price
        min_notional=50.0,    # need at least this $ at/under cap
        fee_bps=600, slip_bps=200,
        dust_price=0.02, dust_min_notional=20.0,
        poll_every=60, backoff_base=120, backoff_max=1800
    )
    mgr.seed_from_gamma(markets)

    # helpers that close over our bank
    fee = mgr.fee
    slip = mgr.slip
    price_cap = mgr.max_no_price

    def bet_size_fn(takeable_notional):
        # spend up to desired_bet but not more than takeable notional
        return float(min(desired_bet, takeable_notional))

    def open_position_fn(cid, market, side, dollars, best_ask, book):
        nonlocal bank
        if side != "NO":
            return False
        if dollars <= 0 or bank < dollars:
            return False

        spent_after, shares, avg_price = simulate_take_from_asks(
            book, dollars, fee=fee, slip=slip, price_cap=price_cap
        )
        if shares <= 0:
            return False

        # lock capital (simple: just subtract now; you can integrate your full position store)
        bank -= spent_after
        print(f"\n✅ ENTER {side} | {market.get('question')}")
        print(f"   spent(after)={spent_after:.2f} | shares={shares:.2f} | avg_price={avg_price:.4f} | bank={bank:.2f}")
        return True

    # 3) poll loop (Ctrl+C to stop)
    try:
        while True:
            now_ts = int(time.time())
            opened = mgr.step(
                now_ts,
                fetch_book_fn=fetch_book,
                open_position_fn=open_position_fn,
                bet_size_fn=bet_size_fn
            )
            # here you could also call your settle_due_positions(..., now, ...) if you integrate live settlement
            if opened == 0:
                print(".", end="", flush=True)  # quiet heartbeat
            time.sleep(mgr.poll_every)
    except KeyboardInterrupt:
        print("\nStopped.")


from datetime import datetime, timedelta, timezone

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"

def fetch_open_yesno_fast(limit=250, max_pages=20, days_back=90,
                          require_clob=True, min_liquidity=None, min_volume=None,
                          session=SESSION, verbose=True):
    """
    Quickly fetch currently tradable Yes/No markets without walking full history.
    """
    params = {
        "limit": limit,
        "order": "startDate",           # stable ordering so offset works
        "ascending": False,             # newest first
        "closed": False,                # only open
    }
    # time window (reduce dataset)
    if days_back:
        params["start_date_min"] = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
    if require_clob:
        params["enableOrderBook"] = True
    if min_liquidity is not None:
        params["liquidity_num_min"] = float(min_liquidity)
    if min_volume is not None:
        params["volume_num_min"] = float(min_volume)

    all_rows, seen_ids = [], set()
    offset, pages = 0, 0

    while pages < max_pages:
        q = dict(params, offset=offset)
        try:
            r = session.get(BASE_GAMMA, params=q, timeout=20)
            r.raise_for_status()
        except Exception as e:
            if verbose: print(f"[WARN] fetch failed at offset {offset} with {q}: {e}")
            break

        page = r.json() or []
        if not page:
            if verbose: print("[INFO] empty page; done.")
            break

        # filter to exactly Yes/No
        added = 0
        for m in page:
            outs = m.get("outcomes")
            if isinstance(outs, str):
                try: outs = json.loads(outs)
                except Exception: outs = None
            if outs == ["Yes","No"]:
                mid = m.get("id")
                if mid not in seen_ids:
                    all_rows.append(m)
                    seen_ids.add(mid)
                    added += 1

        if verbose:
            print(f"[PAGE {pages}] got {len(page)} raw, added {added} new, total {len(all_rows)}")

        if len(page) < limit:
            if verbose: print("[INFO] last page (short).")
            break

        pages += 1
        offset += limit

    # sort locally for your pipeline
    all_rows.sort(key=lambda m: m.get("startDate") or m.get("createdAt") or "")
    if verbose:
        print(f"✅ Total open Yes/No markets: {len(all_rows)}")
    return all_rows

def is_actively_tradable(m):
    # CLOB-enabled, has token IDs, and has a visible quote
    if not m.get("enableOrderBook"):
        return False
    toks = m.get("clobTokenIds") or []
    if isinstance(toks, str):
        try: toks = json.loads(toks)
        except: toks = []
    has_quote = (m.get("bestBid") is not None) or (m.get("bestAsk") is not None)
    return bool(toks) and has_quote

#last 13
def new_markets():
    old_markets = load_markets_from_jsonl("old_live_markets.jsonl")
    old_id = []
    for id in old_markets:
        old_id.append(id["id"])
    open_markets = fetch_open_yesno_fast()
    markets = [m for m in open_markets if is_actively_tradable(m)]
    new_markets = [n for n in markets if n["id"] not in old_id]
    return new_markets


def interpret_book(market):
    toks = market.get("clobTokenIds")
    if isinstance(toks, str):
        toks = json.loads(toks)

    token = toks[0]  # first token (usually YES)
    book = fetch_book(token)

    best_bid = book["bids"][0]["price"] if book["bids"] else None
    best_ask = book["asks"][0]["price"] if book["asks"] else None

    print(f"\n📊 {market['question']}")
    print(f"Best bid: {best_bid}")
    print(f"Best ask: {best_ask}")
    print(f"Spread:   {round((float(best_ask) - float(best_bid)), 3) if best_bid and best_ask else 'N/A'}")
    print(f"Depth:    {len(book['bids'])} bids / {len(book['asks'])} asks")


def fetch_book(token_id, depth=10):
    """
    fetch book for a single market
    """
    url = f"{BASE_BOOK}?token_id={token_id}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    book = r.json()
    
    # optional: trim to depth
    book["bids"] = book.get("bids", [])[:depth]
    book["asks"] = book.get("asks", [])[:depth]
    print(book)
    return book

def value_book(book):
    """
    calculate how much value can be taken
    """

def decide_book(bookvalue):
    """
    given value book, decide 
    """

def fetch_old(filename):
    result = []
    # ensure parent folder exists
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
    with open(filename, "r", encoding="utf-8") as file:
        # if file already has content, add newline first
        for line in file:
            result.append(line["id"])
    return result

def fech_file(filename):
    result = []
    # ensure parent folder exists
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
    with open(filename, "r", encoding="utf-8") as file:
        # if file already has content, add newline first
        for line in file:
            result.append(line)
    return result

def append_markets_to_file(filename, data, key_field="id"):
    """
    Append markets to a .jsonl file, skipping duplicates based on `key_field`.
    Each line is one JSON object.
    """
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)

    # --- load existing keys to skip duplicates ---
    existing_keys = set()
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    if key_field in record:
                        existing_keys.add(record[key_field])
                except json.JSONDecodeError:
                    continue

    # --- append only new items ---
    added = 0
    with open(filename, "a", encoding="utf-8") as f:
        for market in data:
            key = market.get(key_field)
            if not key or key in existing_keys:
                continue  # skip duplicates or invalid
            f.write(json.dumps(market, ensure_ascii=False) + "\n")
            existing_keys.add(key)
            added += 1

    print(f"✅ Added {added} new markets (skipped {len(data)-added} duplicates) to {filename}")

def load_markets_from_jsonl(filename):
    markets = []
    if not os.path.exists(filename):
        return markets
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                markets.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return markets

def save_line_to_file(filename, data):
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