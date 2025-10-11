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
import random, time
import os, json, time, math
from datetime import datetime, timedelta, timezone

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
    outs = market.get("outcomes")
    if isinstance(outs, str):
        try:
            outs = json.loads(outs)
        except Exception:
            outs = None

    toks = market.get("clobTokenIds") or []
    if isinstance(toks, str):
        try:
            toks = json.loads(toks)
        except Exception:
            toks = []

    if not outs or len(outs) != 2 or len(toks) != 2:
        return None

    # Normalize outcome names
    o0 = str(outs[0]).strip().lower()
    o1 = str(outs[1]).strip().lower()

    # exact Yes/No ordering
    if (o0, o1) == ("yes", "no"):
        return toks[1]
    if (o0, o1) == ("no", "yes"):
        return toks[0]

    # If they’re synonyms/casey, fall back to matching the word "no"
    if "no" in o0 and "yes" in o1:
        return toks[0]
    if "yes" in o0 and "no" in o1:
        return toks[1]

    # Last-resort fallback: assume second token is NO (often true)
    return toks[1]

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