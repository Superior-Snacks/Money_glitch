#!/usr/bin/env python3
"""
Trades-Only Market Scanner (slow + single-file state)

What it does
------------
- Prompts for a start time (ISO like '2025-10-01T00:00:00Z' OR relative like 'hours=6', 'days=2').
- Fetches all open Yes/No Polymarket markets.
- For each market, SLOWLY pages trade history back to the start time.
- Computes per-market stats from *trades only*:
    * min_trade_px_raw (lowest observed raw price; NO-only if available, else all prints)
    * min_trade_px_inc (with fee/slip multiplier)
    * per budget: the *tightest price interval (spread)* whose dollar liquidity >= budget
      + VWAP for that interval (raw & inc-fee)
- Maintains a single JSON file on disk (`trades_only_state.json`) with a dict:
    { market_id: { question, slug, updated_at, min_trade_px_raw, min_trade_px_inc, budgets{...}, under_cap: bool } }
- After completing a full pass over markets, prints a report and sleeps before next pass.

Notes
-----
- “Under cap” is evaluated as (min_trade_px_inc <= CAP_INC_FEE).
- This is a lower-bound view of achievable prices (from actual prints), not current book liquidity.
- API paging:
    * Primary: `starting_before` (older-than timestamp in seconds) if supported.
    * Fallback: `offset` paging.
- Tuned for low RPS. Increase delays if you want to be gentler.
"""

import json, time, os, math, random, re, gzip, glob, sys
from datetime import datetime, timezone, timedelta
from collections import deque
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from email.utils import parsedate_to_datetime

# ----------------- Config -----------------
BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
DATA_TRADES  = "https://data-api.polymarket.com/trades"

name_log = input("name log: ")
if name_log:
    STATE_PATH = os.path.join("logs", name_log)
else:
    STATE_PATH = os.path.join("logs", f"trades_only_state.json")

# Fee/slip as FRACTIONS (not bps)
FEE  = 0.00
SLIP = 0.00
FEE_MULT = 1.0 + FEE + SLIP

# Inc-fee CAP used to mark markets "under_cap"
CAP_INC_FEE = 0.70

# Budgets to evaluate (USD notional, using raw trade prices in the window)
TARGET_BUDGETS = [5, 10, 50, 100, 200]

# Pull cadence
MISS_LIMIT_TO_CLOSE = 3  # how many consecutive refreshes a market can be "missing" before we mark it closed
MARKET_REFRESH_SEC    = 5 * 60      # was 60; fewer full reloads, less churn
SLEEP_BETWEEN_PAGES   = 0.0         # let the rate limiter do the pacing
SLEEP_BETWEEN_MARKETS = 0.0         # ^
SLEEP_AFTER_FULL_PASS = 60.0        # shorter idle between passes

RPS_TARGET            = 3.5         # was 1.0; ~3–4 req/s is still gentle
SAVE_EVERY            = 200         # was 25; far fewer disk writes per pass
# --- adaptive RPS control (drop-in) ---
RPS_TARGET = 3.5      # your desired peak
_RPS_SCALE = 1.0      # auto-tuned 0.3–1.0
_RPS_MIN   = 0.3
_RPS_RECOVER_PER_SEC = 0.03  # slow healing toward target

_last_tokens_ts = time.monotonic()
_bucket = RPS_TARGET

def _rate_limit():
    """Token-bucket using current adaptive RPS."""
    global _last_tokens_ts, _bucket
    now = time.monotonic()
    rps_now = max(RPS_TARGET * _RPS_SCALE, 0.1)
    _bucket = min(rps_now, _bucket + (now - _last_tokens_ts) * rps_now)
    _last_tokens_ts = now
    if _bucket < 1.0:
        need = (1.0 - _bucket) / rps_now
        time.sleep(need)
        now2 = time.monotonic()
        _bucket = min(rps_now, _bucket + (now2 - _last_tokens_ts) * rps_now)
        _last_tokens_ts = now2
    _bucket -= 1.0

def _rps_on_429():
    """Cut RPS scale when we get rate limited."""
    global _RPS_SCALE
    _RPS_SCALE = max(_RPS_MIN, _RPS_SCALE * 0.7)  # 30% haircut

def _rps_recover(dt_sec: float):
    """Recover slowly on each successful request."""
    global _RPS_SCALE
    _RPS_SCALE = min(1.0, _RPS_SCALE + _RPS_RECOVER_PER_SEC * dt_sec)

def _retry_after_seconds(resp) -> float:
    ra = resp.headers.get("Retry-After")
    if not ra:
        return 0.0
    # numeric seconds?
    try:
        return max(0.0, float(ra))
    except Exception:
        pass
    # HTTP-date (e.g., "Fri, 24 Oct 2025 12:15:40 GMT")
    try:
        dt = parsedate_to_datetime(ra)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
    except Exception:
        return 0.0

def http_get_with_backoff(url, *, params=None, timeout=20, max_tries=8):
    """
    Centralized GET that cooperates with _rate_limit(), honors 429 Retry-After,
    does exponential backoff with jitter, and nudges RPS up on success.
    """
    back = 0.5
    tries = 0
    last_t = time.monotonic()
    while True:
        _rate_limit()
        try:
            r = SESSION.get(url, params=params or {}, timeout=timeout)
        except requests.RequestException as e:
            # network/timeout -> backoff
            time.sleep(back + random.random()*0.2)
            back = min(back*1.7, 20.0)
            tries += 1
            if tries >= max_tries:
                raise
            continue

        # Success path
        if r.status_code < 400:
            # gentle recovery of RPS scale
            now = time.monotonic()
            _rps_recover(now - last_t)
            last_t = now
            return r

        # 429: honor Retry-After, cut RPS, backoff
        if r.status_code == 429:
            _rps_on_429()
            ra = _retry_after_seconds(r)
            sleep_s = max(ra, back) + random.random()*0.3
            time.sleep(sleep_s)
            back = min(back*1.8, 30.0)
            tries += 1
            if tries >= max_tries:
                r.raise_for_status()
            continue

        # 5xx: exponential backoff
        if 500 <= r.status_code < 600:
            time.sleep(back + random.random()*0.2)
            back = min(back*1.7, 20.0)
            tries += 1
            if tries >= max_tries:
                r.raise_for_status()
            continue

        # other 4xx: just raise
        r.raise_for_status()
# ----------------- HTTP session -----------------
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429,500,502,503,504),
        allowed_methods=("GET","POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=40, pool_maxsize=40)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "pm-trades-only-slow/1.0"})
    return s

SESSION = make_session()

# ----------------- Time parsing -----------------
def parse_start_input(user_input: str) -> int:
    """
    Accepts:
      - ISO 8601 e.g. '2025-10-01T00:00:00Z' or '2025-10-01 00:00:00'
      - Relative like 'hours=6', 'days=2', 'minutes=30'
    Returns epoch seconds UTC.
    """
    s = user_input.strip()
    # relative?
    if "=" in s and all(k in s for k in ["="]):
        parts = dict(
            (k.strip(), float(v))
            for k,v in (p.split("=",1) for p in s.split(","))
        )
        delta = timedelta(
            days=parts.get("days", 0.0),
            hours=parts.get("hours", 0.0),
            minutes=parts.get("minutes", 0.0)
        )
        t = datetime.now(timezone.utc) - delta
        return int(t.timestamp())

    # ISO variants
    s2 = s.replace("Z","+00:00")
    try:
        t = datetime.fromisoformat(s2).astimezone(timezone.utc)
        return int(t.timestamp())
    except Exception:
        print("Could not parse time. Try ISO like 2025-10-01T00:00:00Z or relative like hours=6")
        sys.exit(1)

def dt_iso():
    return datetime.now(timezone.utc).isoformat()

# ----------------- Market + Trades fetch -----------------
def _parse_iso_utc_epoch(s):
    if not s: return None
    try:
        return int(datetime.fromisoformat(str(s).replace("Z","+00:00"))
                   .astimezone(timezone.utc).timestamp())
    except Exception:
        return None

def _market_time_epoch(m):
    # prefer createdAt, else startDate, else endDate/closedTime as a fallback
    for k in ("createdAt", "startDate", "endDate", "closedTime", "resolveTime", "resolvedTime"):
        ts = _parse_iso_utc_epoch(m.get(k))
        if ts: return ts
    return None  # last resort: treat as unknown/new

def fetch_yesno_markets_full(since_epoch: int, page_limit=250, include_closed=True):
    def _pull(closed_flag):
        seen, out = set(), []
        offset = 0
        while True:
            params = {
                "limit": page_limit,
                "order": "startDate",
                "ascending": False,
                "enableOrderBook": True,
                "closed": closed_flag,
                "offset": offset,
            }
            r = http_get_with_backoff(BASE_GAMMA, params=params, timeout=30)
            data = r.json() or []
            if not data:
                break

            # keep only yes/no binaries and de-dupe by conditionId/id
            added = 0
            oldest_on_page = None
            for m in data:
                outs = m.get("outcomes")
                if isinstance(outs, str):
                    try: outs = json.loads(outs)
                    except: outs = None
                if not (isinstance(outs, list) and len(outs) == 2 and
                        {str(x).strip().lower() for x in outs} == {"yes","no"}):
                    continue

                key = m.get("conditionId") or m.get("id")
                if not key or key in seen:
                    # still track oldest_on_page even if we skip the row
                    mt = _market_time_epoch(m)
                    if mt is not None:
                        oldest_on_page = mt if oldest_on_page is None else min(oldest_on_page, mt)
                    continue

                out.append(m); seen.add(key); added += 1

                mt = _market_time_epoch(m)
                if mt is not None:
                    oldest_on_page = mt if oldest_on_page is None else min(oldest_on_page, mt)

            # stop if this page is already older than our start time OR last page is short
            if len(data) < page_limit:
                break
            if oldest_on_page is not None and oldest_on_page < since_epoch:
                break

            offset += page_limit
            time.sleep(0.5)  # be polite
        return out

    mkts = _pull(False)
    if include_closed:
        mkts += _pull(True)

    # final de-dupe, prefer entries that actually have conditionId
    by_key = {}
    for m in mkts:
        key = m.get("conditionId") or m.get("id")
        if not key: continue
        if key not in by_key or (by_key[key].get("conditionId") is None and m.get("conditionId")):
            by_key[key] = m
    # sort newest → oldest just for consistent logging
    return sorted(by_key.values(), key=_market_time_epoch or (lambda _: 0), reverse=True)

def fetch_trades_page_ms(cid: str, limit=250, starting_before_s=None, offset=None):
    params = {"market": cid, "sort": "desc", "limit": int(limit)}
    if starting_before_s is not None:
        params["starting_before"] = int(starting_before_s * 1000)  # ms
    if offset is not None:
        params["offset"] = int(offset)
    r = http_get_with_backoff(DATA_TRADES, params=params, timeout=20)
    return r.json() or []

def fetch_all_trades_since_slow(cid: str, since_epoch_s: int, page_limit=250, slow_delay=SLEEP_BETWEEN_PAGES):
    """
    Backfills slowly to since_epoch (seconds). Timestamp-paging first, then offset.
    De-duplicates trades.
    """
    out = []
    starting_before = None
    last_len = None

    # timestamp-paging (preferred)
    for _ in range(10_000):
        data = fetch_trades_page_ms(cid, limit=page_limit, starting_before_s=starting_before)
        if not data:
            break
        out.extend(data)

        # compute oldest seen (seconds)
        oldest_ts = min(_to_epoch(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in data)
        if oldest_ts <= since_epoch_s:
            break

        starting_before = oldest_ts  # keep seconds here; ms conversion happens in fetch_trades_page_ms
        if slow_delay:
            time.sleep(slow_delay)

        if last_len == len(data):   # no progress, stop timestamp pass
            break
        last_len = len(data)

    # fallback: offset paging if we didn't reach since_epoch
    if (not out) or (min((_to_epoch(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0) for t in out) > since_epoch_s):
        offset = 0
        for _ in range(2000):
            data = fetch_trades_page_ms(cid, limit=page_limit, offset=offset)
            if not data:
                break
            out.extend(data)
            oldest_ts = min(_to_epoch(t.get("timestamp") or t.get("time") or t.get("ts") or 0) or 0 for t in data)
            if oldest_ts <= since_epoch_s:
                break
            offset += len(data)
            if slow_delay:
                time.sleep(slow_delay)

    # de-dup
    seen, uniq = set(), []
    for t in out:
        key = t.get("id") or (t.get("timestamp"), t.get("price"), t.get("size"), t.get("side"), t.get("outcome"))
        if key in seen: 
            continue
        seen.add(key); uniq.append(t)
    return uniq

def process_one_market(state: dict, m: dict, since_epoch_s: int, cap_inc_fee: float, fee_mult: float) -> bool:
    """
    Handles a single market:
      - extracts fields,
      - fetches trades (if conditionId present),
      - trims post-resolution drift for YES winners,
      - computes stats,
      - updates state in-memory.
    Returns True if state was updated.
    """
    fields = safe_market_fields(m)
    cid = fields["cid"]
    question, slug = fields["question"], fields["slug"]
    closed, winner, res_ts = fields["closed"], fields["winner"], fields["resolved_at"]

    # If no conditionId, record minimal info and skip trades (counts in reports)
    if not cid:
        key = m.get("id") or f"legacy:{random.randint(1, 1_000_000)}"
        state[key] = {
            "market_id": key,
            "question": question,
            "slug": slug,
            "updated_at": dt_iso(),
            "status": "closed" if closed else "open",
            "winner": winner,
            "resolved_at": res_ts,
            "no_trades": True,
            "note": "missing conditionId; skipped trade fetch"
        }
        return True

    # Fetch trades slowly back to since_epoch
    trades = fetch_all_trades_since_slow(cid, since_epoch_s)

    # For closed YES winners, cut post-resolution drift (yes→$1, no→$0 drift after resolve)
    trades_for_stats = trades
    if closed and winner == "YES" and res_ts:
        trimmed = []
        for t in trades:
            tts = _to_epoch(t.get("timestamp") or t.get("time") or t.get("ts"))
            if tts is not None and tts <= res_ts:
                trimmed.append(t)
        trades_for_stats = trimmed

    stats = compute_stats_from_trades(trades_for_stats)
    entry = {
        "market_id": cid,
        "question": question,
        "slug": slug,
        "updated_at": dt_iso(),
        "status": "closed" if closed else "open",
        "winner": winner,
        "resolved_at": res_ts,
    }

    if stats:
        entry.update({
            "min_trade_px_raw": stats["min_trade_px_raw"],
            "min_trade_px_inc": stats["min_trade_px_inc"],
            "budgets": stats["budgets"],
            "under_cap": (stats["min_trade_px_inc"] <= cap_inc_fee)
        })
    else:
        entry.update({
            "no_trades": True
        })

    state[cid] = {**state.get(cid, {}), **entry}
    return True

def save_state_periodic(state: dict, counter: int, every: int, path=STATE_PATH):
    """Save to disk only every N updates to reduce I/O."""
    if counter % every == 0:
        save_state(state, path)

def tally_groups(state: dict, cap_inc_fee: float):
    """Build grouped counts for report."""
    groups = {
        "open": {"under":0, "over":0, "unknown":0, "count":0},
        "closed_yes": {"under":0, "over":0, "unknown":0, "count":0},
        "closed_no":  {"under":0, "over":0, "unknown":0, "count":0},
        "all": {"under":0, "over":0, "unknown":0, "count":0},
    }

    for row in state.values():
        px = row.get("min_trade_px_inc")
        status = (row.get("status") or "open").lower()
        win = (row.get("winner") or "").upper()

        if status == "closed" and win == "YES":
            bucket = "closed_yes"
        elif status == "closed" and win == "NO":
            bucket = "closed_no"
        elif status == "closed":
            bucket = None
        else:
            bucket = "open"

        def _tally(bn):
            g = groups[bn]
            g["count"] += 1
            if px is None:
                g["unknown"] += 1
            elif float(px) <= float(cap_inc_fee) + 1e-12:
                g["under"] += 1
            else:
                g["over"] += 1

        if bucket:
            _tally(bucket)
        _tally("all")

    return groups

# ----------------- Analytics -----------------
def cap_for_raw(raw, fee_bps, slip_bps):
    return raw * (1 + fee_bps/10000 + slip_bps/10000)

def _to_epoch(x):
    try:
        f = float(x)
        return int(f/1000) if f > 1e12 else int(f)
    except Exception:
        return None

def tighten_intervals_min_spread_for_budget(trades, budget_usd):
    """
    trades: list of (price, size) RAW (no fee)
    Return (spread, lo, hi, dollars, shares, vwap_raw) for the *narrowest* price interval
    whose Σ(price*size) >= budget_usd. Two-pointer on price-sorted trades.
    """
    pts = sorted(((float(p), float(s)) for p,s in trades if p>0 and s>0), key=lambda x: x[0])
    if not pts:
        return None
    n = len(pts)
    best = None
    j = 0
    dollars = 0.0
    shares  = 0.0
    for i in range(n):
        while j < n and dollars < budget_usd:
            p,s = pts[j]; dollars += p*s; shares += s; j += 1
        if dollars >= budget_usd:
            lo, _ = pts[i]; hi, _ = pts[j-1]
            spread = hi - lo
            if (best is None) or (spread < best[0]):
                best = (spread, i, j-1, dollars, shares)
        # remove i from window
        p,s = pts[i]; dollars -= p*s; shares -= s
    if not best:
        return None
    spread, i0, j0, dollars, shares = best
    lo = pts[i0][0]; hi = pts[j0][0]
    vwap_raw = (sum(p*s for p,s in pts[i0:j0+1]) /
                max(1e-12, sum(s for _,s in pts[i0:j0+1])))
    return (spread, lo, hi, dollars, shares, vwap_raw)

def compute_stats_from_trades(trades):
    """
    trades: raw trade dicts. Prefer NO-only; fallback to all.
    Returns dict with min prices and per-budget interval metrics.
    """
    tuples_no = [(float(t.get("price",0)), float(t.get("size",0)))
                 for t in trades
                 if str(t.get("outcome","")).lower()=="no" and float(t.get("price",0))>0 and float(t.get("size",0))>0]
    tuples_all = [(float(t.get("price",0)), float(t.get("size",0)))
                  for t in trades
                  if float(t.get("price",0))>0 and float(t.get("size",0))>0]
    tuples = tuples_no if tuples_no else tuples_all
    if not tuples:
        return None

    min_px = min(p for p,_ in tuples)
    res = {
        "min_trade_px_raw": round(min_px, 6),
        "min_trade_px_inc": round(min_px * FEE_MULT, 6),
        "budgets": {}
    }
    for B in TARGET_BUDGETS:
        win = tighten_intervals_min_spread_for_budget(tuples, B)
        if win is None:
            res["budgets"][str(B)] = None
        else:
            spread, lo, hi, dollars, shares, vwap_raw = win
            res["budgets"][str(B)] = {
                "min_spread_raw": round(spread, 6),
                "lo": round(lo, 6),
                "hi": round(hi, 6),
                "vwap_raw": round(vwap_raw, 6),
                "vwap_inc": round(vwap_raw * FEE_MULT, 6),
                "covered_dollars": round(dollars, 2),
                "covered_shares": round(shares, 6),
            }
    return res

def _parse_iso_utc(s):
    if not s:
        return None
    try:
        return int(datetime.fromisoformat(str(s).replace("Z","+00:00"))
                   .astimezone(timezone.utc).timestamp())
    except Exception:
        return None

def winner_from_market(m):
    # Try common keys Polymarket (or mirrors) expose
    for k in ["winningOutcome", "resolvedOutcome", "winner", "resolveOutcome", "resolution", "outcome"]:
        v = m.get(k)
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("yes", "no"):
                return s.upper()
    # Some payloads embed a result object
    res = m.get("result") or m.get("resolutionData") or {}
    if isinstance(res, dict):
        for k in ["winner", "winningOutcome", "outcome"]:
            v = res.get(k)
            if isinstance(v, str) and v.strip().lower() in ("yes", "no"):
                return v.strip().upper()
    return None

def resolved_ts_from_market(m):
    # Prefer explicit resolved time, else closed/end dates as fallback
    for k in ["resolvedTime", "resolveTime", "resolutionTime", "closedTime", "endDate", "closeTime"]:
        ts = _parse_iso_utc(m.get(k))
        if ts:
            return ts
    return None

def is_closed_market(m):
    if m.get("closed") is True:  # some feeds send a boolean
        return True
    # treat as closed if we can see a winner
    return winner_from_market(m) is not None

def safe_market_fields(m: dict):
    """Extract commonly used fields safely."""
    cid = m.get("conditionId")
    mid = m.get("id")
    question = m.get("question")
    slug = m.get("slug") or m.get("market_slug")

    # winner / closed / resolved time helpers
    def _parse_iso_utc(s):
        if not s: return None
        try:
            return int(datetime.fromisoformat(str(s).replace("Z","+00:00"))
                       .astimezone(timezone.utc).timestamp())
        except Exception:
            return None

    def _winner(m):
        for k in ["winningOutcome", "resolvedOutcome", "winner", "resolveOutcome", "resolution", "outcome"]:
            v = m.get(k)
            if isinstance(v, str) and v.strip().lower() in ("yes","no"):
                return v.strip().upper()
        res = m.get("result") or m.get("resolutionData") or {}
        if isinstance(res, dict):
            for k in ["winner", "winningOutcome", "outcome"]:
                v = res.get(k)
                if isinstance(v, str) and v.strip().lower() in ("yes","no"):
                    return v.strip().upper()
        return None

    def _resolved_ts(m):
        for k in ["resolvedTime", "resolveTime", "resolutionTime", "closedTime", "endDate", "closeTime"]:
            ts = _parse_iso_utc(m.get(k))
            if ts: return ts
        return None

    win = _winner(m)
    closed = bool(m.get("closed") is True or win is not None)
    res_ts = _resolved_ts(m)

    return {
        "cid": cid,
        "mid": mid,
        "question": question,
        "slug": slug,
        "winner": win,         # "YES" | "NO" | None
        "closed": closed,
        "resolved_at": res_ts  # epoch seconds or None
    }

# ----------------- State I/O -----------------
def load_state(path=STATE_PATH):
    if not os.path.exists(path):
        return {}
    state = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                mid = rec.get("market_id")
                if mid:
                    state[mid] = rec
            except Exception:
                continue
    return state

def save_state(state, path=STATE_PATH):
    """Rewrites the entire state dict as JSONL (1 line per market)."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for rec in state.values():
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    os.replace(tmp, path)

def append_state_delta(state_delta: dict, path):
    """Append new/updated market(s) without rewriting the full file."""
    with open(path, "a", encoding="utf-8") as f:
        for rec in state_delta.values():
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def compact_state(path=STATE_PATH):
    seen = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                mid = rec.get("market_id")
                if mid:
                    seen[mid] = rec  # last line wins
            except Exception:
                continue
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as out:
        for rec in seen.values():
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
    os.replace(tmp, path)

def compact_state_gz(path=STATE_PATH):
    import gzip
    seen = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                mid = rec.get("market_id")
                if mid:
                    seen[mid] = rec
            except Exception:
                continue
    gz_path = path + ".gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as out:
        for rec in seen.values():
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"[COMPACT] wrote {len(seen)} markets → {gz_path}")

def mark_open(state, cid, now_iso_str):
    st = state.setdefault(cid, {})
    st["status"] = "open"
    st["misses"] = 0
    st["last_seen_open_at"] = now_iso_str

def mark_closed_if_missing(state, open_ids, now_iso_str):
    """
    For any market in state that's not in `open_ids`, increment a 'misses' counter.
    If it reaches MISS_LIMIT_TO_CLOSE, mark status='closed' and set closed_at (once).
    """
    for cid, st in state.items():
        if st.get("status") == "closed":
            continue
        if cid not in open_ids:
            st["misses"] = int(st.get("misses", 0)) + 1
            if st["misses"] >= MISS_LIMIT_TO_CLOSE:
                st["status"] = "closed"
                st.setdefault("closed_at", now_iso_str)
        else:
            # is open this cycle; make sure it's reset (defensive)
            st["misses"] = 0

# ----------------- Main loop -----------------
def main():
    # ask when to start
    user = input("Start time (ISO like 2025-10-01T00:00:00Z or relative like hours=6): ").strip()
    since_epoch = parse_start_input(user)
    print(f"→ scanning trades since {datetime.fromtimestamp(since_epoch, tz=timezone.utc).isoformat()}")

    state = load_state()
    markets = []
    last_market_pull = 0
    updates = 0  # total updates across runs

    while True:
        now_s = int(time.time())

        # refresh full yes/no market list (open + closed) back to since_epoch
        if (now_s - last_market_pull >= MARKET_REFRESH_SEC) or not markets:
            try:
                markets = fetch_yesno_markets_full(since_epoch, page_limit=250, include_closed=True)
                last_market_pull = now_s
                print(f"[MKT] loaded {len(markets)} markets back to start @ {dt_iso()}")
                open_ids = {m.get("conditionId") or m.get("id") for m in markets if not is_closed_market(m)}
                mark_closed_if_missing(state, open_ids, dt_iso())
                save_state(state)  # quick persist after refresh
            except Exception as e:
                print(f"[WARN markets] {e}; keeping previous list")
                time.sleep(5)

        # ======= WIRED-UP PER-PASS SECTION =======
        updates_this_pass = 0
        for idx, m in enumerate(markets, 1):
            # optional progress ping
            if idx % 200 == 0:
                print(f"[{idx}/{len(markets)}] {dt_iso()}")

            try:
                if process_one_market(state, m, since_epoch, CAP_INC_FEE, FEE_MULT):
                    updates += 1
                    updates_this_pass += 1
                    save_state_periodic(state, updates, SAVE_EVERY)
            except Exception as e:
                cid_dbg = (m.get("conditionId") or m.get("id") or "unknown")[:10]
                print(f"[WARN {cid_dbg}] {e}")
                time.sleep(2)

            # be gentle between markets
            time.sleep(SLEEP_BETWEEN_MARKETS)

        # end of pass → final save
        save_state(state)
        # ======= END WIRED-UP SECTION =======

        # Build and print grouped report from entire DB
        groups = tally_groups(state, CAP_INC_FEE)
        print("===== PASS REPORT =====")
        print(f"Time:                     {dt_iso()}")
        print(f"Markets in DB:            {len(state)}")
        print("")
        print(f"OPEN         → total:{groups['open']['count']:4d}  under:{groups['open']['under']:4d}  over:{groups['open']['over']:4d}  unknown:{groups['open']['unknown']:4d}")
        print(f"CLOSED (YES) → total:{groups['closed_yes']['count']:4d}  under:{groups['closed_yes']['under']:4d}  over:{groups['closed_yes']['over']:4d}  unknown:{groups['closed_yes']['unknown']:4d}")
        print(f"CLOSED (NO)  → total:{groups['closed_no']['count']:4d}   under:{groups['closed_no']['under']:4d}   over:{groups['closed_no']['over']:4d}   unknown:{groups['closed_no']['unknown']:4d}")
        print("—")
        print(f"ALL          → total:{groups['all']['count']:4d}  under:{groups['all']['under']:4d}  over:{groups['all']['over']:4d}  unknown:{groups['all']['unknown']:4d}")
        print("=======================")

        # rest before starting again
        time.sleep(SLEEP_AFTER_FULL_PASS)
# ------------- utils -------------
def _to_epoch_any(ts):
    try:
        x = float(ts); return int(x/1000) if x>1e12 else int(x)
    except Exception: return None

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nbye.")