#!/usr/bin/env python3
# prod_cover_100.py
#
# Goal: enter up to 100 YES/NO markets with $1 NO each,
# as cheaply as possible, ASAP after market creation.
# Strategy:
#   - Compute an EV-based effective cap (defaults to 0.70*(1-0.01)=0.693).
#   - Derive a pre-fee cap from fees; post a post-only NO bid at:
#         quote_px = min(pre_fee_cap, best_bid + TICK)
#     (rounded to tick).
#   - If we see a better ask (<= pre-fee cap), take it immediately for $1.
#   - Maintain at most one order per market; cancel/replace if quote target changes.
#   - Stop after 100 markets “entered”.
#
# Live order *stubs* are provided; set DRY_RUN=False after wiring your account.

import os, json, time, random, math, signal, uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Config ----------
# Endpoints
GAMMA_URL = "https://gamma-api.polymarket.com/markets"
BOOK_URL  = "https://clob.polymarket.com/book"

# Run controls
DRY_RUN = True               # set False after wiring order functions
TARGET_MARKETS = 100
PER_MARKET_USD = 1.00

# Discovery cadence
RESEED_SEC = 10              # check for new markets every 10s
DAYS_BACK  = 0.08            # ~2 hours
MAX_CHECKS_PER_TICK = 500
TICK_SLEEP_ACTIVE = 0.15
TICK_SLEEP_IDLE   = 1.5

# Fees & EV
P_NO_BASE   = 0.70           # your historical rate
SETTLE_FEE  = 0.01           # winner fee
ENTRY_FEE   = 0.06           # trading fee (assume taker==maker fee here)
ENTRY_SLIP  = 0.00           # we treat maker as no slippage; taker on $1 is negligible
TICK        = 0.01

# Risk limits (paranoid)
GLOBAL_BANK_LIMIT = 5000.0   # hard cap exposure (should never hit with $1 x 100 = $100)
RATE_LIMIT_RPS     = 10.0
HTTP_TIMEOUT       = 12

# ---------- HTTP + RPS ----------
def session():
    s = requests.Session()
    retry = Retry(total=5, connect=5, read=5, backoff_factor=0.5,
                  status_forcelist=(429,500,502,503,504),
                  allowed_methods=("GET","POST"))
    ad = HTTPAdapter(max_retries=retry, pool_connections=40, pool_maxsize=40)
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update({"User-Agent": "prod-cover-100/1.0"})
    return s

S = session()
_last = time.monotonic()
bucket = RATE_LIMIT_RPS

def rps():
    global _last, bucket
    now = time.monotonic()
    bucket = min(RATE_LIMIT_RPS, bucket + (now - _last)*RATE_LIMIT_RPS)
    _last = now
    if bucket < 1.0:
        time.sleep((1.0 - bucket)/RATE_LIMIT_RPS)
        now2 = time.monotonic()
        bucket = min(RATE_LIMIT_RPS, bucket + (now2 - _last)*RATE_LIMIT_RPS)
        _last = now2
    bucket -= 1.0

# ---------- Helpers ----------
def now_iso(): return datetime.now(timezone.utc).isoformat()

def dt(s):
    try:
        return datetime.fromisoformat(str(s).replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def ev_effective_cap(p_no=P_NO_BASE, settle=SETTLE_FEE):
    # p_eff <= p_no * (1 - settle)
    return p_no * (1.0 - settle)

def pre_fee_cap(eff_cap, entry_fee=ENTRY_FEE, slip=ENTRY_SLIP):
    mult = 1.0 + entry_fee + slip
    # pre_fee * mult <= eff_cap
    return max(0.01, eff_cap / mult - 1e-6)

def tick_round(px):
    return max(0.01, round(px / TICK) * TICK)

def best_of_book(book):
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    bb = max((float(b["price"]) for b in bids if "price" in b), default=None)
    ba = min((float(a["price"]) for a in asks if "price" in a), default=None)
    return bb, ba

def parse_outcomes(m):
    outs = m.get("outcomes"); toks = m.get("clobTokenIds")
    if isinstance(outs,str):
        try: outs=json.loads(outs)
        except: outs=None
    if isinstance(toks,str):
        try: toks=json.loads(toks)
        except: toks=None
    if isinstance(outs,list) and isinstance(toks,list) and len(outs)==2 and len(toks)==2:
        o0,o1=str(outs[0]).upper(),str(outs[1]).upper()
        if o0=="YES" and o1=="NO": return {"YES":toks[0],"NO":toks[1]}
        if o0=="NO"  and o1=="YES": return {"YES":toks[1],"NO":toks[0]}
        if "NO" in o0 and "YES" in o1: return {"YES":toks[1],"NO":toks[0]}
        if "YES" in o0 and "NO" in o1: return {"YES":toks[0],"NO":toks[1]}
    if isinstance(toks,list) and len(toks)==2:
        return {"YES":toks[0],"NO":toks[1]}
    return None

# ---------- Data fetch ----------
def fetch_open_yesno(limit=250, max_pages=1, days_back=DAYS_BACK):
    out, seen = [], set()
    offset = 0
    for _ in range(max_pages):
        rps()
        q = {
            "limit": limit,
            "order": "startDate",
            "ascending": False,
            "closed": False,
            "enableOrderBook": True,
            "start_date_min": (datetime.now(timezone.utc)-timedelta(days=days_back)).isoformat()
        }
        q["offset"] = offset
        r = S.get(GAMMA_URL, params=q, timeout=HTTP_TIMEOUT); r.raise_for_status()
        page = r.json() or []
        if not page: break
        added=0
        for m in page:
            if m.get("enableOrderBook") is not True: continue
            mm = parse_outcomes(m)
            if not mm: continue
            mid = m.get("id") or m.get("conditionId")
            if mid in seen: continue
            out.append(m); seen.add(mid); added+=1
        if len(page) < limit: break
        offset += limit
    out.sort(key=lambda m: (m.get("startDate") or m.get("createdAt") or ""), reverse=True)
    return out

def fetch_book(token_id: str, depth: int = 60):
    rps()
    r = S.get(BOOK_URL, params={"token_id": token_id}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    book = r.json() or {}
    book["bids"] = (book.get("bids") or [])[:depth]
    book["asks"] = (book.get("asks") or [])[:depth]
    return book

# ---------- LIVE ORDER STUBS ----------
# You must implement these with your signing/auth for the Polymarket CLOB.
# Keep secrets in env vars; do not hardcode.
def create_order(*, token_id: str, side: str, price: float, size_shares: float,
                 post_only: bool = True, client_id: Optional[str] = None) -> dict:
    """
    Return {'ok': True, 'order_id': '...'} on success; otherwise {'ok': False, 'error': '...'}.
    side: 'buy' for NO (buy NO shares), 'sell' to close.
    """
    if DRY_RUN:
        return {"ok": True, "order_id": client_id or f"dry_{uuid.uuid4()}"}
    # TODO: sign + POST to CLOB /orders endpoint with post_only flag.
    raise NotImplementedError("Wire create_order() to your account")

def cancel_order(order_id: str) -> dict:
    if DRY_RUN:
        return {"ok": True}
    # TODO: sign + DELETE /orders/{order_id}
    raise NotImplementedError("Wire cancel_order() to your account")

def get_my_open_orders(token_id: Optional[str] = None) -> List[dict]:
    if DRY_RUN:
        return []
    # TODO: GET /orders?status=open&token_id=...
    raise NotImplementedError("Wire get_my_open_orders() to your account")

# ---------- Engine ----------
class MarketState:
    __slots__ = ("cid","question","slug","no_token","created_ts",
                 "order_id","order_px","entered","last_checked")
    def __init__(self, cid, question, slug, no_token, created_ts):
        self.cid = cid; self.question = question; self.slug = slug
        self.no_token = no_token
        self.created_ts = created_ts
        self.order_id = None          # live order id (one per market)
        self.order_px = None
        self.entered = False          # set True once $1 fills (taker or maker)
        self.last_checked = 0

class Cover100:
    def __init__(self):
        self.eff_cap = ev_effective_cap(P_NO_BASE, SETTLE_FEE)       # ≈0.693
        self.pre_cap = pre_fee_cap(self.eff_cap, ENTRY_FEE, ENTRY_SLIP)
        self.markets: Dict[str, MarketState] = {}
        self.entered_count = 0
        self.bank_spent = 0.0
        self.stop = False

    # quoting rule for NO:
    #   quote at min(pre_cap, best_bid + tick) rounded
    def target_no_quote(self, book) -> Optional[float]:
        bb, _ = best_of_book(book)
        raw = min(self.pre_cap, (bb or 0.0) + TICK)
        px  = tick_round(raw)
        if px <= 0.0: return None
        return px

    def try_micro_taker(self, ms: MarketState, book) -> bool:
        """If best ask <= pre_cap, take $1 immediately."""
        _, ba = best_of_book(book)
        if ba is None: return False
        if ba > self.pre_cap: return False
        # $1 at price ba => shares = dollars/price
        shares = max(0.01, PER_MARKET_USD / ba)
        # For prod: place a LIMIT BUY at price=ba with size=shares (it will cross immediately)
        cid_tag = f"{ms.cid[:10]}.."
        res = create_order(token_id=ms.no_token, side="buy", price=ba, size_shares=shares,
                           post_only=False, client_id=f"taker_{cid_tag}_{int(time.time())}")
        if not res.get("ok"):
            print(f"[taker] FAIL create: {res.get('error')}")
            return False
        # Assume executed (with $1 it should fill); you can also poll fills if you want certainty.
        self.bank_spent += PER_MARKET_USD * (1.0 + ENTRY_FEE + ENTRY_SLIP)
        ms.entered = True
        self.entered_count += 1
        print(f"✅ ENTERED (TAKER) ${PER_MARKET_USD:.2f} | {ms.question[:80]} @ {ba:.2f}  total_entered={self.entered_count}")
        return True

    def place_or_replace_maker(self, ms: MarketState, book) -> bool:
        """Ensure we have a post-only NO bid at the current target price; replace if needed."""
        target_px = self.target_no_quote(book)
        if not target_px: return False
        if ms.order_id and ms.order_px is not None:
            # If current quote worse than target (target lower) or better (target higher), replace
            if abs(ms.order_px - target_px) < 1e-9:
                return False  # good standing
            # replace: cancel then place
            cancel_order(ms.order_id)
            ms.order_id = None
            ms.order_px = None

        # place new post-only buy NO $1 at target_px
        shares = max(0.01, PER_MARKET_USD / target_px)
        cid_tag = f"{ms.cid[:10]}.."
        res = create_order(token_id=ms.no_token, side="buy", price=target_px, size_shares=shares,
                           post_only=True, client_id=f"maker_{cid_tag}_{int(time.time())}")
        if not res.get("ok"):
            print(f"[maker] FAIL create: {res.get('error')}")
            return False
        ms.order_id = res["order_id"]; ms.order_px = target_px
        print(f"📝 POST NO ${PER_MARKET_USD:.2f} @ {target_px:.2f} | {ms.question[:80]}")
        return True

    def cancel_open(self, ms: MarketState):
        if ms.order_id:
            cancel_order(ms.order_id)
            ms.order_id = None
            ms.order_px = None

    def seed(self):
        fresh = fetch_open_yesno(limit=250, max_pages=1, days_back=DAYS_BACK)
        # Newest first, filter to unseen
        added = 0
        for m in fresh:
            cid = m.get("conditionId") or m.get("id")
            if not cid or cid in self.markets: continue
            om = parse_outcomes(m); 
            if not om: continue
            no_token = om["NO"]
            created = dt(m.get("createdAt")) or dt(m.get("startDate")) or datetime.now(timezone.utc)
            ms = MarketState(cid, m.get("question") or "", m.get("slug") or "", no_token, created.timestamp())
            self.markets[cid] = ms
            added += 1
        if added:
            print(f"[seed] +{added} markets (total watch={len(self.markets)})")

    def run(self):
        print(f"=== prod-cover-100 ===  eff_cap≈{self.eff_cap:.3f}  pre_fee_cap≈{self.pre_cap:.3f}  tick={TICK:.2f}")
        last_seed = 0

        def handle_sig(sig, frame):
            self.stop = True
            print("\n[stop] cancelling open maker orders...")
            # Cancel all remaining live orders (best-effort)
            for ms in list(self.markets.values()):
                self.cancel_open(ms)
            print("[stop] done.")
        signal.signal(signal.SIGINT, handle_sig)
        signal.signal(signal.SIGTERM, handle_sig)

        try:
            while not self.stop and self.entered_count < TARGET_MARKETS:
                now_ts = int(time.time())
                if now_ts - last_seed >= RESEED_SEC:
                    self.seed()
                    last_seed = now_ts

                # sort by creation time (youngest first)
                due = [ms for ms in self.markets.values() if not ms.entered]
                due.sort(key=lambda x: x.created_ts, reverse=True)

                checked = 0
                for ms in due[:MAX_CHECKS_PER_TICK]:
                    if self.entered_count >= TARGET_MARKETS:
                        break
                    # fetch book
                    try:
                        book = fetch_book(ms.no_token, depth=60)
                    except Exception as e:
                        continue

                    # opportunistic taker if cheaper ask under cap
                    if not ms.entered:
                        if self.try_micro_taker(ms, book):
                            # after entry, cancel any resting order for this market
                            self.cancel_open(ms)
                            continue

                    # otherwise, ensure a resting post-only bid at cap
                    if not ms.entered:
                        self.place_or_replace_maker(ms, book)

                    checked += 1

                if checked == 0:
                    time.sleep(TICK_SLEEP_IDLE)
                else:
                    time.sleep(TICK_SLEEP_ACTIVE)

            print(f"🎯 Reached target: entered={self.entered_count} markets. Exiting.")
        finally:
            # best-effort cancel on exit
            for ms in list(self.markets.values()):
                self.cancel_open(ms)

if __name__ == "__main__":
    Cover100().run()