#!/usr/bin/env python3
# prod_cover_100_log.py
#
# Live coverage test: enter up to 100 markets, $1 NO each.
# Logs identical to simulation: trades.jsonl, decisions.jsonl, snapshots.jsonl, net_usage.jsonl
# DRY_RUN=True by default (safe).  Flip to False after wiring live order endpoints.

import os, json, time, uuid, signal, random
from datetime import datetime, timezone, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError as ReqConnErr


# -------------------- Config --------------------
GAMMA_URL = "https://gamma-api.polymarket.com/markets"
BOOK_URL  = "https://clob.polymarket.com/book"
RUN_ID    = f"prod_run_{int(time.time())}"
LOG_DIR   = RUN_ID

DRY_RUN   = True
TARGET_MARKETS = 100
PER_MARKET_USD = 1.0
DAYS_BACK = 0.08
RESEED_SEC = 10
MAX_CHECKS = 400
TICK_SLEEP_ACTIVE = 0.15
TICK_SLEEP_IDLE   = 1.5

# EV / fee assumptions
P_NO_BASE  = 0.70
SETTLE_FEE = 0.01
ENTRY_FEE  = 0.06
ENTRY_SLIP = 0.00
TICK       = 0.01
RATE_LIMIT_RPS = 10
HTTP_TIMEOUT   = (5, 35)

# -------------- Logging identical to sim ---------------
TRADE_LOG_BASE = "trades.jsonl"
DECISIONS_LOG_BASE = "decisions.jsonl"
SNAP_LOG_BASE = "snapshots.jsonl"
NET_LOG_BASE = "net_usage.jsonl"
MAX_LOG_BYTES = 50 * 1024 * 1024

def _ensure_logdir():
    os.makedirs(LOG_DIR, exist_ok=True)

def _dated_with_part(path_base):
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    name, ext = os.path.splitext(path_base)
    base = os.path.join(LOG_DIR, f"{name}_{day}{ext}")
    if os.path.exists(base) and os.path.getsize(base) >= MAX_LOG_BYTES:
        i=1
        while True:
            cand=os.path.join(LOG_DIR,f"{name}_{day}_part{i}{ext}")
            if not os.path.exists(cand) or os.path.getsize(cand)<MAX_LOG_BYTES:
                return cand
            i+=1
    return base

def append_jsonl(path_base, record):
    _ensure_logdir()
    path=_dated_with_part(path_base)
    with open(path,"a",encoding="utf-8") as f:
        f.write(json.dumps(record,ensure_ascii=False,default=str)+"\n")

def now_iso(): return datetime.now(timezone.utc).isoformat()

def log_net_error(where: str, err: Exception, meta: dict | None = None):
    rec = {
        "ts": now_iso(),
        "type": "net_error",
        "where": where,
        "error": repr(err)
    }
    if meta:
        rec.update(meta)
    append_jsonl(DECISIONS_LOG_BASE, rec)

# -------------- Helpers / EV cap ----------------
def ev_cap(p_no=P_NO_BASE, settle=SETTLE_FEE): return p_no*(1-settle)
def pre_cap(eff,fee=ENTRY_FEE,slip=ENTRY_SLIP):
    mult=1+fee+slip; return max(0.01, eff/mult-1e-6)
def tick_round(x): return max(0.01, round(x/TICK)*TICK)

# -------------- HTTP + rate limiting --------------
S=requests.Session()
retry=Retry(total=5,connect=5,read=5,backoff_factor=0.5,
            status_forcelist=(429,500,502,503,504),
            allowed_methods=("GET","POST"))
ad=HTTPAdapter(max_retries=retry,pool_connections=40,pool_maxsize=40)
S.mount("https://",ad);S.mount("http://",ad)
S.headers.update({"User-Agent":"prod-cover-100/1.0"})
_last=time.monotonic();bucket=RATE_LIMIT_RPS
def rps():
    global _last,bucket
    now=time.monotonic()
    bucket=min(RATE_LIMIT_RPS,bucket+(now-_last)*RATE_LIMIT_RPS)
    _last=now
    if bucket<1:
        time.sleep((1-bucket)/RATE_LIMIT_RPS)
        now2=time.monotonic()
        bucket=min(RATE_LIMIT_RPS,bucket+(now2-_last)*RATE_LIMIT_RPS)
        _last=now2
    bucket-=1

# -------------- API fetchers --------------------
def dt(s):
    try:return datetime.fromisoformat(str(s).replace("Z","+00:00")).astimezone(timezone.utc)
    except:return None

def parse_outcomes(m):
    outs=m.get("outcomes");toks=m.get("clobTokenIds")
    if isinstance(outs,str):
        try:outs=json.loads(outs)
        except:outs=None
    if isinstance(toks,str):
        try:toks=json.loads(toks)
        except:toks=None
    if isinstance(outs,list) and isinstance(toks,list) and len(outs)==2:
        o0,o1=str(outs[0]).upper(),str(outs[1]).upper()
        if o0=="YES" and o1=="NO": return {"YES":toks[0],"NO":toks[1]}
        if o0=="NO" and o1=="YES": return {"YES":toks[1],"NO":toks[0]}
        if "NO" in o0 and "YES" in o1: return {"YES":toks[1],"NO":toks[0]}
        if "YES" in o0 and "NO" in o1: return {"YES":toks[0],"NO":toks[1]}
    return None

def fetch_markets(limit=200):
    rps()
    q = {
        "limit": limit,
        "order": "startDate",
        "ascending": False,
        "closed": False,
        "enableOrderBook": True,
        "start_date_min": (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).isoformat()
    }
    try:
        r = S.get(GAMMA_URL, params=q, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        page = r.json() or []
    except (ReadTimeout, ConnectTimeout, ReqConnErr) as e:
        log_net_error("fetch_markets", e, {"params": q})
        # Return empty so caller can back off without crashing
        return []
    except Exception as e:
        log_net_error("fetch_markets_other", e, {"params": q})
        return []

    out = []
    for m in page:
        if not m.get("enableOrderBook"):
            continue
        if parse_outcomes(m):
            out.append(m)
    out.sort(key=lambda m: (m.get("createdAt") or ""), reverse=True)
    return out

def fetch_book(token):
    rps()
    try:
        r = S.get(BOOK_URL, params={"token_id": token}, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        book = r.json() or {}
    except (ReadTimeout, ConnectTimeout, ReqConnErr) as e:
        log_net_error("fetch_book", e, {"token_id": token})
        return {"bids": [], "asks": []}
    except Exception as e:
        log_net_error("fetch_book_other", e, {"token_id": token})
        return {"bids": [], "asks": []}

    book["bids"] = (book.get("bids") or [])[:60]
    book["asks"] = (book.get("asks") or [])[:60]
    return book

def best_of_book(book):
    bids,asks=book.get("bids") or [],book.get("asks") or []
    bb=max((float(b["price"]) for b in bids if "price" in b),default=None)
    ba=min((float(a["price"]) for a in asks if "price" in a),default=None)
    return bb,ba

# -------------- Live order stubs (DRY_RUN) --------------
def create_order(token,side,price,shares,post_only=True,client_id=None):
    rec={"ts":now_iso(),"type":"create_order","token":token,
         "side":side,"price":price,"shares":shares,
         "post_only":post_only,"client_id":client_id}
    append_jsonl(DECISIONS_LOG_BASE,rec)
    if DRY_RUN:return {"ok":True,"order_id":client_id or f"dry_{uuid.uuid4()}"}
    raise NotImplementedError

def cancel_order(order_id):
    append_jsonl(DECISIONS_LOG_BASE,{"ts":now_iso(),"type":"cancel","order_id":order_id})
    if DRY_RUN:return {"ok":True}
    raise NotImplementedError

# -------------- Engine --------------------
class Market:
    def __init__(self,m):
        self.cid=m.get("conditionId") or m.get("id")
        self.question=m.get("question") or ""
        self.slug=m.get("slug") or ""
        self.no_token=parse_outcomes(m)["NO"]
        self.created_ts=(dt(m.get("createdAt")) or datetime.now(timezone.utc)).timestamp()
        self.entered=False;self.order_id=None;self.order_px=None
        self.seed_backoff = 0          # seconds; 0 means no backoff
        self.seed_backoff_max = 60

class CoverBot:
    def __init__(self):
        self.eff_cap=ev_cap();self.pre_cap=pre_cap(self.eff_cap)
        self.markets={};self.entered_count=0;self.bank_spent=0;self.stop=False

    def log_trade(self,mkt,event,price,shares,spent):
        append_jsonl(TRADE_LOG_BASE,{
            "ts":now_iso(),
            "market_id":mkt.cid,"market_slug":mkt.slug,
            "question":mkt.question,
            "side":"NO","token_id":mkt.no_token,
            "spent_after":round(spent,6),
            "shares":round(shares,6),
            "avg_price_eff":round(price,6),
            "event":event,
            "bank_after":round(self.bank_spent,6)
        })

    def log_snapshot(self):
        snap={"ts":now_iso(),"entered":self.entered_count,
              "eff_cap":self.eff_cap,"pre_cap":self.pre_cap}
        append_jsonl(SNAP_LOG_BASE,snap)

    def target_quote(self,book):
        bb,_=best_of_book(book);raw=min(self.pre_cap,(bb or 0)+TICK)
        return tick_round(raw)

    def try_taker(self,mkt,book):
        _,ba=best_of_book(book)
        if ba is None or ba>self.pre_cap:return False
        shares=max(0.01,PER_MARKET_USD/ba)
        cid=f"{mkt.cid[:10]}.."
        create_order(mkt.no_token,"buy",ba,shares,post_only=False,client_id=f"taker_{cid}")
        self.bank_spent+=PER_MARKET_USD*(1+ENTRY_FEE+ENTRY_SLIP)
        self.entered_count+=1;mkt.entered=True
        self.log_trade(mkt,"taker_fill",ba,shares,PER_MARKET_USD)
        print(f"✅ taker {mkt.question[:60]} @{ba:.2f} ({self.entered_count})")
        return True

    def maker_place_or_replace(self,mkt,book):
        px=self.target_quote(book)
        if not px:return False
        if mkt.order_id and abs(mkt.order_px-px)<1e-9:return False
        if mkt.order_id: cancel_order(mkt.order_id)
        shares=max(0.01,PER_MARKET_USD/px)
        cid=f"{mkt.cid[:10]}.."
        res=create_order(mkt.no_token,"buy",px,shares,post_only=True,client_id=f"maker_{cid}")
        if res.get("ok"):
            mkt.order_id=res["order_id"];mkt.order_px=px
            append_jsonl(DECISIONS_LOG_BASE,{
                "ts":now_iso(),"type":"maker_post",
                "market_id":mkt.cid,"side":"NO","price_pre_fee":round(px,6),
                "dollars":PER_MARKET_USD,"order_id":mkt.order_id})
            print(f"📝 maker {mkt.question[:60]} @{px:.2f}")
        return True

    def seed(self):
        fresh = fetch_markets()
        if not fresh:
            # network hiccup → back off
            self.seed_backoff = min(self.seed_backoff * 2 + 5, self.seed_backoff_max) if self.seed_backoff else 5
            append_jsonl(DECISIONS_LOG_BASE, {
                "ts": now_iso(),
                "type": "seed_backoff",
                "backoff_sec": self.seed_backoff
            })
            return

        # success → clear backoff
        if self.seed_backoff:
            append_jsonl(DECISIONS_LOG_BASE, {
                "ts": now_iso(),
                "type": "seed_backoff_clear",
                "prev_backoff_sec": self.seed_backoff
            })
        self.seed_backoff = 0

        added = 0
        for m in fresh:
            cid = m.get("conditionId") or m.get("id")
            if cid in self.markets: continue
            self.markets[cid] = Market(m); added += 1
        if added:
            print(f"[seed]+{added}")

    def run(self):
        print(f"=== prod-cover-100-log === eff_cap={self.eff_cap:.3f} pre_cap={self.pre_cap:.3f}")
        last_seed=0
        signal.signal(signal.SIGINT,lambda s,f:setattr(self,"stop",True))
        try:
            while not self.stop and self.entered_count<TARGET_MARKETS:
                now=time.time()
                if now - last_seed >= RESEED_SEC:
                    self.seed()
                    self.log_snapshot()
                    last_seed = now
                    if self.seed_backoff:
                        # sleep extra to honor backoff before next loop
                        time.sleep(self.seed_backoff)
                        continue

                
                due=[m for m in self.markets.values() if not m.entered]
                due.sort(key=lambda x:x.created_ts,reverse=True)
                if not due:
                    time.sleep(TICK_SLEEP_IDLE);continue
                for mkt in due[:MAX_CHECKS]:
                    if self.entered_count>=TARGET_MARKETS:break
                    try:book=fetch_book(mkt.no_token)
                    except:continue
                    if self.try_taker(mkt,book):continue
                    self.maker_place_or_replace(mkt,book)
                time.sleep(TICK_SLEEP_ACTIVE)
        finally:
            for m in self.markets.values():
                if m.order_id: cancel_order(m.order_id)
            self.log_snapshot()
            print(f"Done entered={self.entered_count}")

if __name__=="__main__":
    CoverBot().run()