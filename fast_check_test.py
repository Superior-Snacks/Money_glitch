#!/usr/bin/env python3
# coverage_tester.py
#
# Goal: Maximize ENTRY COVERAGE across Yes/No markets.
# Strategy:
#   1) EV-based cap from your base rate P(NO)=~0.70
#   2) Micro-taker "dust" bites when best ask <= cap (to count as entered)
#   3) Maker ladder: post 3 small NO quotes (post-only simulated) just under cap
#   4) Prioritize NEWEST markets; reseed fast
#   5) Track coverage metrics for comparison
#
# NOTE: This is a *simulation* of placing/taking; no live authenticated orders.

import os, re, json, time, math, glob, gzip, random
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------------------- Config ---------------------------------

BASE_GAMMA   = "https://gamma-api.polymarket.com/markets"
BASE_BOOK    = "https://clob.polymarket.com/book"

# Coverage emphasis
RESEED_EVERY_SEC       = 15   # refresh market list fast
DAYS_BACK_FOR_SEED     = 0.05 # ~72 minutes
MAX_CHECKS_PER_TICK    = 400
MIN_PROBE_WHEN_IDLE    = 200
TICK_SLEEP_ACTIVE      = 0.20
TICK_SLEEP_IDLE        = 2.5

# Fees & EV (adjust as needed)
P_NO_BASE              = 0.70     # your measured base-rate
SETTLE_FEE             = 0.01     # 1% winner fee
TAKER_ENTRY_FEE        = 0.06     # taker effective entry premium
MAKER_ENTRY_FEE        = 0.06     # maker uses fee only (no slippage in sim)
TAKER_SLIP             = 0.02     # only for taker simulation; maker uses 0
MAKER_SLIP             = 0.00

# Budget & sizing
GLOBAL_BANK_START      = 5_000_000.0
PER_MARKET_BUDGET      = 100.0    # target at each market total
MICRO_TAKER_DUST_USD   = 5.0      # small bite to count as "entered"
MAKER_LADDER_SIZES     = [5.0, 5.0, 5.0]  # dollars per ladder rung

# Books & throttling
BOOK_DEPTH             = 80
RPS_TARGET             = 10.0      # token bucket
HTTP_TIMEOUT           = 15
TICK                   = 0.01

# Logs
RUN_ID                 = f"coverage_run_{int(time.time())}"
LOG_DIR                = RUN_ID
TRADE_LOG_BASE         = "trades.jsonl"
DECISIONS_LOG_BASE     = "decisions.jsonl"
SNAP_LOG_BASE          = "snapshots.jsonl"
NET_LOG_BASE           = "net_usage.jsonl"
MAX_LOG_BYTES          = 50 * 1024 * 1024
RETAIN_DAYS            = 7
COMPRESS_AFTER_DAYS    = 1

VERBOSE                = True
SHOW_DEBUG_BOOKS       = False

# -------------------------- Helpers/Infra ------------------------------

def vprint(*a, **k):
    if VERBOSE: print(*a, **k)

def make_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429,500,502,503,504),
        allowed_methods=("GET","POST"),
        raise_on_status=False,
    )
    ad = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update({"User-Agent": "coverage-bot/1.0"})
    return s

SESSION = make_session()
_last_tokens_ts = time.monotonic()
_bucket = RPS_TARGET
bytes_in_total = 0

def _rate_limit():
    global _last_tokens_ts, _bucket
    now = time.monotonic()
    _bucket = min(RPS_TARGET, _bucket + (now - _last_tokens_ts) * RPS_TARGET)
    _last_tokens_ts = now
    if _bucket < 1.0:
        need = (1.0 - _bucket) / RPS_TARGET
        time.sleep(need)
        now2 = time.monotonic()
        _bucket = min(RPS_TARGET, _bucket + (now2 - _last_tokens_ts) * RPS_TARGET)
        _last_tokens_ts = now2
    _bucket -= 1.0

def _ensure_logdir():
    os.makedirs(LOG_DIR, exist_ok=True)

def _dated_with_part(path_base: str) -> str:
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    name, ext = os.path.splitext(path_base)
    base = os.path.join(LOG_DIR, f"{name}_{day}{ext}")
    if os.path.exists(base) and os.path.getsize(base) >= MAX_LOG_BYTES:
        i = 1
        while True:
            cand = os.path.join(LOG_DIR, f"{name}_{day}_part{i}{ext}")
            if not os.path.exists(cand) or os.path.getsize(cand) < MAX_LOG_BYTES:
                return cand
            i += 1
    return base

def append_jsonl(path_base: str, record: dict):
    _ensure_logdir()
    path = _dated_with_part(path_base)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def log_net_usage():
    append_jsonl(NET_LOG_BASE, {
        "ts": now_iso(),
        "bytes_in_total": bytes_in_total
    })

def _parse_day_from_filename(path):
    base = os.path.basename(path)
    m = re.search(r"_(\d{4}-\d{2}-\d{2})(?:_part\d+)?\.jsonl$", base)
    if not m: return None
    try: return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except: return None

def compress_and_prune_logs(log_dir=LOG_DIR,
                            retain_days=RETAIN_DAYS,
                            compress_after_days=COMPRESS_AFTER_DAYS):
    today = datetime.now(timezone.utc).date()
    for path in glob.glob(os.path.join(log_dir, "*.jsonl")):
        d = _parse_day_from_filename(path)
        if not d: 
            continue
        age_days = (today - d).days
        gz_path = path + ".gz"
        if age_days >= compress_after_days and not os.path.exists(gz_path):
            try:
                if d != today:
                    with open(path, "rb") as fin, gzip.open(gz_path, "wb") as fout:
                        fout.writelines(fin)
                    os.remove(path)
                    vprint(f"[LOG] compressed {os.path.basename(path)}")
            except Exception as e:
                vprint(f"[LOG WARN] compress failed for {path}: {e}")
    for path in glob.glob(os.path.join(log_dir, "*.jsonl*")):
        d = _parse_day_from_filename(path.replace(".gz",""))
        if not d: 
            continue
        age_days = (today - d).days
        if age_days > retain_days:
            try:
                os.remove(path)
                vprint(f"[LOG] deleted old log {os.path.basename(path)}")
            except Exception as e:
                vprint(f"[LOG WARN] delete failed for {path}: {e}")

# -------------------------- Market/Books -------------------------------

def _dt(s):
    try:
        return datetime.fromisoformat(str(s).replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def fetch_open_yesno_fast(limit=250, max_pages=2, days_back=0.1,
                          require_clob=True, session=SESSION, verbose=False):
    params = {
        "limit": limit, "order": "startDate",
        "ascending": False, "closed": False
    }
    if days_back:
        params["start_date_min"] = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
    if require_clob:
        params["enableOrderBook"] = True

    all_rows, seen_ids = [], set()
    offset, pages = 0, 0

    while pages < max_pages:
        q = dict(params, offset=offset)
        _rate_limit()
        r = session.get(BASE_GAMMA, params=q, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        page = r.json() or []
        if not page:
            break
        added = 0
        for m in page:
            outs = m.get("outcomes")
            if isinstance(outs, str):
                try: outs = json.loads(outs)
                except: outs = None
            def is_yesno(lst):
                if not isinstance(lst, list) or len(lst) != 2: return False
                s = [str(x).strip().lower() for x in lst]
                return set(s) == {"yes","no"}
            if is_yesno(outs):
                mid = m.get("id")
                if mid not in seen_ids:
                    all_rows.append(m); seen_ids.add(mid); added += 1
        if verbose:
            vprint(f"[PAGE {pages}] raw={len(page)} add_yesno={added} total={len(all_rows)}")
        if len(page) < limit: break
        pages += 1; offset += limit

    all_rows.sort(key=lambda m: (m.get("startDate") or m.get("createdAt") or ""), reverse=True)
    if verbose:
        vprint(f"✅ Total open Yes/No: {len(all_rows)}")
    return all_rows

def outcome_map_from_market(market: dict) -> dict:
    outs = market.get("outcomes")
    toks = market.get("clobTokenIds")
    if isinstance(outs, str):
        try: outs = json.loads(outs)
        except: outs = None
    if isinstance(toks, str):
        try: toks = json.loads(toks)
        except: toks = None
    if isinstance(outs, list) and isinstance(toks, list) and len(outs) == 2 and len(toks) == 2:
        o0, o1 = str(outs[0]).strip().upper(), str(outs[1]).strip().upper()
        if (o0, o1) == ("YES","NO"): return {"YES": toks[0], "NO": toks[1]}
        if (o0, o1) == ("NO","YES"): return {"YES": toks[1], "NO": toks[0]}
        if "NO" in o0 and "YES" in o1: return {"YES": toks[1], "NO": toks[0]}
        if "YES" in o0 and "NO" in o1: return {"YES": toks[0], "NO": toks[1]}
    if isinstance(toks, list) and len(toks) == 2:
        return {"YES": toks[0], "NO": toks[1]}
    raise ValueError(f"Cannot resolve YES/NO token ids for market {market.get('id') or market.get('conditionId')}")

def get_no_token_id(market: dict) -> Optional[str]:
    try: return outcome_map_from_market(market)["NO"]
    except: return None

def fetch_book(token_id: str, depth: int = BOOK_DEPTH, session=SESSION):
    _rate_limit()
    r = session.get(BASE_BOOK, params={"token_id": token_id}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    body = r.content
    book = r.json() if body else {}
    book["bids"] = (book.get("bids") or [])[:depth]
    book["asks"] = (book.get("asks") or [])[:depth]
    global bytes_in_total
    bytes_in_total += len(body) if body else 0
    return book

def best_of_book(book):
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    try: best_bid = max((float(b["price"]) for b in bids if "price" in b), default=None)
    except ValueError: best_bid = None
    try: best_ask = min((float(a["price"]) for a in asks if "price" in a), default=None)
    except ValueError: best_ask = None
    return best_bid, best_ask

# ----------------------------- EV Cap ---------------------------------

def ev_effective_cap(p_no=P_NO_BASE, settle_fee=SETTLE_FEE):
    # p_eff <= p_no*(1 - settle_fee)
    return p_no * (1.0 - settle_fee)

def pre_fee_cap_from_effective(eff_cap: float, entry_fee_mult: float):
    # pre_fee * entry_fee_mult <= eff_cap  =>  pre_fee <= eff_cap / entry_fee_mult
    return max(0.01, eff_cap / entry_fee_mult - 1e-6)

def price_floor_tick(x, tick=TICK):
    return max(0.01, round(x / tick) * tick)

# ---------------------- Simulated fills & orders -----------------------

def simulate_take_from_asks(book: dict, dollars: float, entry_mult: float, price_cap_eff: float):
    """
    Take NO asks up to 'dollars' while keeping effective VWAP <= price_cap_eff.
    entry_mult = (1+fee+slip). Returns (spent_after, shares, avg_eff_price)
    """
    asks = book.get("asks") or []
    if not asks or dollars <= 0: return (0.0, 0.0, 0.0)

    try:
        asks = sorted(
            [{"price": float(a["price"]), "size": float(a["size"])}
             for a in asks
             if float(a.get("price",0)) > 0 and float(a.get("size",0)) > 0],
            key=lambda a: a["price"]
        )
    except Exception:
        return (0.0, 0.0, 0.0)

    spent_ex = shares = 0.0
    budget_left = float(dollars)

    for lvl in asks:
        px, sz = lvl["price"], lvl["size"]
        lvl_dollars_cap = min(budget_left, px * sz)
        if lvl_dollars_cap <= 0: 
            continue

        new_spent_ex = spent_ex + lvl_dollars_cap
        new_shares   = shares + (lvl_dollars_cap / px)
        vwap_ex      = new_spent_ex / new_shares
        vwap_eff     = vwap_ex * entry_mult

        if vwap_eff <= price_cap_eff:
            spent_ex = new_spent_ex; shares = new_shares
            budget_left -= lvl_dollars_cap
            if budget_left <= 1e-9: break
            continue

        # partial at this level
        target_ex = price_cap_eff / entry_mult
        denom = 1.0 - (target_ex / px)
        rhs   = target_ex * shares - spent_ex
        if denom <= 0: 
            continue
        x_max = max(0.0, min(lvl_dollars_cap, rhs / denom))
        if x_max > 1e-8:
            spent_ex += x_max
            shares   += x_max / px
            budget_left -= x_max
            break

    if shares <= 0: return (0.0, 0.0, 0.0)
    spent_eff = spent_ex * entry_mult
    avg_eff = spent_eff / shares
    return (spent_eff, shares, avg_eff)

class MakerOrder:
    def __init__(self, price, dollars, fee_mult):
        self.price = float(price)     # pre-fee quote
        self.dollars = float(dollars) # budget at this rung
        self.fee_mult = float(fee_mult)
        self.shares = max(0.01, self.dollars / self.price)
        self.remaining = self.shares
        self.placed_ts = time.time()
        self.last_refresh_ts = self.placed_ts

class CoverageEngine:
    def __init__(self,
                 p_no_base=P_NO_BASE,
                 settle_fee=SETTLE_FEE,
                 taker_fee=TAKER_ENTRY_FEE,
                 taker_slip=TAKER_SLIP,
                 maker_fee=MAKER_ENTRY_FEE,
                 maker_slip=MAKER_SLIP,
                 per_market_budget=PER_MARKET_BUDGET,
                 micro_taker_dust=MICRO_TAKER_DUST_USD,
                 maker_sizes=MAKER_LADDER_SIZES,
                 global_bank=GLOBAL_BANK_START):
        self.p_no_base = p_no_base
        self.settle_fee = settle_fee
        self.taker_mult = 1.0 + taker_fee + taker_slip
        self.maker_mult = 1.0 + maker_fee + maker_slip
        self.eff_cap = ev_effective_cap(p_no_base, settle_fee)
        self.pre_fee_cap_taker = pre_fee_cap_from_effective(self.eff_cap, self.taker_mult)
        self.pre_fee_cap_maker = pre_fee_cap_from_effective(self.eff_cap, self.maker_mult)
        self.per_market_budget = per_market_budget
        self.micro_taker_dust = micro_taker_dust
        self.maker_sizes = maker_sizes
        self.bank = global_bank

        # coverage metrics
        self.markets_seen = 0
        self.markets_entered = 0
        self.entered_set = set()  # conditionId
        self.first_entry_ts: Dict[str, float] = {}
        self.created_ts: Dict[str, float] = {}

        # state
        self.watch: Dict[str, dict] = {}     # cid -> state
        self.maker_orders: Dict[str, List[MakerOrder]] = {}  # cid -> ladder

    def is_tradable(self, m):
        if not m.get("enableOrderBook"): return False
        toks = m.get("clobTokenIds")
        if isinstance(toks, str):
            try: toks = json.loads(toks)
            except: toks = []
        return isinstance(toks, list) and len(toks) == 2

    def seed(self, markets: List[dict], created_cutoff: datetime):
        now = int(time.time())
        added = 0
        for m in markets:
            cid = m.get("conditionId") or m.get("id")
            if not cid or cid in self.watch: 
                continue
            no_token = get_no_token_id(m)
            if not no_token: 
                continue
            cts = _dt(m.get("createdAt")) or _dt(m.get("startDate")) or datetime.now(timezone.utc)
            self.watch[cid] = {
                "m": m,
                "no_token": no_token,
                "next_check": now + random.randint(0,2),
                "fails": 0,
                "last_seen_ts": now,
                "ever_under_cap": False
            }
            self.created_ts[cid] = cts.timestamp()
            added += 1
        if added:
            vprint(f"[SEED] added {added} (watch={len(self.watch)})")

    def ladder_prices(self, pre_fee_cap: float, best_bid: float) -> List[float]:
        p0 = max(0.01, min(pre_fee_cap, (best_bid or 0.0) + TICK))
        p1 = max(0.01, p0 - 0.02)
        p2 = max(0.01, p0 - 0.05)
        prices = sorted({price_floor_tick(p0), price_floor_tick(p1), price_floor_tick(p2)}, reverse=True)
        return prices

    def _mark_entered(self, cid: str):
        if cid not in self.entered_set:
            self.entered_set.add(cid)
            self.markets_entered += 1
            self.first_entry_ts.setdefault(cid, time.time())

    def _log_trade(self, m, token_id, side, spent_after, shares, avg_eff_price, event):
        append_jsonl(TRADE_LOG_BASE, {
            "ts": now_iso(),
            "market_id": m.get("conditionId"),
            "market_slug": m.get("slug"),
            "question": m.get("question"),
            "side": side,
            "token_id": token_id,
            "spent_after": round(float(spent_after), 6),
            "shares": round(float(shares), 6),
            "avg_price_eff": round(float(avg_eff_price), 6),
            "event": event,
            "bank_after": round(float(self.bank), 6)
        })

    def step_market(self, cid: str) -> Tuple[int,int]:
        """
        Returns (entered, checked) where 'entered' is 1 if we achieved any position in this step.
        """
        st = self.watch.get(cid)
        if not st: return (0,0)
        m = st["m"]; token = st["no_token"]
        entered_now = 0

        # 1) fetch book
        book = fetch_book(token, depth=BOOK_DEPTH)
        st["last_seen_ts"] = int(time.time())

        # 2) micro-taker dust if best ask under taker cap (effective)
        best_bid, best_ask = best_of_book(book)
        if best_ask is not None:
            if (best_ask * self.taker_mult) <= self.eff_cap and self.bank >= 0.01:
                take_dollars = min(self.micro_taker_dust, self.per_market_budget)  # limit per-market
                spent, shares, avg_eff = simulate_take_from_asks(
                    book, take_dollars, entry_mult=self.taker_mult, price_cap_eff=self.eff_cap
                )
                if shares > 0:
                    self.bank -= spent
                    entered_now = 1
                    self._mark_entered(cid)
                    self._log_trade(m, token, "NO", spent, shares, avg_eff, event="micro_taker_fill")

        # 3) maker ladder if we still have budget to allocate at this market
        allocated_here = 0.0
        if cid in self.maker_orders:
            for mo in self.maker_orders[cid]:
                allocated_here += mo.price * mo.remaining * self.maker_mult
        remaining_budget = max(0.0, self.per_market_budget - allocated_here)

        if remaining_budget > 0.01 and self.bank > 0.01:
            prices = self.ladder_prices(self.pre_fee_cap_maker, best_bid or 0.0)
            # build/refresh a ladder of at most len(maker_sizes)
            ladder = self.maker_orders.setdefault(cid, [])
            # keep number of rungs <= len(maker_sizes)
            while len(ladder) > len(self.maker_sizes):
                ladder.pop()

            # place/refresh rungs
            for i, rung_usd in enumerate(self.maker_sizes):
                if i >= len(prices): break
                px = prices[i]
                # if rung exists, maybe simulate a nibble; else create it
                if i < len(ladder):
                    mo = ladder[i]
                    # replace if price drifted
                    if abs(mo.price - px) > 1e-9:
                        ladder[i] = MakerOrder(px, rung_usd, self.maker_mult)
                        mo = ladder[i]
                        append_jsonl(DECISIONS_LOG_BASE, {
                            "ts": now_iso(), "type": "maker_replace",
                            "market_id": m.get("conditionId"), "side":"NO",
                            "price_pre_fee": round(float(px),6), "dollars": rung_usd
                        })
                    # simulate small probability fill nibble at resting price
                    fill_prob = 0.15  # crude; tweak as desired
                    if random.random() < fill_prob and mo.remaining > 0:
                        nibble_shares = max(0.01, mo.remaining * 0.25)
                        nibble_shares = min(nibble_shares, mo.remaining)
                        spent = nibble_shares * mo.price * self.maker_mult
                        if self.bank >= spent:
                            self.bank -= spent
                            mo.remaining -= nibble_shares
                            entered_now = 1
                            self._mark_entered(cid)
                            self._log_trade(m, token, "NO", spent, nibble_shares, mo.price*self.maker_mult, event="maker_partial_fill")
                else:
                    # create new rung if budget allows
                    mo = MakerOrder(px, rung_usd, self.maker_mult)
                    # scale if bank insufficient
                    needed = mo.shares * mo.price * self.maker_mult
                    if self.bank < needed:
                        if self.bank <= 0: break
                        dollars_fit = self.bank / self.maker_mult
                        if dollars_fit < 0.50: 
                            break
                        mo = MakerOrder(px, dollars_fit, self.maker_mult)
                        needed = mo.shares * mo.price * self.maker_mult
                    ladder.append(mo)
                    self.bank -= 0.0  # no immediate spend; spend occurs on simulated fills
                    append_jsonl(DECISIONS_LOG_BASE, {
                        "ts": now_iso(), "type":"maker_quote",
                        "market_id": m.get("conditionId"), "side":"NO",
                        "price_pre_fee": round(float(px),6), "dollars": round(float(mo.dollars),2)
                    })

        # schedule next check (faster after actions)
        st["next_check"] = int(time.time()) + (2 if entered_now else 5)
        return (entered_now, 1)

    def log_snapshot(self):
        # avg time to first entry
        deltas = []
        for cid in self.entered_set:
            if cid in self.first_entry_ts and cid in self.created_ts:
                deltas.append(self.first_entry_ts[cid] - self.created_ts[cid])
        avg_t_entry = (sum(deltas) / len(deltas)) if deltas else None
        snap = {
            "ts": now_iso(),
            "bank": round(float(self.bank), 2),
            "markets_seen": self.markets_seen,
            "markets_entered": self.markets_entered,
            "entry_rate": round(float(self.markets_entered / self.markets_seen), 4) if self.markets_seen else None,
            "avg_time_to_first_entry_sec": round(float(avg_t_entry), 3) if avg_t_entry is not None else None,
            "pre_fee_cap_taker": round(float(self.pre_fee_cap_taker), 4),
            "pre_fee_cap_maker": round(float(self.pre_fee_cap_maker), 4),
            "effective_cap": round(float(self.eff_cap), 4),
        }
        append_jsonl(SNAP_LOG_BASE, snap)

# ------------------------------ Main ----------------------------------

def main():
    vprint(f"=== Coverage Tester ===\nRun ID: {RUN_ID}\nLog Dir: {LOG_DIR}")
    vprint(f"P(NO)={P_NO_BASE:.3f}  settle_fee={SETTLE_FEE:.3f}  eff_cap={ev_effective_cap():.3f}")
    vprint(f"taker pre-fee cap≈{pre_fee_cap_from_effective(ev_effective_cap(), 1+TAKER_ENTRY_FEE+TAKER_SLIP):.3f} "
           f"maker pre-fee cap≈{pre_fee_cap_from_effective(ev_effective_cap(), 1+MAKER_ENTRY_FEE+MAKER_SLIP):.3f}")

    eng = CoverageEngine()
    created_cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
    last_seed = 0

    try:
        while True:
            now_ts = int(time.time())

            # reseed new markets frequently (youngest first)
            if now_ts - last_seed >= RESEED_EVERY_SEC:
                try:
                    fresh = fetch_open_yesno_fast(limit=250, max_pages=1,
                                                  days_back=DAYS_BACK_FOR_SEED, verbose=False)
                    new_only = [m for m in fresh if _dt(m.get("createdAt")) and _dt(m["createdAt"]) > created_cutoff]
                    tradable = [m for m in new_only if eng.is_tradable(m)]
                    eng.markets_seen += len(tradable)
                    eng.seed(tradable, created_cutoff)
                    seen_max = max((_dt(m.get("createdAt")) for m in new_only if _dt(m.get("createdAt"))), default=created_cutoff)
                    created_cutoff = max(created_cutoff, seen_max)
                    vprint(f"[REFRESH] seen+={len(tradable)} watch={len(eng.watch)} entered={eng.markets_entered}")
                    eng.log_snapshot()
                    log_net_usage()
                    compress_and_prune_logs()
                except Exception as e:
                    vprint(f"[WARN reseed] {e}")
                last_seed = now_ts

            # choose due markets (newest first, not yet entered gets priority)
            due = [cid for cid, st in eng.watch.items() if st["next_check"] <= now_ts]
            due.sort(key=lambda cid: (
                cid not in eng.entered_set,                             # push not-entered first
                eng.created_ts.get(cid, 0.0)                            # newest first
            ), reverse=True)

            if len(due) < MIN_PROBE_WHEN_IDLE:
                # force-probe a slice of newest markets
                all_ids = list(eng.watch.keys())
                all_ids.sort(key=lambda cid: eng.created_ts.get(cid, 0.0), reverse=True)
                probe_ids = [cid for cid in all_ids if cid not in eng.entered_set][:MIN_PROBE_WHEN_IDLE]
                for cid in probe_ids:
                    eng.watch[cid]["next_check"] = now_ts
                due = [cid for cid, st in eng.watch.items() if st["next_check"] <= now_ts]

            checked = entered_cnt = 0
            for cid in due[:MAX_CHECKS_PER_TICK]:
                e, c = eng.step_market(cid)
                entered_cnt += e; checked += c

            # pace loop
            if checked == 0:
                time.sleep(TICK_SLEEP_IDLE)
            else:
                time.sleep(TICK_SLEEP_ACTIVE)

    except KeyboardInterrupt:
        vprint("\nStopped by user.")
        eng.log_snapshot()

if __name__ == "__main__":
    main()