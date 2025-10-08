import json
import time
import requests
from datetime import datetime, timezone

BASE_GAMMA = "https://gamma-api.polymarket.com/markets"
BASE_HISTORY = "https://clob.polymarket.com/prices-history"
BASE_TRADES = "http://data-api.polymarket.com/trades"
BASE_BOOK = "https://clob.polymarket.com/book"

def fetch_markets(limit=20, offset=4811):
    params = {
        "limit": limit,
        "offset": offset,
        "outcomes": ["YES", "NO"],
        "sortBy": "creationTime"}
    r = requests.get(BASE_GAMMA, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def filter_markets(markets):
    """
    making sure to only check the weird bets
    """
    print("filtering markets")
    if not markets:
        print("ERROR")
        return None
    cleaned = []
    for mk in markets:
        outcomes = mk["outcomes"]
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if outcomes == ["Yes", "No"]:
            cleaned.append(mk)
    return cleaned


def get_trade_for_market(marked_dict):
    params = {"market": marked_dict["conditionId"],
              "sort": "asc",
              "limit": "max"}
    r = requests.get(BASE_TRADES, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    #print(payload[0].keys())
    return payload

def filter_no_trades(trades):
    """
    only interested in the no position rn
    """
    #print("started filtering out correct trades")
    if not trades:
        print("ERROR")
        return None
    bucket = []
    for tr in trades:
        if tr["side"] == "BUY" and tr["outcome"] == "No":
            bucket.append(tr)
            #print("got no")
        elif tr["side"] == "SELL" and tr["outcome"] == "Yes":
            bucket.append(tr)
            #print("got YES")
    return bucket

def compress_trades(trades):
    """
    adds up trades that are the same giving a lower bound on how much was able to be bought
    needs to be filterd for the types I want to look at
    """
    if not trades:
        print("ERROR")
        return None
    sections = []
    trades = sorted(trades, key=lambda t: t["timestamp"])
    curr_price = trades[0]["price"]
    curr_time = trades[0]["timestamp"]
    curr_size = 0 #first trade will always be true

    for trade in trades:
        if trade["price"] == curr_price:
            curr_size += trade["size"]
        else:
            sections.append({"time":curr_time, "size":curr_size, "price":curr_price})
            curr_price = trade["price"]
            curr_size = trade["size"]
            curr_time = trade["timestamp"]
    sections.append({"time":curr_time, "size":curr_size, "price":curr_price})
    return sections

def print_list(li):
    for i in li:
        print(i)

def calc_dollar_value(yes_price, shares):
    """
    Dollar cost of buying `shares` NO at given YES price.
    NO price = 1 - YES price.
    """
    return (1 - yes_price) * shares

def basic_buy_no_algo(trades, target=100):
    """
    Try to buy `target` dollars worth of NO shares.
    Assumes trades are chronological and all at same or better price
    until price changes.
    Returns (shares_bought, avg_price_paid).
    """
    if not trades:
        return 0,0
    spent = 0.0
    bought_shares = 0.0
    for tr in trades:
        price = float(tr["price"])
        size = float(tr["size"])
        available_value = calc_dollar_value(price, size)

        if available_value + spent <= target:
            spent += available_value
            bought_shares += size
        else:
            remaining = target - spent
            partial_shares = remaining / (1 - price)
            bought_shares += partial_shares
            spent += remaining
            print(f"Filled at NO={1-price:.3f} (YES={price:.3f})")
            return bought_shares, spent

    return bought_shares, spent


        
def go_through_it_all():
    offset_trade = 4811


def main():
    test2()
    """    offset_trade = 4811 + 55999
    result = []
    pl = 0
    markets = fetch_markets(50, offset_trade)
    filterd_markets = filter_markets(markets)
    for market in filterd_markets:
        trades = get_trade_for_market(market)
        corrected_trade = filter_no_trades(trades)
        compress = compress_blocks_conservative(corrected_trade)
        decision = basic_buy_no_algo(compress)
        outcome = json.loads(market["outcomePrices"])
        shares, spent = decision
        if spent > 0:
            if outcome == ["0","1"]:
                pl += (shares - spent)
                print("won")
            else:
                pl -= spent
                print("lost")
        result.append([market["question"], outcome, decision, pl])
        time.sleep(1)
    print_list(result)
    print(f"profit / loss {pl}")"""
    
    # Start near your discovered first-with-history offset
    #offset_history = 74669      #history start
    #offset_trade = 4811 + 39999       #trade start
    #markets = fetch_markets(20, offset_trade)
    #markets = markets[0]  # pass a single dict, not the list
    #look = filter_markets(markets)
    #for i in look:
    #    print(i["question"], i["outcomes"])
"""    print(markets)
    n = get_trade_for_market(markets)
    #p = calculate_price(n)
    filt = n
    filt = filter_no_trades(n)
    print(n[0])
    c = compress_trades(filt)
    print_list(c)
    print(f"size of filterd market {len(c)}")
    print(f"size of unfliterd market {len(markets)}")
"""



TICK = 0.01
def snap(p): return round(round(float(p)/TICK)*TICK, 2)

def is_take_no(tr):
    o = str(tr.get("outcome","")).strip().lower()
    s = str(tr.get("side","")).strip().upper()
    return (o == "no" and s == "BUY") or (o == "yes" and s == "SELL")

def notional_no(tr):
    p = float(tr["price"])
    q = float(tr["size"])
    return q * (1.0 - p)

def compress_blocks_conservative(trades, window_s=5, min_trade_notional=5.0, min_price_no=0.02):
    """
    Build lower-bound blocks for 'taking NO' with hard stops:
      - same snapped price
      - same direction (taking NO)
      - no intervening trade at a different price (any side)
      - time-bounded window (<= window_s from block start)
      - ignore dust trades (< min_trade_notional)
      - ignore extreme NO prices (< min_price_no) unless cumulative notional >= min_trade_notional within window
    Returns blocks: [{time, price_yes, shares, spent_no}]
    """
    if not trades: return []
    trades = sorted(trades, key=lambda t: t["timestamp"])

    blocks = []
    i = 0
    while i < len(trades):
        tr = trades[i]
        p_yes = snap(tr["price"])
        p_no  = round(1.0 - p_yes, 2)
        t0    = tr["timestamp"]

        # Only start a block if this trade is actually 'taking NO' and not dust/too extreme
        if not is_take_no(tr):
            i += 1
            continue
        if notional_no(tr) < min_trade_notional:
            i += 1
            continue
        if p_no < min_price_no:
            # allow starting only if we'll accumulate >= min_trade_notional within the window
            # we'll re-check after accumulation; skip for now
            pass

        shares = 0.0
        spent  = 0.0
        j = i
        ok = True
        while j < len(trades):
            tj = trades[j]
            pj = snap(tj["price"])
            # stop if time window exceeded
            if tj["timestamp"] - t0 > window_s: break
            # stop if any trade at a different price (ANY side) appears
            if pj != p_yes:
                break
            # only count 'taking NO' at exactly this price
            if is_take_no(tj):
                val = notional_no(tj)
                if val >= min_trade_notional:  # ignore dust prints
                    shares += float(tj["size"])
                    spent  += val
            j += 1

        # enforce extreme-price guard
        if p_no < min_price_no and spent < min_trade_notional:
            ok = False

        if ok and shares > 0:
            blocks.append({"time": t0, "price_yes": p_yes, "shares": shares, "spent_no": spent})

        i = max(j, i+1)

    return blocks

def buy_no_from_blocks(blocks, target=100.0):
    """
    Take blocks in chronological order; each block is 'no price move' size at that YES price.
    """
    bought_shares, spent = 0.0, 0.0
    for b in blocks:
        remaining = target - spent
        if remaining <= 0: break
        p_yes = b["price_yes"]
        p_no  = 1.0 - p_yes
        # how many shares can we still buy at this price?
        max_dollars_here = b["spent_no"]
        if spent + max_dollars_here <= target:
            # take full block
            spent += max_dollars_here
            bought_shares += b["shares"]
        else:
            # partial
            take = remaining
            bought_shares += take / p_no
            spent += take
            break
    return bought_shares, spent


FEE_BPS = 100  # 1%
def apply_fee(notional):
    return notional * (1 + FEE_BPS/10000.0)


def pnl_no(shares, spent, outcome_prices):  # outcome_prices like ["0","1"]
    if spent <= 0: return 0.0
    no_won = (outcome_prices == ["0","1"])
    return (shares - spent) if no_won else (-spent)


def test2(
    offset=4811 + 40000,
    limit=50,
    target_no_usd=100.0,
    fee_bps=100,              # 1% taker fee; set 0 to ignore
    window_s=5,               # block window (seconds)
    min_trade_notional=5.0,   # ignore dust (< $5 NO notional)
    min_price_no=0.02,        # ignore NO prices < 2% unless enough notional in-window
    sleep_s=0.3,
):
    """
    Runs: fetch_markets → filter_markets → get trades → filter NO-taking trades →
          compress into conservative blocks → simulate $ target NO buy → compute P&L.

    Returns (results_list, final_pl)
    results_list rows: [question, outcomePrices(list[str]), (shares, spent, avg_no), pnl, pl_running]
    """

    import time, json

    results = []
    pl_running = 0.0

    # 1) get some markets and filter down to Yes/No
    markets = fetch_markets(limit=limit, offset=offset)
    markets = filter_markets(markets)

    if not markets:
        print("[test2] no markets after filtering")
        return results, pl_running

    for m in markets:
        try:
            # 2) trades (paginate if you have get_trades_all, else fallback)
            condition_id = m["conditionId"]
            trades = get_trade_for_market(m)        # your original single-call version

            if not trades:
                results.append([m["question"], ["?", "?"], (0, 0, 0), 0.0, pl_running])
                continue

            # 3) only NO-taking prints
            no_trades = filter_no_trades(trades)

            # 4) conservative lower-bound blocks (no price move)
            blocks = compress_blocks_conservative(
                no_trades,
                window_s=window_s,
                min_trade_notional=min_trade_notional,
                min_price_no=min_price_no,
            )

            # 5) simulate buying $target_no_usd of NO against those blocks
            shares, spent = buy_no_from_blocks(blocks, target=target_no_usd)

            # fee (taker)
            if fee_bps and spent > 0:
                spent *= (1.0 + fee_bps / 10000.0)

            avg_no = (spent / shares) if shares else 0.0

            # 6) outcome + P&L
            outcome_prices = m["outcomePrices"]
            if isinstance(outcome_prices, str):
                outcome_prices = json.loads(outcome_prices)  # e.g. ["0","1"]

            pnl = pnl_no(shares, spent, outcome_prices)
            pl_running += pnl

            # 7) record + print
            row = [m["question"], outcome_prices, (shares, spent, avg_no), pnl, pl_running]
            results.append(row)
            print(row)

            time.sleep(sleep_s)

        except Exception as e:
            print(f"[test2] skip {m.get('question','<no title>')} due to error: {e}")
            continue

    print("profit / loss", pl_running)
    return results, pl_running


if __name__ == "__main__":
    main()