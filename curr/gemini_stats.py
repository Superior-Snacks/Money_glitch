import os
import json
import argparse
from typing import List, Dict

# ======================= Configuration =======================
DEFAULT_BET_SIZE = 100.0  # Max USDC to deploy per market per cap
# ============================================================

def load_closed_markets(folder_path: str) -> List[Dict]:
    """Loads only closed markets (YES/NO) from the folder."""
    path = os.path.join("logs", folder_path, "closed.jsonl")
    data = []
    if not os.path.exists(path):
        print(f"[ERROR] Could not find {path}")
        return []
    
    print(f"Loading data from {path}...")
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip(): continue
            try:
                row = json.loads(line)
                # Double check status just in case
                if row.get('status') in ('YES', 'NO'):
                    data.append(row)
            except:
                continue
    return data

def analyze_performance(markets: List[Dict], bet_size: float):
    """
    Simulates a maker strategy:
    - Place a Buy Limit Order for NO at specific CAPS.
    - Order Size = bet_size (USDC).
    - If historical volume < bet_size, we get partial fill.
    - If historical volume >= bet_size, we get full fill.
    """
    
    # Initialize stats buckets for each cap found in the data
    # Structure: cap -> {stats}
    stats = {}

    total_markets = len(markets)
    global_yes = sum(1 for m in markets if m['status'] == 'YES')
    global_no  = sum(1 for m in markets if m['status'] == 'NO')

    print(f"\nAnalyzing {total_markets} closed markets.")
    print(f"Global Outcome Distribution: YES: {global_yes} | NO: {global_no}")
    print(f"Simulation: Constant Bet of ${bet_size} USDC at every Cap level.\n")

    # 1. Accumulate Data
    for m in markets:
        outcome = m['status'] # YES or NO
        caps_data = m.get('cap_spread', {}).get('caps', {})

        for cap_str, metrics in caps_data.items():
            cap_price = float(cap_str)
            
            if cap_price not in stats:
                stats[cap_price] = {
                    "filled_markets": 0,
                    "total_invested": 0.0,
                    "total_revenue": 0.0,
                    "wins": 0,
                    "losses": 0
                }
            
            # AVAILABLE LIQUIDITY
            # metrics['dollars'] is the total historical volume traded BELOW or AT this price.
            # If volume was $50 and our bet is $100, we only filled $50.
            available_liquidity = metrics['dollars']
            
            if available_liquidity <= 0.001:
                continue

            # CALCULATE FILL
            fill_usdc = min(available_liquidity, bet_size)
            
            # SHARES ACQUIRED
            # We assume we bought AT the cap price (Limit Order).
            shares = fill_usdc / cap_price
            
            stats[cap_price]["filled_markets"] += 1
            stats[cap_price]["total_invested"] += fill_usdc

            # CALCULATE PAYOUT
            if outcome == "NO":
                # We bought NO, and it resolved NO. Payout $1.00 per share.
                revenue = shares * 1.0
                stats[cap_price]["total_revenue"] += revenue
                stats[cap_price]["wins"] += 1
            else:
                # We bought NO, and it resolved YES. Payout $0.00.
                stats[cap_price]["total_revenue"] += 0.0
                stats[cap_price]["losses"] += 1

    # 2. Process & Format Output
    # Sort by cap price
    sorted_caps = sorted(stats.keys())

    print(f"{'CAP':<8} | {'FILL %':<8} | {'WIN RATE':<10} | {'INVESTED':<12} | {'PROFIT($)':<12} | {'ROI %':<8} | {'EV/MKT':<8}")
    print("-" * 90)

    best_roi = -9999
    best_cap = 0.0

    for cap in sorted_caps:
        s = stats[cap]
        
        invested = s["total_invested"]
        if invested == 0: continue

        revenue = s["total_revenue"]
        profit = revenue - invested
        roi = (profit / invested) * 100
        
        # Win rate based on markets where we actually got a fill
        fills = s["filled_markets"]
        win_rate = (s["wins"] / fills) * 100 if fills > 0 else 0
        
        # Fill Rate: What % of TOTAL markets did we get at least some execution in?
        fill_rate_global = (fills / total_markets) * 100

        # Expected Value per Market (Total Profit / Total Markets Analyzed)
        ev_per_market = profit / total_markets

        if roi > best_roi and fills > 5: # Filter noise
            best_roi = roi
            best_cap = cap

        # Colorizing output (standard terminal colors)
        # Green for profit, Red for loss
        p_str = f"{profit:+.2f}"
        roi_str = f"{roi:+.2f}%"
        
        print(f"{cap:<8.3f} | {fill_rate_global:<7.1f}% | {win_rate:<9.1f}% | ${invested:<11.2f} | ${p_str:<11} | {roi_str:<8} | ${ev_per_market:+.4f}")

    print("-" * 90)
    print(f"\nSUMMARY OBSERVATIONS:")
    print(f"1. Best ROI Strategy: Buying NO at {best_cap:.3f} yielded {best_roi:.2f}% ROI.")
    print(f"2. Note: 'Fill %' indicates liquidity. Low caps have huge ROI but rarely get filled.")
    print(f"3. This assumes you held to resolution (Expiry).")

def main():
    parser = argparse.ArgumentParser(description="Analyze P/L from Polymarket Scraper Logs")
    parser.add_argument("folder", help="Path to the log folder (e.g., logs/daysback_30)")
    parser.add_argument("--bet", type=float, default=DEFAULT_BET_SIZE, help="Constant bet size in USDC (default 100)")
    
    args = parser.parse_args()
    
    data = load_closed_markets(args.folder)
    if not data:
        print("No closed markets found to analyze.")
        return

    analyze_performance(data, args.bet)

if __name__ == "__main__":
    main()