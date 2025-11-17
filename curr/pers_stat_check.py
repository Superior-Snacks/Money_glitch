import os
import json
from datetime import datetime, timezone
from typing import List, Tuple

LOGS_DIR = "logs"

# Toggle this if you want to silence debug later
DEBUG = True


def dt_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_rows(path: str) -> List[dict]:
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    return rows


def get_cap_entry(row: dict, cap: float) -> Tuple[float, float, int]:
    """
    For a given market row and cap, return (shares, dollars, trades)
    at that cap. 'dollars' here is the total cost you'd pay if you
    bought all NO-shares under this cap at actual trade prices.
    """
    spread = row.get("cap_spread") or {}
    caps_dict = spread.get("caps") or {}

    key = f"{cap:.3f}"
    st = caps_dict.get(key) or caps_dict.get(str(cap))
    if not st:
        return 0.0, 0.0, 0

    try:
        shares = float(st.get("shares", 0.0))
    except Exception:
        shares = 0.0
    try:
        dollars = float(st.get("dollars", 0.0))  # cost if you bought all that NO
    except Exception:
        dollars = 0.0
    try:
        trades = int(st.get("trades", 0))
    except Exception:
        trades = 0

    return shares, dollars, trades


def build_status_report(folder: str, cap: float, bet: float) -> str:
    out_dir = os.path.join(LOGS_DIR, folder)
    open_path = os.path.join(out_dir, "log_open_markets_cap_spread.jsonl")
    closed_path = os.path.join(out_dir, "log_closed_markets_cap_spread.jsonl")

    open_rows = load_rows(open_path)
    closed_rows = load_rows(closed_path)
    all_rows = open_rows + closed_rows

    lines: List[str] = []
    lines.append("=== PROJECT STATUS REPORT (BUY NO, LIMIT MAKER, FIXED BET) ===")
    lines.append(f"Folder: {folder}")
    lines.append(f"Cap under analysis: {cap:.3f}")
    lines.append(f"Max spend per market (bet): {bet:.2f}")
    lines.append(f"Generated at: {dt_iso()}")
    lines.append("")

    if not all_rows:
        lines.append("No data found yet (run the main cap-spread script first).")
        return "\n".join(lines)

    # ---- Global market counts (independent of cap) ----
    total_markets = len(all_rows)
    closed_markets = 0
    open_markets = 0

    for r in all_rows:
        status = r.get("status")
        if status in ("YES", "NO"):
            closed_markets += 1
        else:
            open_markets += 1

    lines.append("=== GLOBAL COUNTS (all markets) ===")
    lines.append(f"Total markets in index: {total_markets}")
    lines.append(f"  Open (TBD/other): {open_markets}")
    lines.append(f"  Closed (YES/NO):  {closed_markets}")
    lines.append("")

    # ---- Cap-specific stats (using effective bet per market) ----
    filled_markets = 0              # markets where you actually spend >0 at this cap
    filled_open_markets = 0
    filled_closed_markets = 0

    closed_no_trades = 0
    open_no_trades = 0

    realized_pl = 0.0               # realized P/L on closed entered markets
    total_cost = 0.0                # Σ effective_cost over all entered markets
    total_shares = 0.0              # Σ eff_shares over all entered markets

    cost_entered_markets = 0.0      # same as total_cost

    locked_money_open = 0.0         # Σ effective_cost over OPEN entered markets

    profitable_markets = 0          # closed entered markets with pl >= 0
    losing_markets = 0              # closed entered markets with pl < 0

    closed_no_filled = 0            # closed entered markets that resolved NO
    closed_yes_filled = 0           # closed entered markets that resolved YES

    # ---- Per-market debug header ----
    if DEBUG:
        print("\n=== PER-MARKET DEBUG (BUY NO, cap={:.3f}, bet={:.2f}) ===".format(cap, bet))

    for r in all_rows:
        cid = r.get("conditionId", "unknown")
        q = r.get("question", "").strip()
        status = r.get("status")
        is_closed = status in ("YES", "NO")
        is_open = not is_closed

        full_shares, full_cost, trades = get_cap_entry(r, cap)
        has_liquidity = (trades > 0) or (full_shares > 0) or (full_cost > 0)

        debug_info = {
            "cid": cid,
            "status": status,
            "has_liq": has_liquidity,
            "full_shares": full_shares,
            "full_cost": full_cost,
            "trades": trades,
            "entered": False,
            "effective_cost": 0.0,
            "scale": 0.0,
            "eff_shares": 0.0,
            "pl_if_NO": 0.0,
            "pl_if_YES": 0.0,
            "reason": "",
        }

        if is_closed and not has_liquidity:
            closed_no_trades += 1
            debug_info["reason"] = "closed_no_liquidity"
        if is_open and not has_liquidity:
            open_no_trades += 1
            if not debug_info["reason"]:
                debug_info["reason"] = "open_no_liquidity"

        if not has_liquidity:
            if DEBUG:
                print(f"[DEBUG] cid={cid[:10]} status={status:3} → no liquidity at cap; skipped")
            continue  # nothing available at this cap in this market

        if full_cost <= 0:
            debug_info["reason"] = "full_cost<=0"
            if DEBUG:
                print(
                    f"[DEBUG] cid={cid[:10]} status={status:3} "
                    f"full_shares={full_shares:.4f} full_cost={full_cost:.4f} → invalid full_cost; skipped"
                )
            continue  # degenerate case

        # You don't spend more than bet
        if (full_shares * cap) >= bet:
            effective_cost = bet
        else:
            effective_cost = full_shares * cap
        if effective_cost <= 0:
            debug_info["reason"] = "effective_cost<=0"
            if DEBUG:
                print(
                    f"[DEBUG] cid={cid[:10]} status={status:3} full_cost={full_cost:.4f} "
                    f"→ effective_cost={effective_cost:.4f} <= 0; skipped"
                )
            continue  # should not happen given bet > 0

        # Aggregate stats now use eff_shares and effective_cost
        filled_markets += 1
        total_cost += effective_cost
        cost_entered_markets += effective_cost

        if effective_cost == bet:
            single_market_potential = bet // cap
            single_market_cost = bet
        else:
            single_market_potential = full_shares
            single_market_cost = effective_cost


        if is_open:
            filled_open_markets += 1
            locked_money_open += effective_cost  # max loss if YES
        else:
            filled_closed_markets += 1
            #p/l calk
            if status == "NO":
                profitable_markets += 1
                pl += single_market_potential
            elif status == "YES":
                losing_markets += 1
                pl -= single_market_cost

    # ---- Summary section: cap-specific counts ----
    lines.append("=== CAP-SPECIFIC COUNTS (at this cap, with bet) ===")
    lines.append(f"Cap: {cap:.3f}")
    lines.append(f"Markets you WOULD HAVE ENTERED at this cap: {filled_markets}")
    lines.append(f"  Entered OPEN markets:   {filled_open_markets}")
    lines.append(f"  Entered CLOSED markets: {filled_closed_markets}")
    lines.append(f"Closed markets with NO liquidity at this cap: {closed_no_trades}")
    lines.append(f"Open markets with NO liquidity at this cap:   {open_no_trades}")
    lines.append("")

    # ---- Money / risk / P&L ----

    lines.append("=== MONEY / RISK / P&L (BUY NO, this cap, with bet) ===")
    lines.append(f"Total cost spent (entered markets):         {total_cost:.2f}")
    lines.append(f"Total NO-shares bought (entered markets):   {total_shares:.2f}")
    if pl <= 0:
        lines.append(f"p/l {pl:+.2f}")
    else:
        lines.append(f"p/l {pl:-.2f}")



    return "\n".join(lines)


def main():
    #folder = input("Folder name under logs/: ").strip()
    folder = "gather_markets"
    if not folder:
        print("Need a folder name.")
        return

    #cap_str = input("Cap to analyze (e.g. 0.30): ").strip()
    cap_str = "0.5"
    try:
        cap = float(cap_str)
    except ValueError:
        print("Invalid cap.")
        return

    #bet_str = input("Max spend per market (bet), e.g. 30: ").strip()
    bet_str = "5"
    try:
        bet = float(bet_str)
    except ValueError:
        print("Invalid bet.")
        return
    if bet <= 0:
        print("Bet must be > 0.")
        return

    report = build_status_report(folder, cap, bet)
    print(report)

    out_dir = os.path.join(LOGS_DIR, folder)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"status_report_cap_{cap:.3f}_bet_{bet:.2f}.txt")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)
        f.write("\n")

    print(f"\n[WRITE] Status report saved to: {out_path}")


if __name__ == "__main__":
    main()