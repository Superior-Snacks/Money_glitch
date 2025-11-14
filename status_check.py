import os
import json
from datetime import datetime, timezone
from typing import List, Tuple

LOGS_DIR = "logs"


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
    for that cap. All values default to 0 if missing.
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
        dollars = float(st.get("dollars", 0.0))
    except Exception:
        dollars = 0.0
    try:
        trades = int(st.get("trades", 0))
    except Exception:
        trades = 0

    return shares, dollars, trades


def build_status_report(folder: str, cap: float) -> str:
    out_dir = os.path.join(LOGS_DIR, folder)
    open_path = os.path.join(out_dir, "log_open_markets_cap_spread.jsonl")
    closed_path = os.path.join(out_dir, "log_closed_markets_cap_spread.jsonl")

    open_rows = load_rows(open_path)
    closed_rows = load_rows(closed_path)
    all_rows = open_rows + closed_rows

    lines: List[str] = []
    lines.append("=== PROJECT STATUS REPORT (MAKER NO) ===")
    lines.append(f"Folder: {folder}")
    lines.append(f"Cap under analysis: {cap:.3f}")
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

    # ---- Cap-specific stats ----
    filled_markets = 0
    filled_open_markets = 0
    filled_closed_markets = 0

    closed_no_trades = 0
    open_no_trades = 0

    realized_pl = 0.0          # maker-NO realized P/L (closed markets only)
    total_premium = 0.0        # Σ dollars over all filled markets
    total_shares = 0.0         # Σ shares over all filled markets

    cost_entered_markets = 0.0  # Σ (shares - dollars) over all filled markets (max risk ever taken)
    locked_money_open = 0.0     # Σ (shares - dollars) over OPEN filled markets

    profitable_markets = 0      # closed filled markets with pl >= 0
    losing_markets = 0          # closed filled markets with pl < 0

    # For win-rate as “NO wins vs YES wins” among filled closed markets
    closed_no_filled = 0        # closed markets (with fills) that resolved NO
    closed_yes_filled = 0       # closed markets (with fills) that resolved YES

    for r in all_rows:
        status = r.get("status")
        is_closed = status in ("YES", "NO")
        is_open = not is_closed

        shares, dollars, trades = get_cap_entry(r, cap)

        has_trades = (trades > 0) or (shares > 0) or (dollars > 0)

        if is_closed and not has_trades:
            closed_no_trades += 1
        if is_open and not has_trades:
            open_no_trades += 1

        if not has_trades:
            continue  # nothing happened at this cap in this market

        # This market had fills at this cap
        filled_markets += 1
        total_premium += dollars
        total_shares += shares

        # Max risk for maker NO at this cap for this market
        max_risk = shares - dollars  # (1 - p)*s summed across trades

        cost_entered_markets += max_risk

        if is_open:
            filled_open_markets += 1
            locked_money_open += max_risk
        else:
            filled_closed_markets += 1

            # Track resolution side for win-rate among filled closed markets
            if status == "NO":
                closed_no_filled += 1
            else:  # YES
                closed_yes_filled += 1

            # Realized P/L for maker NO:
            #   Closed NO:   P/L = +premium
            #   Closed YES:  P/L = premium - shares
            if status == "NO":
                pl_mkt = dollars
            else:  # YES
                pl_mkt = dollars - shares

            realized_pl += pl_mkt
            if pl_mkt >= 0:
                profitable_markets += 1
            else:
                losing_markets += 1

    # ---- Summary section: cap-specific counts ----
    lines.append("=== CAP-SPECIFIC COUNTS (at this cap) ===")
    lines.append(f"Cap: {cap:.3f}")
    lines.append(f"Markets with ANY fills at this cap: {filled_markets}")
    lines.append(f"  Filled OPEN markets:   {filled_open_markets}")
    lines.append(f"  Filled CLOSED markets: {filled_closed_markets}")
    lines.append(f"Closed markets with NO trades at this cap: {closed_no_trades}")
    lines.append(f"Open markets with NO trades at this cap:   {open_no_trades}")
    lines.append("")

    # ---- Money / risk / P&L ----
    avg_price = (total_premium / total_shares) if total_shares > 0 else 0.0
    avg_risk_per_filled = (cost_entered_markets / filled_markets) if filled_markets > 0 else 0.0

    # New metrics:
    # 1) Win-rate (NO vs YES) among filled closed markets
    if filled_closed_markets > 0:
        win_rate_no = closed_no_filled / filled_closed_markets
    else:
        win_rate_no = 0.0

    # 2) P/L per filled closed market
    if filled_closed_markets > 0:
        pl_per_closed_market = realized_pl / filled_closed_markets
    else:
        pl_per_closed_market = 0.0

    # 3) Premium efficiency: realized P/L / total risk ever taken
    if cost_entered_markets > 0:
        premium_efficiency = realized_pl / cost_entered_markets
    else:
        premium_efficiency = 0.0

    lines.append("=== MONEY / RISK / P&L (maker NO, this cap) ===")
    lines.append(f"Total premium collected (all filled mkts): {total_premium:.2f}")
    lines.append(f"Total shares sold (all filled mkts):       {total_shares:.2f}")
    lines.append(f"Average effective price:                   {avg_price:.4f}")
    lines.append("")
    lines.append(f"Realized P/L (closed filled markets only): {realized_pl:.2f}")
    lines.append(
        f"  Profitable closed markets: {profitable_markets}, "
        f"Losing closed markets: {losing_markets}"
    )
    lines.append(
        f"  Win-rate (NO wins among filled closed mkts): "
        f"{win_rate_no*100:.2f}%  "
        f"(NO={closed_no_filled}, YES={closed_yes_filled})"
    )
    lines.append(
        f"  Avg P/L per filled closed market:       {pl_per_closed_market:.2f}"
    )
    lines.append(
        f"  Premium efficiency (realized P/L / total risk): "
        f"{premium_efficiency:.4f}"
    )
    lines.append("")
    lines.append(
        "Cost of entered markets (max risk ever taken across all filled markets): "
        f"{cost_entered_markets:.2f}  (Σ(shares - dollars))"
    )
    lines.append(
        "Locked money NOW (open positions only, worst-case if all go YES): "
        f"{locked_money_open:.2f}"
    )
    lines.append(
        "Average max risk per filled market: "
        f"{avg_risk_per_filled:.2f}"
    )
    lines.append("")

    lines.append("Note:")
    lines.append("  • 'Win-rate' here is the fraction of filled closed markets that resolved NO,")
    lines.append("    i.e. where your maker-NO side was correct.")
    lines.append("  • 'Premium efficiency' is how many P/L dollars you got per dollar of total")
    lines.append("    risk (Σ(shares - dollars)). It’s a crude efficiency ratio, not annualized.")
    lines.append("  • 'Cost of entered markets' is the total historical max loss exposure")
    lines.append("    for this cap strategy (over all markets where you actually had fills).")
    lines.append("  • 'Locked money NOW' is current worst-case loss if every open filled")
    lines.append("    market resolved YES from here.")
    lines.append("")

    return "\n".join(lines)


def main():
    folder = input("Folder name under logs/: ").strip()
    if not folder:
        print("Need a folder name.")
        return

    cap_str = input("Cap to analyze (e.g. 0.30): ").strip()
    try:
        cap = float(cap_str)
    except ValueError:
        print("Invalid cap.")
        return

    report = build_status_report(folder, cap)
    print(report)

    out_dir = os.path.join(LOGS_DIR, folder)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"status_report_cap_{cap:.3f}.txt")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)
        f.write("\n")

    print(f"\n[WRITE] Status report saved to: {out_path}")


if __name__ == "__main__":
    main()