import os
import json
from datetime import datetime, timezone
from typing import List, Tuple

LOGS_DIR = "logs"

# Toggle this if you want to see per-market debug later
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
    at that cap.

    - 'shares': total NO shares under this cap
    - 'dollars': total actual spend (sum p*s for p <= cap)
    - 'trades': number of NO trades counted into this bucket
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


def build_status_report(folder: str, cap: float, bet: float) -> str:
    """
    IMPORTANT: This assumes a MAKER-at-cap strategy.

    Semantics:
      - If there is at least `bet` dollars of under-cap NO notional
        (using the 'dollars' field in the cap spread), we assume:
          * You are a maker at exactly `cap`.
          * You get fully filled for exactly `bet` dollars.
          * Fill price is `cap`. No price improvement.
      - If the market resolves NO: P/L = bet * (1/cap - 1)
      - If the market resolves YES: P/L = -bet
      - If there isn't enough liquidity under cap to support bet: you never enter.
    """
    out_dir = os.path.join(LOGS_DIR, folder)
    open_path = os.path.join(out_dir, "log_open_markets_cap_spread.jsonl")
    closed_path = os.path.join(out_dir, "log_closed_markets_cap_spread.jsonl")

    open_rows = load_rows(open_path)
    closed_rows = load_rows(closed_path)
    all_rows = open_rows + closed_rows

    lines: List[str] = []
    lines.append("=== PROJECT STATUS REPORT (BUY NO, LIMIT MAKER AT CAP) ===")
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

    # ---- Cap-specific stats (MAKER at cap, full-bet semantics) ----

    markets_any_liq = 0                 # any liquidity at this cap
    markets_entered = 0                 # markets where full bet can be filled
    markets_entered_open = 0
    markets_entered_closed = 0

    closed_no_liq = 0                   # closed markets with 0 liq at cap
    open_no_liq = 0                     # open markets with 0 liq at cap

    closed_insufficient_liq = 0         # has liq but < bet
    open_insufficient_liq = 0

    locked_money_open = 0.0             # money at risk in open entered markets
    total_cost_entered = 0.0            # Σ bet across entered markets

    closed_profitable_markets = 0       # NO + full fill
    closed_losing_markets = 0           # YES + full fill
    realized_pl = 0.0                   # net realized P/L on closed entered markets

    profit_per_win = bet * (1.0 / cap - 1.0)  # e.g. cap=0.4, bet=10 → 15

    if DEBUG:
        print(
            "\n=== PER-MARKET DEBUG (MAKER at cap={:.3f}, bet={:.2f}) ==="
            .format(cap, bet)
        )

    for r in all_rows:
        cid = r.get("conditionId", "unknown")
        q = r.get("question", "").strip()
        status = r.get("status")
        is_closed = status in ("YES", "NO")
        is_open = not is_closed

        full_shares, full_cost, trades = get_cap_entry(r, cap)

        has_liquidity = (trades > 0) or (full_shares > 0) or (full_cost > 0.0)
        if has_liquidity:
            markets_any_liq += 1

        debug_info = {
            "cid": cid,
            "status": status,
            "full_shares": full_shares,
            "full_cost": full_cost,
            "trades": trades,
            "has_liq": has_liquidity,
            "entered": False,
            "reason": "",
        }

        # No liquidity at all for this cap
        if not has_liquidity:
            if is_closed:
                closed_no_liq += 1
            else:
                open_no_liq += 1

            debug_info["reason"] = "no_liquidity"
            if DEBUG:
                print(
                    f"[DEBUG] cid={cid[:10]} status={status:3} "
                    f"→ NO LIQ at cap; skip"
                )
            continue

        # Check if there is enough under-cap notional to support a full bet.
        # We use 'full_cost' (sum p*s for p <= cap) as conservative proxy:
        # if full_cost >= bet, then there was at least bet of actual volume
        # in the under-cap region, so being a maker at cap should allow full fill.
        liquidity_sufficient = full_cost >= bet - 1e-9

        if not liquidity_sufficient:
            if is_closed:
                closed_insufficient_liq += 1
            else:
                open_insufficient_liq += 1

            debug_info["reason"] = "liq<bet"
            if DEBUG:
                print(
                    f"[DEBUG] cid={cid[:10]} status={status:3} "
                    f"full_cost={full_cost:.4f} < bet={bet:.2f} → INSUFFICIENT liq; skip"
                )
            continue

        # At this point, we *enter* the market as a maker at cap with full bet.
        markets_entered += 1
        total_cost_entered += bet
        debug_info["entered"] = True
        debug_info["reason"] = "entered_full_bet"

        if is_open:
            markets_entered_open += 1
            locked_money_open += bet
        else:
            markets_entered_closed += 1
            # Closed + filled:
            if status == "NO":
                closed_profitable_markets += 1
                realized_pl += profit_per_win
            elif status == "YES":
                closed_losing_markets += 1
                realized_pl -= bet

        if DEBUG:
            print(
                f"[DEBUG] cid={cid[:10]} status={status:3} ENTERED "
                f"(full_cost={full_cost:.4f}, bet={bet:.2f}, "
                f"closed={is_closed}) reason={debug_info['reason']}"
            )

    # ---- Summary section ----
    lines.append("=== CAP-SPECIFIC COUNTS (MAKER-at-cap semantics) ===")
    lines.append(f"Cap: {cap:.3f}")
    lines.append(f"Markets with ANY liquidity at this cap:         {markets_any_liq}")
    lines.append(f"Markets you WOULD HAVE ENTERED (full bet fill): {markets_entered}")
    lines.append(f"  Entered OPEN markets:                         {markets_entered_open}")
    lines.append(f"  Entered CLOSED markets:                       {markets_entered_closed}")
    lines.append("")
    lines.append(f"Closed markets with NO liquidity at this cap:       {closed_no_liq}")
    lines.append(f"Open markets with NO liquidity at this cap:         {open_no_liq}")
    lines.append(f"Closed markets with INSUFFICIENT liq (< bet):       {closed_insufficient_liq}")
    lines.append(f"Open markets with INSUFFICIENT liq (< bet):         {open_insufficient_liq}")
    lines.append("")

    lines.append("=== MONEY / RISK / P&L (BUY NO as MAKER at cap) ===")
    lines.append(f"Total cost spent (entered markets):              {total_cost_entered:.2f}")
    lines.append(
        f"Locked money in OPEN entered markets (max loss): {locked_money_open:.2f}"
    )
    lines.append(
        f"Closed profitable markets (NO, full fill):       {closed_profitable_markets}"
    )
    lines.append(
        f"Closed losing markets (YES, full fill):          {closed_losing_markets}"
    )
    lines.append(
        f"Per winning NO market P/L (cap={cap:.3f}, bet={bet:.2f}): "
        f"{profit_per_win:.2f}"
    )
    if realized_pl >= 0:
        lines.append(f"Net realized P/L on CLOSED entered markets:      +{realized_pl:.2f}")
    else:
        lines.append(f"Net realized P/L on CLOSED entered markets:      {realized_pl:.2f}")

    return "\n".join(lines)


def main():
    # You can hard-code these like before if you want:
    # folder = "gather_markets"
    # cap = 0.4
    # bet = 10.0

    folder = input("Folder name under logs/: ").strip() or "gather_markets"
    if not folder:
        print("Need a folder name.")
        return

    cap_str = input("Cap to analyze (e.g. 0.40) [0.40]: ").strip() or "0.40"
    try:
        cap = float(cap_str)
    except ValueError:
        print("Invalid cap.")
        return

    bet_str = input("Max spend per market (bet), e.g. 10 [10]: ").strip() or "10"
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
    out_path = os.path.join(out_dir, f"status_report_cap_{cap:.3f}_bet_{bet:.2f}_maker.txt")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)
        f.write("\n")

    print(f"\n[WRITE] Status report saved to: {out_path}")


if __name__ == "__main__":
    main()