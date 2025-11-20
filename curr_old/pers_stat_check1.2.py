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
    at that cap. 'dollars' is the historical notional that traded
    under or at that cap, but for the maker-style sim we only really
    need 'shares' and 'trades'.
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


def build_spread_caps(top_cap: float, min_cap: float = 0.2, step: float = 0.1) -> List[float]:
    """
    Dynamically build a descending list of caps:
        [top_cap, top_cap-step, ..., >= min_cap]
    Example:
      top_cap=0.5 → [0.5, 0.4, 0.3, 0.2]
      top_cap=0.4 → [0.4, 0.3, 0.2]
      top_cap=0.25 → [0.25, 0.2]
    If top_cap < min_cap, we just use [top_cap].
    """
    caps: List[float] = []
    if top_cap < min_cap:
        return [round(top_cap, 3)]

    c = top_cap
    while c >= min_cap - 1e-9:
        caps.append(round(c, 3))
        c -= step
    # Avoid duplicates / weird rounding
    seen = set()
    out = []
    for x in caps:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


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
    closed_yes = 0
    closed_no = 0
    open_markets = 0

    for r in all_rows:
        status = r.get("status")
        if status == "YES":
            closed_yes += 1
        elif status == "NO":
            closed_no += 1
        else:
            open_markets += 1

    closed_markets = closed_yes + closed_no

    lines.append("=== GLOBAL COUNTS (all markets) ===")
    lines.append(f"Total markets in index: {total_markets}")
    lines.append(f"  Open (TBD/other): {open_markets}")
    lines.append(f"  Closed (YES/NO):  {closed_markets}")
    lines.append(f"    Closed YES:     {closed_yes}")
    lines.append(f"    Closed NO:      {closed_no}")
    lines.append("")

    # caps for "bet spread" method
    spread_caps = build_spread_caps(cap, min_cap=0.2, step=0.1)

    if DEBUG:
        print(f"\n[DEBUG] Spread caps for bet-split: {spread_caps}")

    # ======================================================
    # 1) SINGLE-CAP MAKER-AT-CAP SEMANTICS (FULL FILL)
    # ======================================================
    markets_with_liq = 0
    markets_full_fill = 0
    entered_open = 0
    entered_closed = 0

    closed_no_liq = 0
    open_no_liq = 0
    closed_insufficient = 0
    open_insufficient = 0

    realized_pl_full = 0.0
    locked_open_full = 0.0

    profitable_full = 0
    losing_full = 0

    # ======================================================
    # 2) SINGLE-CAP MAKER-AT-CAP PARTIAL-FILL
    # ======================================================
    markets_partial_entered = 0
    entered_open_partial = 0
    entered_closed_partial = 0
    realized_pl_partial = 0.0
    locked_open_partial = 0.0

    profitable_partial = 0
    losing_partial = 0

    # Precompute required shares for full fill at this cap
    shares_needed_full = bet / cap

    if DEBUG:
        print("\n=== PER-MARKET DEBUG (SINGLE CAP, maker-at-cap) ===")

    for r in all_rows:
        cid = r.get("conditionId", "unknown")
        q = r.get("question", "").strip()
        status = r.get("status")
        is_closed = status in ("YES", "NO")
        is_open = not is_closed

        full_shares, full_cost_hist, trades = get_cap_entry(r, cap)
        has_liquidity = (trades > 0) or (full_shares > 0) or (full_cost_hist > 0)

        if not has_liquidity:
            if is_closed:
                closed_no_liq += 1
            else:
                open_no_liq += 1
            if DEBUG:
                print(f"[SINGLE] cid={cid[:10]} status={status:3} → no liquidity at cap; skip")
            continue

        markets_with_liq += 1

        # ---- FULL-FILL semantics ----
        if full_shares >= shares_needed_full:
            # We assume we can place a limit at `cap` and get filled
            markets_full_fill += 1
            if is_open:
                entered_open += 1
                locked_open_full += bet
            else:
                entered_closed += 1
                # script A logic: NO pays 1, YES pays 0
                # shares = bet / cap
                shares = shares_needed_full
                cost = bet
                if status == "NO":
                    payout = shares * 1.0
                    pl = payout - cost
                    realized_pl_full += pl
                    profitable_full += 1
                else:  # YES
                    payout = 0.0
                    pl = payout - cost
                    realized_pl_full += pl
                    losing_full += 1
        else:
            # Insufficient liquidity for full bet
            if is_closed:
                closed_insufficient += 1
            else:
                open_insufficient += 1

        # ---- PARTIAL-FILL semantics ----
        # Enter as long as there are any shares available.
        if full_shares > 0:
            markets_partial_entered += 1
            # we buy up to bet, but capped by available shares at this price
            max_cost_possible = full_shares * cap
            effective_cost = min(bet, max_cost_possible)

            if effective_cost <= 0:
                if DEBUG:
                    print(
                        f"[SINGLE-PARTIAL] cid={cid[:10]} status={status:3} "
                        f"full_shares={full_shares:.4f} → effective_cost=0; skipped"
                    )
                continue

            shares_eff = effective_cost / cap

            if is_open:
                entered_open_partial += 1
                locked_open_partial += effective_cost
            else:
                entered_closed_partial += 1
                if status == "NO":
                    payout = shares_eff * 1.0
                    pl = payout - effective_cost
                    realized_pl_partial += pl
                    if pl >= 0:
                        profitable_partial += 1
                    else:
                        losing_partial += 1
                else:  # YES
                    payout = 0.0
                    pl = payout - effective_cost
                    realized_pl_partial += pl
                    losing_partial += 1

        if DEBUG and (full_shares > 0 or has_liquidity):
            print(
                f"[SINGLE] cid={cid[:10]} status={status:3} trades={trades:4d} "
                f"full_shares={full_shares:.4f}"
            )

    # ---------- Single-cap summary ----------
    lines.append("=== CAP-SPECIFIC COUNTS (MAKER-at-cap FULL-FILL) ===")
    lines.append(f"Cap: {cap:.3f}")
    lines.append(f"Markets with ANY liquidity at this cap:         {markets_with_liq}")
    lines.append(f"Markets you WOULD HAVE ENTERED (full bet fill): {markets_full_fill}")
    lines.append(f"  Entered OPEN markets:                         {entered_open}")
    lines.append(f"  Entered CLOSED markets:                       {entered_closed}")
    lines.append("")
    lines.append(f"Closed markets with NO liquidity at this cap:       {closed_no_liq}")
    lines.append(f"Open markets with NO liquidity at this cap:         {open_no_liq}")
    lines.append(f"Closed markets with INSUFFICIENT liq (< bet):       {closed_insufficient}")
    lines.append(f"Open markets with INSUFFICIENT liq (< bet):         {open_insufficient}")
    lines.append("")

    per_win_full = bet * (1.0 / cap - 1.0)
    lines.append("=== MONEY / RISK / P&L (BUY NO as MAKER at cap; FULL FILL) ===")
    lines.append(f"Total cost spent (entered markets):              {markets_full_fill * bet:.2f}")
    lines.append(f"Locked money in OPEN entered markets (max loss): {locked_open_full:.2f}")
    lines.append(f"Closed profitable markets (NO, full fill):       {profitable_full}")
    lines.append(f"Closed losing markets (YES, full fill):          {losing_full}")
    lines.append(f"Per winning NO market P/L (cap={cap:.3f}, bet={bet:.2f}): {per_win_full:.2f}")
    lines.append(f"Net realized P/L on CLOSED entered markets:      {realized_pl_full:+.2f}")
    lines.append("")

    # ---------- Single-cap PARTIAL summary ----------
    lines.append("=== MONEY / RISK / P&L (BUY NO as MAKER at cap; PARTIAL FILL) ===")
    lines.append(f"Markets you WOULD HAVE ENTERED (partial):        {markets_partial_entered}")
    lines.append(f"  Entered OPEN markets (partial):                {entered_open_partial}")
    lines.append(f"  Entered CLOSED markets (partial):              {entered_closed_partial}")
    lines.append(f"Locked money in OPEN entered markets (partial):  {locked_open_partial:.2f}")
    lines.append(f"Closed profitable markets (partial):             {profitable_partial}")
    lines.append(f"Closed losing markets (partial):                 {losing_partial}")
    lines.append(f"Net realized P/L on CLOSED (partial):            {realized_pl_partial:+.2f}")
    lines.append("")

    # ======================================================
    # 3) BET-SPREAD METHOD (FULL FILL ACROSS CAPS)
    # ======================================================
    # We split `bet` evenly across spread_caps, and require
    # full fill at EACH cap for the "full-fill" version.
    # Then we also compute a partial-fill version where each
    # leg can be partially filled independently.
    # ======================================================
    if DEBUG:
        print("\n=== PER-MARKET DEBUG (BET-SPREAD) ===")

    if spread_caps:
        bet_per_cap = bet / len(spread_caps)
    else:
        bet_per_cap = bet

    # FULL-FILL spread stats
    spread_markets_with_liq = 0
    spread_markets_full_fill = 0
    spread_entered_open = 0
    spread_entered_closed = 0

    spread_closed_no_liq = 0
    spread_open_no_liq = 0
    spread_closed_insufficient = 0
    spread_open_insufficient = 0

    spread_realized_pl_full = 0.0
    spread_locked_open_full = 0.0
    spread_profitable_full = 0
    spread_losing_full = 0

    # PARTIAL-FILL spread stats
    spread_markets_partial_entered = 0
    spread_entered_open_partial = 0
    spread_entered_closed_partial = 0
    spread_realized_pl_partial = 0.0
    spread_locked_open_partial = 0.0
    spread_profitable_partial = 0
    spread_losing_partial = 0

    for r in all_rows:
        cid = r.get("conditionId", "unknown")
        status = r.get("status")
        is_closed = status in ("YES", "NO")
        is_open = not is_closed

        # For spread, we need to check liquidity at *each* cap
        per_cap_shares = []
        per_cap_has_liq = []
        all_caps_have_zero = True

        for c in spread_caps:
            sh, _, tr = get_cap_entry(r, c)
            has_liq = (tr > 0) or (sh > 0)
            per_cap_shares.append(sh)
            per_cap_has_liq.append(has_liq)
            if has_liq:
                all_caps_have_zero = False

        if all_caps_have_zero:
            if is_closed:
                spread_closed_no_liq += 1
            else:
                spread_open_no_liq += 1
            if DEBUG:
                print(f"[SPREAD] cid={cid[:10]} status={status:3} → no liq at ANY spread caps")
            continue

        spread_markets_with_liq += 1

        # ---------- FULL-FILL spread ----------
        # Need full fill at every cap
        full_fill_all_caps = True
        for sh, c in zip(per_cap_shares, spread_caps):
            needed_sh = bet_per_cap / c
            if sh < needed_sh:
                full_fill_all_caps = False
                break

        if full_fill_all_caps:
            spread_markets_full_fill += 1
            total_cost = bet  # sum of bet_per_cap across caps

            if is_open:
                spread_entered_open += 1
                spread_locked_open_full += total_cost
            else:
                spread_entered_closed += 1
                # Each leg is like script A: shares = bet_leg/c, payout = shares*1.0 on NO; 0 on YES.
                if status == "NO":
                    payout = 0.0
                    for c in spread_caps:
                        shares_leg = bet_per_cap / c
                        payout += shares_leg * 1.0
                    pl = payout - total_cost
                    spread_realized_pl_full += pl
                    spread_profitable_full += 1
                else:  # YES
                    payout = 0.0
                    pl = payout - total_cost
                    spread_realized_pl_full += pl
                    spread_losing_full += 1
        else:
            if is_closed:
                spread_closed_insufficient += 1
            else:
                spread_open_insufficient += 1

        # ---------- PARTIAL-FILL spread ----------
        # For partial, we enter any cap that has >0 shares, up to bet_per_cap at that cap.
        any_leg_entered = False
        total_cost_partial = 0.0
        total_payout_partial = 0.0

        for sh, c in zip(per_cap_shares, spread_caps):
            if sh <= 0:
                continue
            max_cost_here = sh * c
            eff_cost = min(bet_per_cap, max_cost_here)
            if eff_cost <= 0:
                continue

            any_leg_entered = True
            shares_eff = eff_cost / c
            total_cost_partial += eff_cost

            if is_closed and status == "NO":
                total_payout_partial += shares_eff * 1.0
            elif is_closed and status == "YES":
                total_payout_partial += 0.0

        if not any_leg_entered:
            # no leg had >0 shares
            continue

        spread_markets_partial_entered += 1

        if is_open:
            spread_entered_open_partial += 1
            spread_locked_open_partial += total_cost_partial
        else:
            spread_entered_closed_partial += 1
            pl = total_payout_partial - total_cost_partial
            spread_realized_pl_partial += pl
            if pl >= 0:
                spread_profitable_partial += 1
            else:
                spread_losing_partial += 1

    # ---------- Spread FULL-FILL summary ----------
    lines.append("=== BET-SPREAD COUNTS (FULL-FILL MAKER at spread caps) ===")
    lines.append(f"Spread caps: {', '.join(f'{c:.3f}' for c in spread_caps)}")
    lines.append(f"Markets with ANY liquidity at these caps:        {spread_markets_with_liq}")
    lines.append(f"Markets you WOULD HAVE ENTERED (full bet fill):  {spread_markets_full_fill}")
    lines.append(f"  Entered OPEN markets:                          {spread_entered_open}")
    lines.append(f"  Entered CLOSED markets:                        {spread_entered_closed}")
    lines.append("")
    lines.append(f"Closed markets with NO liquidity at spread caps: {spread_closed_no_liq}")
    lines.append(f"Open markets with NO liquidity at spread caps:   {spread_open_no_liq}")
    lines.append(f"Closed markets with INSUFFICIENT liq (< bet):    {spread_closed_insufficient}")
    lines.append(f"Open markets with INSUFFICIENT liq (< bet):      {spread_open_insufficient}")
    lines.append("")
    lines.append("=== MONEY / RISK / P&L (BET-SPREAD, FULL-FILL) ===")
    lines.append(f"Total cost spent (entered markets):              {spread_markets_full_fill * bet:.2f}")
    lines.append(f"Locked money in OPEN entered markets:            {spread_locked_open_full:.2f}")
    lines.append(f"Closed profitable markets (NO, full fill):       {spread_profitable_full}")
    lines.append(f"Closed losing markets (YES, full fill):          {spread_losing_full}")
    lines.append(f"Net realized P/L on CLOSED entered markets:      {spread_realized_pl_full:+.2f}")
    lines.append("")

    # ---------- Spread PARTIAL summary ----------
    lines.append("=== MONEY / RISK / P&L (BET-SPREAD, PARTIAL-FILL) ===")
    lines.append(f"Markets you WOULD HAVE ENTERED (partial):        {spread_markets_partial_entered}")
    lines.append(f"  Entered OPEN markets (partial):                {spread_entered_open_partial}")
    lines.append(f"  Entered CLOSED markets (partial):              {spread_entered_closed_partial}")
    lines.append(f"Locked money in OPEN entered markets (partial):  {spread_locked_open_partial:.2f}")
    lines.append(f"Closed profitable markets (partial):             {spread_profitable_partial}")
    lines.append(f"Closed losing markets (partial):                 {spread_losing_partial}")
    lines.append(f"Net realized P/L on CLOSED (partial):            {spread_realized_pl_partial:+.2f}")

    return "\n".join(lines)


def main():
    # folder = input("Folder name under logs/: ").strip()
    folder = "gather_markets"
    if not folder:
        print("Need a folder name.")
        return

    # cap_str = input("Cap to analyze (e.g. 0.30): ").strip()
    cap_str = "0.6"
    try:
        cap = float(cap_str)
    except ValueError:
        print("Invalid cap.")
        return

    # bet_str = input("Max spend per market (bet), e.g. 30: ").strip()
    bet_str = "100"
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