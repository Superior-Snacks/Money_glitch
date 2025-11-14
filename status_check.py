import os
import json
from datetime import datetime, timezone
from typing import Dict, List

LOGS_DIR = "logs"


def dt_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_rows(path: str) -> List[dict]:
    """
    Load JSONL rows from a file. Ignores blank / malformed lines.
    """
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


def build_caps_union(rows: List[dict]) -> List[float]:
    """
    Collect all unique caps present across all rows.
    """
    caps = set()
    for row in rows:
        spread = row.get("cap_spread") or {}
        caps_dict = spread.get("caps") or {}
        for k in caps_dict.keys():
            try:
                caps.add(float(k))
            except ValueError:
                continue
    return sorted(caps)


def build_status_report(folder: str) -> str:
    out_dir = os.path.join(LOGS_DIR, folder)
    open_path = os.path.join(out_dir, "log_open_markets_cap_spread.jsonl")
    closed_path = os.path.join(out_dir, "log_closed_markets_cap_spread.jsonl")

    open_rows = load_rows(open_path)
    closed_rows = load_rows(closed_path)
    all_rows = open_rows + closed_rows

    lines: List[str] = []
    lines.append("=== CAP SPREAD STATUS REPORT (MAKER NO) ===")
    lines.append(f"Folder: {folder}")
    lines.append(f"Generated at: {dt_iso()}")
    lines.append("")

    if not all_rows:
        lines.append("No data found yet (run the main cap-spread script first).")
        return "\n".join(lines)

    # ---------- Global market counts ----------
    total_markets = len(all_rows)
    open_count = sum(1 for r in all_rows if r.get("status") == "TBD")
    yes_count = sum(1 for r in all_rows if r.get("status") == "YES")
    no_count = sum(1 for r in all_rows if r.get("status") == "NO")

    lines.append("=== OVERALL MARKETS ===")
    lines.append(f"Total markets in spread index: {total_markets}")
    lines.append(f"  Open (TBD): {open_count}")
    lines.append(f"  Closed YES: {yes_count}")
    lines.append(f"  Closed NO : {no_count}")
    lines.append("")
    lines.append("Note: Per-cap stats below are separate hypothetical strategies.")
    lines.append("      Do NOT sum P/L across caps.")
    lines.append("")

    # ---------- Caps union ----------
    caps = build_caps_union(all_rows)
    if not caps:
        lines.append("No caps found in data.")
        return "\n".join(lines)

    lines.append("Caps detected in data:")
    lines.append("  " + ", ".join(f"{c:.3f}" for c in caps))
    lines.append("")

    # ---------- Per-cap aggregates ----------
    # For each cap c, we track:
    #
    # stats[c] = {
    #   "closed_yes": {dollars, shares, markets, trades},
    #   "closed_no":  {dollars, shares, markets, trades},
    #   "open":       {dollars, shares, markets, trades},
    #   "pl":         {realized, markets_yes, markets_no, profitable_mkts, losing_mkts},
    #   "exposure":   {worst_case, markets},
    # }
    #
    # Maker NO economics:
    #   premium (dollars)  = Σ(p * s) over NO trades with price <= cap
    #   shares             = Σ(s)
    #
    # Closed NO:
    #   P/L = + premium     (keep what you sold)
    #
    # Closed YES:
    #   Pay (1 - p)*s per share
    #   Total payout  = shares - dollars
    #   P/L           = dollars - shares   (typically negative)
    #
    # Open (TBD):
    #   Worst-case exposure if market resolves YES:
    #       exposure = shares - dollars   (max potential payout minus premium)
    #
    stats: Dict[float, dict] = {
        c: {
            "closed_yes": {"dollars": 0.0, "shares": 0.0, "markets": 0, "trades": 0},
            "closed_no": {"dollars": 0.0, "shares": 0.0, "markets": 0, "trades": 0},
            "open": {"dollars": 0.0, "shares": 0.0, "markets": 0, "trades": 0},
            "pl": {
                "realized": 0.0,
                "markets_yes": 0,
                "markets_no": 0,
                "profitable_mkts": 0,
                "losing_mkts": 0,
            },
            "exposure": {"worst_case": 0.0, "markets": 0},
        }
        for c in caps
    }

    def accumulate_for_row(row: dict):
        status = row.get("status")
        spread = row.get("cap_spread") or {}
        caps_dict = spread.get("caps") or {}
        # Track which caps this market actually has liq at
        has_liq_per_cap = {c: False for c in caps}

        # First pass: aggregate raw sums
        for cap_str, st in caps_dict.items():
            try:
                cap = float(cap_str)
            except ValueError:
                continue
            if cap not in stats:
                continue

            dollars = float(st.get("dollars", 0.0))
            shares = float(st.get("shares", 0.0))
            trades = int(st.get("trades", 0))
            if dollars == 0.0 and shares == 0.0 and trades == 0:
                continue

            has_liq_per_cap[cap] = True

            if status == "YES":
                bucket = stats[cap]["closed_yes"]
            elif status == "NO":
                bucket = stats[cap]["closed_no"]
            else:  # "TBD" or anything else treated as open
                bucket = stats[cap]["open"]

            bucket["dollars"] += dollars
            bucket["shares"] += shares
            bucket["trades"] += trades

        # Second pass: per-market P/L and exposure, cap by cap
        for cap in caps:
            if not has_liq_per_cap.get(cap):
                continue

            cap_key_exact = f"{cap:.3f}"
            st = caps_dict.get(cap_key_exact) or caps_dict.get(str(cap))
            if not st:
                continue

            dollars = float(st.get("dollars", 0.0))
            shares = float(st.get("shares", 0.0))

            if status in ("YES", "NO"):
                # Hypothetical realized P/L for maker-NO strategy at this cap
                if status == "NO":
                    pl = dollars  # keep premium
                    stats[cap]["pl"]["markets_no"] += 1
                else:  # YES
                    pl = dollars - shares  # premium minus payout
                    stats[cap]["pl"]["markets_yes"] += 1

                stats[cap]["pl"]["realized"] += pl

                if pl >= 0:
                    stats[cap]["pl"]["profitable_mkts"] += 1
                else:
                    stats[cap]["pl"]["losing_mkts"] += 1
            else:
                # Open: worst-case exposure if YES wins from here
                # Exposure = payout - premium = shares - dollars
                exposure = shares - dollars
                stats[cap]["exposure"]["worst_case"] += exposure

        # Third pass: count markets with any liq per cap
        for cap, had_liq in has_liq_per_cap.items():
            if not had_liq:
                continue
            if status == "YES":
                stats[cap]["closed_yes"]["markets"] += 1
            elif status == "NO":
                stats[cap]["closed_no"]["markets"] += 1
            else:
                stats[cap]["open"]["markets"] += 1
                stats[cap]["exposure"]["markets"] += 1

    for row in all_rows:
        accumulate_for_row(row)

    # ---------- Per-cap summary ----------
    lines.append("=== PER-CAP SUMMARY (MAKER NO VIEW) ===")
    lines.append("Each cap is a separate 'place maker-NO at this price cap' strategy.")
    lines.append("All numbers are aggregated across markets that had NO-liquidity under that cap.")
    lines.append("")

    for cap in caps:
        s = stats[cap]
        cy = s["closed_yes"]
        cn = s["closed_no"]
        op = s["open"]
        pl = s["pl"]
        ex = s["exposure"]

        markets_with_liq = cy["markets"] + cn["markets"] + op["markets"]
        markets_closed = cy["markets"] + cn["markets"]
        realized_pl = pl["realized"]

        lines.append(f"Cap {cap:.3f}:")
        lines.append(
            f"  Markets with liq at this cap: {markets_with_liq} "
            f"(closed={markets_closed}, open={op['markets']})"
        )
        lines.append(
            f"    Closed NO: markets={cn['markets']}, "
            f"premium={cn['dollars']:.2f}, shares_sold={cn['shares']:.2f}, trades={cn['trades']}"
        )
        lines.append(
            f"    Closed YES: markets={cy['markets']}, "
            f"premium={cy['dollars']:.2f}, shares_sold={cy['shares']:.2f}, trades={cy['trades']}"
        )
        lines.append(
            f"    Open (TBD): markets={op['markets']}, "
            f"premium={op['dollars']:.2f}, shares_sold={op['shares']:.2f}, trades={op['trades']}"
        )
        lines.append(
            "  Realized P/L (maker-NO, hypothetical if used on all these markets): "
            f"{realized_pl:.2f} "
            f"(markets_NO={pl['markets_no']}, markets_YES={pl['markets_yes']}, "
            f"profitable_markets={pl['profitable_mkts']}, losing_markets={pl['losing_mkts']})"
        )
        lines.append(
            f"  Worst-case open exposure (if all open resolve YES): "
            f"{ex['worst_case']:.2f} across {ex['markets']} open markets"
        )
        lines.append("")

    return "\n".join(lines)


def main():
    folder = input("Folder name under logs/: ").strip()
    if not folder:
        print("Need a folder name.")
        return

    report = build_status_report(folder)
    print(report)

    out_dir = os.path.join(LOGS_DIR, folder)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "status_report.txt")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)
        f.write("\n")

    print(f"\n[WRITE] Status report saved to: {out_path}")


if __name__ == "__main__":
    main()