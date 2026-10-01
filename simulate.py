#!/usr/bin/env python3
"""meme-radar simulate -- would these entry rules have made money on pumpswap?

Read-only over the ledger written by ``scanner.py``.

Universe: dex ``pumpswap``, first-seen market cap >= $1,000 and first-seen
liquidity >= $1,500, and a 5m snapshot to enter on.  Those pairs are split by
first-seen time into an earlier and a later half.

Quartile cutoffs for the 5m market cap and 5m liquidity are computed on the
EARLIER half only, and printed in dollars, so the later half is a genuine
out-of-sample test of rules that were never fitted to it.

Four rules are then evaluated separately on both halves:

* ``A``           5m market cap in the bottom quartile,
* ``B``           5m liquidity in the third quartile (median to 75th),
* ``C``           A and B together,
* ``D``           every pumpswap pair, as the baseline.

A trade enters at the 5m price and exits at the 1h price (``--entry 15m`` enters on
the 15m snapshot instead, and buckets on its market cap and liquidity).  Costs, all
optional flags: ``--size`` notional per trade, ``--fee`` per side, and price impact
per side of about ``2 * size / liquidity_usd`` at the time of that side's trade
(constant-product approximation).  A pair that is dead or missing at 1h returns
-100%.  ``--exit-rule`` chooses what else counts as a total loss: the default
``hybrid`` books one when the price collapsed *or* the exit is unusable (missing,
under $100, or under 5% of entry liquidity), while ``liq`` uses a fifth of entry
liquidity and ``price`` only looks at the price.  A 1h price above 50x the entry
price is treated as a bad reading and dropped from every statistic, with the
count reported per rule-half.  ``--compact`` prints a 60-column summary.  Exit impact always uses the 1h liquidity when it is available, but
never less than a tenth of entry liquidity, so a flaky reading cannot invent
absurd costs.

Reported per rule and half: n, win rate, mean and median net return, total PnL,
worst trade, the mean with each rule-half's three best trades removed, and a 95%
bootstrap interval for the mean (2,000 resamples, fixed seed).

Output is plain ASCII so it survives a Windows cp1252 console or Termux.

Usage:
    python simulate.py                 # $20 per trade
    python simulate.py --size 100      # bigger size, more slippage
"""

from __future__ import annotations

import argparse
import csv
import random
import sqlite3
import statistics
import sys
from collections.abc import Sequence
from typing import Any

from scanner import DEFAULT_DB, SNAPSHOT_DEAD, __version__, make_output_robust

#: Only this launchpad is simulated.
DEX = "pumpswap"

#: A pair must clear both bars at first sight.
MIN_FIRST_MARKET_CAP = 1000.0
MIN_FIRST_LIQUIDITY = 1500.0

#: Labels a trade may enter on; the exit is always ``EXIT_LABEL``.
ENTRY_CHOICES = ("5m", "15m")
DEFAULT_ENTRY = "5m"
EXIT_LABEL = "1h"

#: A pair that vanished by the exit returns this.
TOTAL_LOSS_PCT = -100.0

#: ``liq`` calls a trade a loss when the exit liquidity is missing or thin.
EXIT_RULE_LIQ = "liq"

#: ``price`` calls a loss only when the 1h price itself is missing or collapsed.
EXIT_RULE_PRICE = "price"

#: ``hybrid`` books a loss on either a collapsed price or an unusable exit.
EXIT_RULE_HYBRID = "hybrid"
EXIT_RULE_CHOICES = (EXIT_RULE_HYBRID, EXIT_RULE_LIQ, EXIT_RULE_PRICE)
DEFAULT_EXIT_RULE = EXIT_RULE_HYBRID

#: An exit on less than this fraction of the entry liquidity counts as a loss.
MIN_EXIT_LIQUIDITY_RATIO = 0.20

#: Under ``liq``, the same test on the exit liquidity.
LIQ_EXIT_LIQUIDITY_RATIO = 0.20

#: Under ``hybrid``, an exit below this fraction of entry liquidity is a loss.
HYBRID_EXIT_LIQUIDITY_RATIO = 0.05

#: Under ``hybrid``, an exit liquidity under this absolute floor is a loss.
HYBRID_MIN_EXIT_LIQUIDITY = 100.0

#: With ``price`` and ``hybrid``, a 1h price under this fraction of entry is a loss.
MIN_EXIT_PRICE_RATIO = 0.10

#: A 1h price this many times the entry price is a data error, not a trade.
MAX_PRICE_RATIO = 50.0

#: How many of the largest excluded ratios the note line lists.
TOP_OUTLIERS_SHOWN = 5

#: Floor on the liquidity used for exit impact, so a flaky reading cannot
#: invent absurd costs.  Never below this fraction of the entry liquidity.
MIN_IMPACT_LIQUIDITY_RATIO = 0.10

#: Two-sided slippage multiplier on size / liquidity.
IMPACT_FACTOR = 2.0

#: Bootstrap settings for the mean interval.
BOOTSTRAP_RESAMPLES = 2000
BOOTSTRAP_SEED = 20260929

_UNIVERSE_SQL = """
SELECT p.pair_address,
       p.first_seen,
       COALESCE(p.dex_id, '') AS dex_id,
       e.price_usd AS entry_price,
       e.liquidity_usd AS entry_liquidity,
       e.market_cap AS entry_market_cap,
       e.status AS entry_status,
       x.price_usd AS exit_price,
       x.liquidity_usd AS exit_liquidity,
       x.status AS exit_status
FROM pairs AS p
JOIN snapshots AS e ON e.pair_address = p.pair_address AND e.label = ?
LEFT JOIN snapshots AS x ON x.pair_address = p.pair_address AND x.label = ?
ORDER BY p.first_seen
"""


def load_universe(
    path: str,
    entry_label: str = DEFAULT_ENTRY,
    exit_rule: str = DEFAULT_EXIT_RULE,
) -> list[dict[str, Any]]:
    """Eligible, tradable pumpswap pairs, oldest first.

    ``entry_label`` picks the milestone to enter on: its price is the entry, and
    its market cap and liquidity are what the rules bucket on.  ``exit_rule``
    picks when a trade counts as a total loss: ``liq`` also calls it when the
    exit liquidity is missing or below a fifth of entry, ``price`` only when the
    1h price itself is missing, zero, or under a tenth of entry.  Opens SQLite
    read-only so the simulator never runs the scanner's schema.
    """
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(_UNIVERSE_SQL, (entry_label, EXIT_LABEL)).fetchall()
    finally:
        conn.close()

    trades: list[dict[str, Any]] = []
    for row in rows:
        if str(row["dex_id"]).strip().lower() != DEX:
            continue
        if row["entry_market_cap"] is None or float(row["entry_market_cap"]) < MIN_FIRST_MARKET_CAP:
            continue
        if row["entry_liquidity"] is None or float(row["entry_liquidity"]) < MIN_FIRST_LIQUIDITY:
            continue
        entry_price = row["entry_price"]
        if entry_price is None or float(entry_price) <= 0.0:
            continue  # cannot enter, so it is not a trade
        exit_price = row["exit_price"]
        entry_liquidity = float(row["entry_liquidity"])
        exit_liquidity = _number(row["exit_liquidity"])
        gone = (
            str(row["exit_status"]) == SNAPSHOT_DEAD
            or exit_price is None
            or float(exit_price) <= 0.0
        )
        # Work the ratio out before any -100% test, so a pool that collapsed and
        # still quotes 50x can be recognised as such rather than silently lost.
        has_quote = exit_price is not None and float(exit_price) > 0.0
        ratio = float(exit_price) / float(entry_price) if has_quote else None
        huge_price = ratio is not None and ratio > MAX_PRICE_RATIO
        liquidity_gone = (
            exit_liquidity is None
            or exit_liquidity < HYBRID_MIN_EXIT_LIQUIDITY
            or exit_liquidity < entry_liquidity * HYBRID_EXIT_LIQUIDITY_RATIO
        )

        # The -100% conditions come first, so a collapsed pool is a loss even if
        # its last quote read 50x or more.
        if not gone and exit_rule == EXIT_RULE_LIQ:
            # No liquidity left to sell into: the exit is worth nothing.
            gone = exit_liquidity is None or exit_liquidity < entry_liquidity * LIQ_EXIT_LIQUIDITY_RATIO
        if not gone and exit_rule == EXIT_RULE_PRICE:
            # The token still quotes, but the price has collapsed: treat it as gone.
            gone = float(exit_price) < float(entry_price) * MIN_EXIT_PRICE_RATIO
        if not gone and exit_rule == EXIT_RULE_HYBRID:
            # Either failure mode is enough: no usable exit, or a collapsed price.
            gone = liquidity_gone or float(exit_price) < float(entry_price) * MIN_EXIT_PRICE_RATIO

        # Only a trade that cleared every -100% condition can be a data error.
        data_error = not gone and huge_price
        collapsed_huge = gone and liquidity_gone and huge_price
        trades.append(
            {
                "first_seen": str(row["first_seen"]),
                "entry_price": float(entry_price),
                "entry_liquidity": float(row["entry_liquidity"]),
                "entry_market_cap": float(row["entry_market_cap"]),
                "exit_liquidity": exit_liquidity,
                "price_ratio": ratio,
                # A price this far above entry is a bad reading, not a real 50x.
                "data_error": data_error,
                "huge_price": huge_price,
                "liquidity_gone": liquidity_gone,
                # -100% because the liquidity vanished, despite a 50x+ quote.
                "collapsed_huge": collapsed_huge,
                "gone": gone,
                "raw_return": None if gone else ratio - 1.0,
            }
        )
    return trades


def _number(value: Any) -> float | None:
    """Float or None for a nullable REAL column."""
    return None if value is None else float(value)


def split_halves(trades: Sequence[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split by first-seen time into an earlier and a later half.

    ``load_universe`` already returns them oldest first, so this is a straight
    cut at the midpoint; an odd count puts the extra trade in the later half.
    """
    middle = len(trades) // 2
    return list(trades[:middle]), list(trades[middle:])


def quartile(values: Sequence[float], fraction: float) -> float | None:
    """Linear-interpolated quantile; None when there is nothing to cut."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = fraction * (len(ordered) - 1)
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return float(ordered[low] + (ordered[high] - ordered[low]) * (position - low))


def cutoffs(earlier: Sequence[dict[str, Any]]) -> dict[str, float]:
    """The 25/50/75 cutoffs of 5m market cap and liquidity, from the earlier half."""
    caps = [trade["entry_market_cap"] for trade in earlier]
    liqs = [trade["entry_liquidity"] for trade in earlier]
    return {
        "cap_q25": quartile(caps, 0.25),
        "cap_q50": quartile(caps, 0.50),
        "cap_q75": quartile(caps, 0.75),
        "liq_q25": quartile(liqs, 0.25),
        "liq_q50": quartile(liqs, 0.50),
        "liq_q75": quartile(liqs, 0.75),
    }


def rule_a(trade: dict[str, Any], cut: dict[str, float]) -> bool:
    """A: 5m market cap in the bottom quartile."""
    return trade["entry_market_cap"] <= cut["cap_q25"]


def rule_b(trade: dict[str, Any], cut: dict[str, float]) -> bool:
    """B: 5m liquidity in the third quartile, i.e. between the median and the 75th."""
    return cut["liq_q50"] < trade["entry_liquidity"] <= cut["liq_q75"]


def rule_c(trade: dict[str, Any], cut: dict[str, float]) -> bool:
    """C: A and B together."""
    return rule_a(trade, cut) and rule_b(trade, cut)


def rule_d(trade: dict[str, Any], cut: dict[str, float]) -> bool:
    """D: every pumpswap pair, as the baseline."""
    return True


RULES = (
    ("A", "entry cap in bottom quartile", rule_a),
    ("B", "entry liq in third quartile", rule_b),
    ("C", "A and B", rule_c),
    ("D", "all pairs (baseline)", rule_d),
)


def net_return(trade: dict[str, Any], size: float, fee: float) -> float:
    """Net return of one trade, in percent, after fees and price impact.

    Entry is paid ``entry * (1 + fee + impact)`` and the exit receives
    ``exit * (1 - fee - impact)``, each side impacting on the liquidity visible
    at that side.  A pair that vanished by the exit returns -100%.
    """
    if trade["gone"]:
        return TOTAL_LOSS_PCT
    entry_impact = _impact(size, trade["entry_liquidity"])
    exit_impact = _impact(size, _impact_liquidity(trade))
    entry_cost = trade["entry_price"] * (1.0 + fee + entry_impact)
    proceeds = trade["entry_price"] * (1.0 + trade["raw_return"]) * (1.0 - fee - exit_impact)
    return (proceeds / entry_cost - 1.0) * 100.0


def _impact_liquidity(trade: dict[str, Any]) -> float:
    """Liquidity to size the exit impact against, floored.

    Uses the 1h liquidity when it is present and sane, otherwise the entry
    liquidity, and never less than ``MIN_IMPACT_LIQUIDITY_RATIO`` of entry: a
    flaky liquidity reading must not manufacture absurd costs.
    """
    floor = trade["entry_liquidity"] * MIN_IMPACT_LIQUIDITY_RATIO
    reported = trade["exit_liquidity"]
    if reported is None or reported < floor:
        return max(trade["entry_liquidity"], floor)
    return float(reported)


def _impact(size: float, liquidity: float) -> float:
    """Constant-product slippage for one side, ``2 * size / liquidity``."""
    if liquidity <= 0.0:
        return 0.0
    return IMPACT_FACTOR * size / liquidity


def mean_ex_top(returns: Sequence[float], drop: int = 3) -> float | None:
    """Mean after removing the ``drop`` best trades; None when nothing is left."""
    if not returns:
        return None
    kept = sorted(returns)[: max(len(returns) - drop, 0)]
    return statistics.mean(kept) if kept else None


def bootstrap_mean_ci(
    returns: Sequence[float],
    resamples: int = BOOTSTRAP_RESAMPLES,
    seed: int = BOOTSTRAP_SEED,
) -> tuple[float | None, float | None]:
    """Percentile 95% interval for the mean, by resampling with replacement.

    A fixed seed keeps the interval reproducible across runs.  One trade has no
    spread to resample, so the interval collapses onto that single value.
    """
    n = len(returns)
    if n == 0:
        return None, None
    if n == 1:
        return float(returns[0]), float(returns[0])
    rng = random.Random(seed)
    means: list[float] = []
    for _ in range(resamples):
        total = 0.0
        for _ in range(n):
            total += returns[rng.randrange(n)]
        means.append(total / n)
    means.sort()
    return quartile(means, 0.025), quartile(means, 0.975)


def simulate(
    trades: Sequence[dict[str, Any]],
    rule: Any,
    cut: dict[str, float],
    size: float,
    fee: float,
) -> dict[str, Any]:
    """Statistics of one rule over one half, plus how many trades were dropped.

    A trade whose 1h price is more than ``MAX_PRICE_RATIO`` times the entry price
    is a data error, so it is excluded here and reported as ``excluded``.
    """
    matched = [trade for trade in trades if rule(trade, cut)]
    picked = [trade for trade in matched if not trade["data_error"]]
    excluded = len(matched) - len(picked)
    returns = [net_return(trade, size, fee) for trade in picked]
    n = len(returns)
    if not n:
        return {
            "n": 0,
            "excluded": excluded,
            "win_rate": None,
            "mean": None,
            "median": None,
            "pnl": 0.0,
            "worst": None,
            "mean_ex_top3": None,
            "low": None,
            "high": None,
        }
    mean = statistics.mean(returns)
    low, high = bootstrap_mean_ci(returns)
    return {
        "n": n,
        "excluded": excluded,
        "win_rate": sum(1 for value in returns if value > 0.0) / n * 100.0,
        "mean": mean,
        "median": statistics.median(returns),
        "pnl": mean / 100.0 * size * n,
        "worst": min(returns),
        "mean_ex_top3": mean_ex_top(returns),
        "low": low,
        "high": high,
    }


def fmt_money(value: float | None) -> str:
    """``+$1,234`` / ``-$12`` / ``n/a``."""
    return "n/a" if value is None else f"{value:+,.0f}"


def fmt_percent(value: float | None) -> str:
    """Signed percentage with an ASCII hyphen, or n/a."""
    return "n/a" if value is None else f"{value:+.2f}%"


def fmt_share(value: float | None) -> str:
    """Unsigned percentage, or n/a."""
    return "n/a" if value is None else f"{value:.1f}%"


HEADERS = ("rule", "half", "n", "win", "mean", "median", "total PnL", "worst", "mean ex-top3", "mean 95% low", "mean 95% high")
CSV_HEADERS = (
    "rule",
    "half",
    "n",
    "win_rate_pct",
    "mean_net_pct",
    "median_net_pct",
    "total_pnl_usd",
    "worst_net_pct",
    "mean_ex_top3_pct",
    "mean_ci95_low_pct",
    "mean_ci95_high_pct",
)


def print_table(rows: Sequence[tuple[str, ...]]) -> None:
    """Render the results table as plain ASCII."""
    widths = [max([len(HEADERS[i])] + [len(row[i]) for row in rows]) for i in range(len(HEADERS))]
    rule = "+" + "+".join("-" * (width + 2) for width in widths) + "+"
    print(rule)
    print("| " + " | ".join(h.ljust(w) for h, w in zip(HEADERS, widths)) + " |")
    print(rule)
    for row in rows:
        print("| " + " | ".join(cell.rjust(w) for cell, w in zip(row, widths)) + " |")
    print(rule)


def _cell(text: str) -> float | None:
    """Parse a formatted cell back to a float; n/a becomes None."""
    cleaned = text.replace("%", "").replace("$", "").replace(",", "").replace("+", "").strip()
    return None if cleaned in ("n/a", "", "-") else float(cleaned)


def result_rows(
    earlier: Sequence[dict[str, Any]],
    later: Sequence[dict[str, Any]],
    cut: dict[str, float],
    size: float,
    fee: float,
) -> list[tuple[str, ...]]:
    """Two rows (earlier, later) per rule, in A, B, C, D order."""
    rows: list[tuple[str, ...]] = []
    for code, _label, rule in RULES:
        for half_name, half in (("earlier", earlier), ("later", later)):
            summary = simulate(half, rule, cut, size, fee)
            rows.append(
                (
                    code,
                    half_name,
                    f"{summary['n']:,}",
                    fmt_share(summary["win_rate"]),
                    fmt_percent(summary["mean"]),
                    fmt_percent(summary["median"]),
                    fmt_money(summary["pnl"]),
                    fmt_percent(summary["worst"]),
                    fmt_percent(summary["mean_ex_top3"]),
                    fmt_percent(summary["low"]),
                    fmt_percent(summary["high"]),
                )
            )
    return rows


def print_csv(rows: Sequence[tuple[str, ...]]) -> None:
    """CSV records with raw numbers, one header first."""
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(CSV_HEADERS)
    for code, half_name, n, win, mean, median, pnl, worst, ex_top3, low, high in rows:
        writer.writerow(
            [
                code,
                half_name,
                int(n.replace(",", "")),
                _cell(win),
                _cell(mean),
                _cell(median),
                _cell(pnl),
                _cell(worst),
                _cell(ex_top3),
                _cell(low),
                _cell(high),
            ]
        )



def exit_rule_note(exit_rule: str) -> str:
    """One line describing when this run books a trade as a total loss."""
    shared = f"dead or missing or zero {EXIT_LABEL} price always returns {TOTAL_LOSS_PCT:.0f}%"
    if exit_rule == EXIT_RULE_HYBRID:
        return (
            shared
            + f", and so does a {EXIT_LABEL} price below {MIN_EXIT_PRICE_RATIO:.0%} of entry, "
            + f"or missing exit liquidity, or liquidity below ${HYBRID_MIN_EXIT_LIQUIDITY:,.0f} "
            + f"or below {HYBRID_EXIT_LIQUIDITY_RATIO:.0%} of entry"
        )
    if exit_rule == EXIT_RULE_PRICE:
        return shared + f", and so does a {EXIT_LABEL} price below {MIN_EXIT_PRICE_RATIO:.0%} of entry"
    return shared + f", and so does missing exit liquidity or liquidity below {LIQ_EXIT_LIQUIDITY_RATIO:.0%} of entry"


COMPACT_HEADERS = ("rule", "half", "n", "win", "median", "ex-top3", "mean")


#: Column widths that keep the compact table inside 60 characters.  The half
#: column is 7 wide so "earlier" never spills into the next one.
COMPACT_WIDTHS = (1, 7, 4, 5, 8, 8, 8)
COMPACT_GAP = " "
COMPACT_LIMIT = 60


def print_compact(rows: Sequence[tuple[str, ...]]) -> None:
    """A narrow table that fits a 60-column terminal.

    Every column is padded to a fixed width wide enough for its own header and
    its widest value, so the rows stay aligned no matter how long "earlier" is.
    """
    print(COMPACT_GAP.join(h.ljust(w) for h, w in zip(COMPACT_HEADERS, COMPACT_WIDTHS)).rstrip())
    for code, half_name, n, win, mean, median, _pnl, _worst, ex_top3, _low, _high in rows:
        cells = (code, half_name, n.replace(",", ""), win.rstrip("%"), median, ex_top3, mean)
        print(COMPACT_GAP.join(cell.ljust(w) for cell, w in zip(cells, COMPACT_WIDTHS)).rstrip())


def print_note(text: str, limit: int = COMPACT_LIMIT) -> None:
    """Wrap a note to ``limit`` characters so --compact stays narrow."""
    line = ""
    for word in text.split():
        if line and len(line) + 1 + len(word) > limit:
            print(line)
            line = word
        else:
            line = f"{line} {word}".strip()
    if line:
        print(line)


def outlier_note(
    earlier: Sequence[dict[str, Any]],
    later: Sequence[dict[str, Any]],
    cut: dict[str, float],
) -> str:
    """One note line: bad readings excluded, and collapsed pools booked at -100%.

    The two counts are separate on purpose.  A price above ``MAX_PRICE_RATIO`` is
    only excluded when the trade cleared every -100% condition; when the
    liquidity had already collapsed the trade is a real loss, so it is counted
    here instead of being dropped as noise.
    """
    excluded: dict[str, int] = {}
    collapsed: dict[str, int] = {}
    ratios: list[float] = []
    collapsed_ratios: list[float] = []
    for code, _label, rule in RULES:
        for half_name, half in (("earlier", earlier), ("later", later)):
            matched = [trade for trade in half if rule(trade, cut)]
            bad = [trade for trade in matched if trade["data_error"]]
            lost = [trade for trade in matched if trade["collapsed_huge"]]
            if bad:
                excluded[f"{code}/{half_name}"] = len(bad)
                ratios.extend(trade["price_ratio"] for trade in bad)
            if lost:
                collapsed[f"{code}/{half_name}"] = len(lost)
                collapsed_ratios.extend(trade["price_ratio"] for trade in lost)

    def _detail(counts: dict[str, int], values: list[float]) -> str:
        if not counts:
            return ""
        breakdown = "; ".join(f"{key} {count}" for key, count in sorted(counts.items()))
        top = ", ".join(f"{ratio:.0f}x" for ratio in sorted(values, reverse=True)[:TOP_OUTLIERS_SHOWN])
        return f" ({breakdown}; largest: {top})"

    total_excluded = len(ratios)
    total_collapsed = len(collapsed_ratios)
    if total_excluded == 0 and total_collapsed == 0:
        return f"note: no 1h price above {MAX_PRICE_RATIO:.0f}x the entry price"
    return (
        f"note: excluded {total_excluded} trade(s) above {MAX_PRICE_RATIO:.0f}x entry as bad readings"
        f"{_detail(excluded, ratios)}; booked {total_collapsed} at {TOTAL_LOSS_PCT:.0f}% because "
        f"liquidity collapsed despite a 50x+ price{_detail(collapsed, collapsed_ratios)}."
    )


def build_parser() -> argparse.ArgumentParser:
    """Command line interface of the simulator."""
    parser = argparse.ArgumentParser(
        prog="simulate.py",
        description="Backtest pumpswap entry rules on the stored snapshots.",
    )
    parser.add_argument("--db", default=DEFAULT_DB, metavar="PATH", help="SQLite file (default: %(default)s)")
    parser.add_argument("--size", type=float, default=20.0, metavar="USD", help="notional per trade (default: $20)")
    parser.add_argument("--fee", type=float, default=0.005, metavar="RATE", help="fee per side, as a rate (default: 0.005)")
    parser.add_argument(
        "--entry",
        choices=ENTRY_CHOICES,
        default=DEFAULT_ENTRY,
        help="milestone to enter on; its price is the entry and its market cap and "
        "liquidity are what the rules bucket on (default: 5m)",
    )
    parser.add_argument(
        "--exit-rule",
        choices=EXIT_RULE_CHOICES,
        default=DEFAULT_EXIT_RULE,
        help="when a trade counts as a total loss: 'hybrid' (default) on a "
        "collapsed 1h price or an unusable exit; 'liq' on missing exit "
        "liquidity or below a fifth of entry; 'price' only on a missing, zero, "
        "or collapsed 1h price",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="print a short table (rule, half, n, win, median, mean ex-top3, mean) "
        "that fits a 60-column terminal, with no header lecture",
    )
    parser.add_argument("--csv", action="store_true", help="write CSV instead of the table")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the backtest for the ledger in --db."""
    args = build_parser().parse_args(argv)
    make_output_robust()

    try:
        trades = load_universe(args.db, args.entry, args.exit_rule)
    except sqlite3.Error as exc:
        print(f"{args.db}: {exc}", file=sys.stderr)
        print("hint: run scanner.py against this database first; it creates the snapshots table.", file=sys.stderr)
        return 1

    if not trades:
        if args.csv:
            print(",".join(CSV_HEADERS))
        else:
            print(f"No tradable {DEX} pairs in {args.db} yet.")
            print(
                f"Need dex={DEX}, {args.entry} market cap >= ${MIN_FIRST_MARKET_CAP:,.0f}, "
                f"{args.entry} liquidity >= ${MIN_FIRST_LIQUIDITY:,.0f}, and a positive {args.entry} price."
            )
        return 0

    earlier, later = split_halves(trades)
    cut = cutoffs(earlier)
    rows = result_rows(earlier, later, cut, args.size, args.fee)
    note = outlier_note(earlier, later, cut)

    if args.csv:
        print_csv(rows)
        return 0

    if args.compact:
        print_compact(rows)
        print()
        print_note(note)
        return 0

    print(f"meme-radar simulate -- {args.db}")
    print(f"exit rule: {args.exit_rule}")
    print(
        f"{DEX}: {len(trades):,} tradable pairs ({args.entry} cap >= ${MIN_FIRST_MARKET_CAP:,.0f}, "
        f"{args.entry} liquidity >= ${MIN_FIRST_LIQUIDITY:,.0f}, positive {args.entry} price)"
    )
    print(
        f"split by first_seen: earlier {len(earlier):,} ({earlier[0]['first_seen']} .. "
        f"{earlier[-1]['first_seen']}) | later {len(later):,} ({later[0]['first_seen']} .. {later[-1]['first_seen']})"
    )
    print()
    print(f"cutoffs from the EARLIER half only ({args.entry} snapshot values):")
    print(f"  market cap   q25 ${cut['cap_q25']:,.2f}   q50 ${cut['cap_q50']:,.2f}   q75 ${cut['cap_q75']:,.2f}")
    print(f"  liquidity    q25 ${cut['liq_q25']:,.2f}   q50 ${cut['liq_q50']:,.2f}   q75 ${cut['liq_q75']:,.2f}")
    print()
    print("rules:")
    for code, label, _rule in RULES:
        print(f"  {code}  {label}")
    print()
    print(f"costs: size ${args.size:,.2f} per trade, fee {args.fee * 100:.3f}% per side, impact ~{IMPACT_FACTOR:.0f} * size / liquidity per side")
    print(f"       entry at the {args.entry} price, exit at the {EXIT_LABEL} price")
    print(f"       exit rule '{args.exit_rule}': " + exit_rule_note(args.exit_rule))
    print(f"       exit impact uses the {EXIT_LABEL} liquidity, floored at {MIN_IMPACT_LIQUIDITY_RATIO:.0%} of entry liquidity")
    print()
    print_table(rows)
    print()
    print(note)
    print(f"win = share of trades with a positive net return; 95% interval = {BOOTSTRAP_RESAMPLES:,}-resample bootstrap of the mean (seed {BOOTSTRAP_SEED})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
