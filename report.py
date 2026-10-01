#!/usr/bin/env python3
"""meme-radar report -- market-cap drift and mortality per snapshot label.

Reads the ledger written by ``scanner.py`` and prints, for every snapshot label
(5m, 15m, 1h, 4h):

* the sample size,
* the median percentage change versus the pair's first-seen market cap,
* the p10 / p25 / p75 / p90 of that change, so the tail is visible,
* the share of tokens down more than 50% and more than 90%,
* the share of tokens up more than 2x and more than 5x,
* mortality under the definition below.

Sample filter: a pair only counts when its first-seen market cap is known and at
least ``MIN_FIRST_MARKET_CAP`` ($1,000).  Dust and unknown baselines distort
every percentile, so they are excluded up front and counted separately.

"Dead" means the market cap was down more than 90% at that label, *or* the pool
is no longer returned by GeckoTerminal (``snapshots.status = 'dead'``).

Output is plain ASCII so it survives a Windows cp1252 console or Termux.

Usage:
    python report.py                     # every label, table output
    python report.py --label 1h          # single label
    python report.py --by-dex            # same table per dex group
    python report.py --csv               # machine readable
"""

from __future__ import annotations

import argparse
import csv
import math
import sqlite3
import statistics
import sys
from collections.abc import Sequence
from typing import Any

from scanner import (
    DEFAULT_DB,
    SNAPSHOT_DEAD,
    SNAPSHOT_LABELS,
    __version__,
    make_output_robust,
)

#: Pairs first seen below this market cap are excluded from every statistic.
MIN_FIRST_MARKET_CAP = 1000.0

#: ``--by-dex`` groups (or labels inside a group) below this are skipped as thin.
MIN_GROUP_SAMPLES = 20

#: Dexes that get their own table; everything else lands in ``other``.
DEX_GROUPS = ("pump-fun", "pumpswap", "meteora-damm-v2")
OTHER_GROUP = "other"
ALL_GROUPS = (*DEX_GROUPS, OTHER_GROUP)

DOWN_50 = -50.0
DOWN_90 = -90.0
UP_2X = 100.0
UP_5X = 400.0

HEADERS = (
    "label",
    "n",
    "median",
    "p10",
    "p25",
    "p75",
    "p90",
    "down>50%",
    "down>90%",
    "up>2x",
    "up>5x",
    "dead",
    "mortality",
)
CSV_HEADERS = (
    "label",
    "n",
    "median_change_pct",
    "p10_change_pct",
    "p25_change_pct",
    "p75_change_pct",
    "p90_change_pct",
    "pct_down_50",
    "pct_down_90",
    "pct_up_2x",
    "pct_up_5x",
    "dead",
    "mortality_pct",
)

_SNAPSHOT_SQL = """
SELECT s.pair_address, s.label, s.status,
       s.market_cap AS snapshot_market_cap,
       p.market_cap AS first_market_cap,
       COALESCE(p.dex_id, '') AS dex_id
FROM snapshots AS s
LEFT JOIN pairs AS p ON p.pair_address = s.pair_address
ORDER BY s.label, s.taken_at
"""


def snapshot_rows(path: str) -> list[sqlite3.Row]:
    """Every snapshot joined with its pair baseline and dex.

    Read straight from SQLite (read-only) rather than through ``Store`` so the
    report never runs the scanner's ``CREATE TABLE`` statements, and so the
    ``dex_id`` of the pair is available for ``--by-dex``.
    """
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    try:
        conn.row_factory = sqlite3.Row
        return conn.execute(_SNAPSHOT_SQL).fetchall()
    finally:
        conn.close()


def dex_group(dex_id: str) -> str:
    """``pump-fun``/``pumpswap``/``meteora-damm-v2`` verbatim, anything else ``other``."""
    dex = dex_id.strip().lower()
    return dex if dex in DEX_GROUPS else OTHER_GROUP


def percentile(values: Sequence[float], fraction: float) -> float | None:
    """Linear-interpolated percentile (0.1 ... 0.9); None when empty."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = fraction * (len(ordered) - 1)
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return float(ordered[int(position)])
    weight = position - low
    return float(ordered[low] * (1.0 - weight) + ordered[high] * weight)


def ordered_labels(stats: dict[str, Any], wanted: str | None = None) -> list[str]:
    """The requested label only, or every known label in milestone order."""
    if wanted is not None:
        return [wanted]
    order = {label: index for index, (label, _seconds) in enumerate(SNAPSHOT_LABELS)}
    return sorted(stats, key=lambda label: order.get(label, len(order)))


def collect(rows: Sequence[sqlite3.Row], wanted: str | None = None) -> dict[str, dict[str, Any]]:
    """Aggregate snapshot rows into one accumulator per label.

    A row is dead when GeckoTerminal no longer returns the pool, when its market
    cap is missing, or when its market cap fell more than 90% from the baseline.
    """
    stats: dict[str, dict[str, Any]] = {}
    for row in rows:
        label = str(row["label"])
        if wanted is not None and label != wanted:
            continue
        bucket = stats.setdefault(
            label,
            {"snapshots": 0, "excluded": 0, "no_change": 0, "dead": 0, "changes": []},
        )
        bucket["snapshots"] += 1

        baseline = row["first_market_cap"]
        if baseline is None or float(baseline) < MIN_FIRST_MARKET_CAP:
            bucket["excluded"] += 1
            continue

        market_cap = row["snapshot_market_cap"]
        change: float | None = None
        if market_cap is not None:
            change = (float(market_cap) - float(baseline)) / float(baseline) * 100.0
        if str(row["status"]) == SNAPSHOT_DEAD or change is None or change <= DOWN_90:
            bucket["dead"] += 1
        if change is None:
            bucket["no_change"] += 1
            continue
        bucket["changes"].append(change)
    return stats


def summarise(bucket: dict[str, Any]) -> dict[str, Any]:
    """Turn one accumulator into the numbers the report shows."""
    n = int(bucket["snapshots"])
    changes: list[float] = bucket["changes"]
    measured = len(changes)
    dead = int(bucket["dead"])
    return {
        "n": n,
        "excluded": int(bucket["excluded"]),
        "measured": measured,
        "dead": dead,
        "no_change": int(bucket["no_change"]),
        "median_change_pct": statistics.median(changes) if changes else None,
        "p10_change_pct": percentile(changes, 0.10),
        "p25_change_pct": percentile(changes, 0.25),
        "p75_change_pct": percentile(changes, 0.75),
        "p90_change_pct": percentile(changes, 0.90),
        "pct_down_50": _share(changes, DOWN_50, measured),
        "pct_down_90": _share(changes, DOWN_90, measured),
        "pct_up_2x": _share(changes, UP_2X, measured),
        "pct_up_5x": _share(changes, UP_5X, measured),
        "mortality_pct": dead / n * 100.0 if n else None,
    }


def _share(changes: Sequence[float], threshold: float, measured: int) -> float | None:
    """Percent of the measured distribution past threshold (<= below, >= above)."""
    if not measured:
        return None
    if threshold < 0:
        hits = sum(1 for change in changes if change <= threshold)
    else:
        hits = sum(1 for change in changes if change >= threshold)
    return hits / measured * 100.0


def fmt_percent(value: float | None) -> str:
    """Signed percentage with an ASCII hyphen, or n/a."""
    return "n/a" if value is None else f"{value:+.1f}%"


def fmt_share(value: float | None) -> str:
    """Unsigned percentage, or n/a."""
    return "n/a" if value is None else f"{value:.1f}%"


def table_row(label: str, summary: dict[str, Any]) -> tuple[str, ...]:
    """One table row: counts first, then the drift columns."""
    return (
        label,
        f"{summary['n']:,}",
        fmt_percent(summary["median_change_pct"]),
        fmt_percent(summary["p10_change_pct"]),
        fmt_percent(summary["p25_change_pct"]),
        fmt_percent(summary["p75_change_pct"]),
        fmt_percent(summary["p90_change_pct"]),
        fmt_share(summary["pct_down_50"]),
        fmt_share(summary["pct_down_90"]),
        fmt_share(summary["pct_up_2x"]),
        fmt_share(summary["pct_up_5x"]),
        f"{summary['dead']:,}",
        fmt_share(summary["mortality_pct"]),
    )


def print_table(rows: list[tuple[str, dict[str, Any]]], title: str | None = None) -> None:
    """Render one table of (label, summary) rows as plain ASCII."""
    if not rows:
        print("  (no labels)")
        return
    table = [table_row(label, summary) for label, summary in rows]
    widths = [max([len(HEADERS[i])] + [len(row[i]) for row in table]) for i in range(len(HEADERS))]
    rule = "+" + "+".join("-" * (width + 2) for width in widths) + "+"
    if title:
        print(title)
    print(rule)
    print("| " + " | ".join(h.ljust(w) for h, w in zip(HEADERS, widths)) + " |")
    print(rule)
    for row in table:
        print("| " + " | ".join(cell.rjust(w) for cell, w in zip(row, widths)) + " |")
    print(rule)


def _csv_number(value: float | None) -> str:
    """Two-decimal number, or an empty cell when unknown."""
    return "" if value is None else f"{value:.2f}"


def print_csv(rows: list[tuple[str, dict[str, Any]]], group: str | None = None, header: bool = True) -> None:
    """CSV with raw numbers, so a spreadsheet can do its own maths."""
    writer = csv.writer(sys.stdout, lineterminator="\n")
    by_group = group is not None
    if header:
        writer.writerow((["group"] if by_group else []) + list(CSV_HEADERS))
    for label, summary in rows:
        writer.writerow(
            ([group] if by_group else [])
            + [
                label,
                summary["n"],
                _csv_number(summary["median_change_pct"]),
                _csv_number(summary["p10_change_pct"]),
                _csv_number(summary["p25_change_pct"]),
                _csv_number(summary["p75_change_pct"]),
                _csv_number(summary["p90_change_pct"]),
                _csv_number(summary["pct_down_50"]),
                _csv_number(summary["pct_down_90"]),
                _csv_number(summary["pct_up_2x"]),
                _csv_number(summary["pct_up_5x"]),
                summary["dead"],
                _csv_number(summary["mortality_pct"]),
            ]
        )


def group_rows(
    rows: Sequence[sqlite3.Row],
    wanted: str | None,
) -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """One ranked (label, summary) table per dex group."""
    buckets: dict[str, list[sqlite3.Row]] = {name: [] for name in ALL_GROUPS}
    for row in rows:
        if wanted is not None and str(row["label"]) != wanted:
            continue
        buckets[dex_group(str(row["dex_id"]))].append(row)
    tables: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    for name in ALL_GROUPS:
        stats = collect(buckets[name], wanted)
        tables[name] = [
            (label, summarise(stats[label]))
            for label in ordered_labels(stats, wanted)
            if label in stats
        ]
    return tables


def split_thin(
    ranked: list[tuple[str, dict[str, Any]]],
) -> tuple[list[tuple[str, dict[str, Any]]], list[tuple[str, dict[str, Any]]]]:
    """Split by the MIN_GROUP_SAMPLES floor, keeping the label order."""
    shown = [row for row in ranked if row[1]["n"] >= MIN_GROUP_SAMPLES]
    skipped = [row for row in ranked if row[1]["n"] < MIN_GROUP_SAMPLES]
    return shown, skipped


def report_by_dex(tables: dict[str, list[tuple[str, dict[str, Any]]]]) -> None:
    """Print one table per dex group, naming every group or label that was skipped."""
    for name in ALL_GROUPS:
        ranked = tables[name]
        total = sum(summary["n"] for _label, summary in ranked)
        if not ranked:
            print(f"[{name}] skipped: no snapshots at all")
            print()
            continue
        shown, skipped = split_thin(ranked)
        if not shown:
            print(f"[{name}] skipped: {total:,} sample(s) in total, none reach {MIN_GROUP_SAMPLES}")
            print()
            continue
        print_table(shown, title=f"=== dex group: {name} ({total:,} sample(s)) ===")
        for label, summary in skipped:
            print(f"  {label}: skipped, {summary['n']:,} sample(s) < {MIN_GROUP_SAMPLES}")
        print()


def build_parser() -> argparse.ArgumentParser:
    """Command line interface of the report."""
    parser = argparse.ArgumentParser(
        prog="report.py",
        description="Market-cap drift and mortality per snapshot label.",
    )
    parser.add_argument("--db", default=DEFAULT_DB, metavar="PATH", help="SQLite file (default: %(default)s)")
    parser.add_argument("--label", default=None, metavar="LABEL", help="report a single label (5m, 15m, 1h, 4h)")
    parser.add_argument("--csv", action="store_true", help="write CSV instead of the table")
    parser.add_argument(
        "--by-dex",
        action="store_true",
        help="repeat the table per dex group; groups under "
        + str(MIN_GROUP_SAMPLES)
        + " samples are skipped",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Print the snapshot report for the ledger in --db."""
    args = build_parser().parse_args(argv)
    make_output_robust()

    try:
        rows = snapshot_rows(args.db)
    except sqlite3.Error as exc:
        print(f"{args.db}: {exc}", file=sys.stderr)
        print("hint: run scanner.py against this database first; it creates the snapshots table.", file=sys.stderr)
        return 1

    if args.by_dex:
        tables = group_rows(rows, args.label)
        if args.csv:
            for position, name in enumerate(ALL_GROUPS):
                print_csv(tables[name], group=name, header=position == 0)
            return 0
        report_by_dex(tables)
        return 0

    stats = collect(rows, args.label)
    ranked = [
        (label, summarise(stats[label]))
        for label in ordered_labels(stats, args.label)
        if label in stats
    ]
    if not ranked:
        if args.csv:
            print(",".join(CSV_HEADERS))
        else:
            print(f"No snapshots recorded for {args.label or 'any label'} in {args.db} yet.")
            print("Rows appear once stored pools reach their milestone ages (5m, 15m, 1h, 4h).")
        return 0

    if args.csv:
        print_csv(ranked)
        return 0

    dead_total = sum(summary["dead"] for _label, summary in ranked)
    excluded_total = sum(summary["excluded"] for _label, summary in ranked)
    missing = sum(summary["no_change"] for _label, summary in ranked)
    print(f"meme-radar snapshot report -- {args.db}")
    print(f"{len(rows):,} snapshot rows | {dead_total:,} dead at their label")
    print(
        f"samples: first-seen market cap >= ${MIN_FIRST_MARKET_CAP:,.0f}; "
        f"dead = market cap down >{abs(DOWN_90):.0f}% or gone from GeckoTerminal"
    )
    print()
    print_table(ranked)
    if excluded_total:
        print(
            f"note: {excluded_total:,} snapshot(s) excluded -- first-seen market cap "
            f"NULL or under ${MIN_FIRST_MARKET_CAP:,.0f}."
        )
    if missing:
        print(
            f"note: {missing:,} sample(s) had no market cap at their label; "
            f"they count as dead and contribute no change value."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
