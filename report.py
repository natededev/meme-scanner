#!/usr/bin/env python3
"""meme-radar report -- market-cap drift and mortality per snapshot label.

Reads the ledger written by ``scanner.py`` and prints, for every snapshot label
(5m, 15m, 1h, 4h):

* how many pairs have a snapshot for that label,
* the median percentage change in market cap versus the pair's first-seen value,
* the mean change, as a check on how long the tail is,
* the median market cap at that label,
* how many of those pools died before their snapshot was taken.

Usage:
    python report.py                     # every label, table output
    python report.py --label 1h          # single label
    python report.py --csv               # machine readable
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import statistics
import sys
from collections.abc import Sequence
from typing import Any

from scanner import (
    DEFAULT_DB,
    SNAPSHOT_DEAD,
    SNAPSHOT_LABELS,
    Store,
    __version__,
    fmt_money,
    make_output_robust,
)

HEADERS = ("label", "snapshots", "alive", "dead", "mortality", "median mcap", "median chg", "mean chg")
CSV_HEADERS = ("label", "snapshots", "alive", "dead", "mortality_pct", "median_market_cap", "median_change_pct", "mean_change_pct")


def collect(store: Store, wanted: str | None = None) -> dict[str, dict[str, Any]]:
    """Aggregate every stored snapshot into one bucket per label.

    Dead pools are counted but excluded from the change columns, because their
    market cap at that label is unknown rather than zero.
    """
    stats: dict[str, dict[str, Any]] = {}
    for row in store.snapshot_rows():
        label = str(row["label"])
        if wanted is not None and label != wanted:
            continue
        bucket = stats.setdefault(
            label,
            {"snapshots": 0, "alive": 0, "dead": 0, "no_baseline": 0, "changes": [], "market_caps": []},
        )
        bucket["snapshots"] += 1

        if str(row["status"]) == SNAPSHOT_DEAD:
            bucket["dead"] += 1
            continue
        bucket["alive"] += 1

        market_cap = row["snapshot_market_cap"]
        baseline = row["first_market_cap"]
        if market_cap is not None:
            bucket["market_caps"].append(float(market_cap))
        if market_cap is None or not baseline:  # NULL numbers, or a zero baseline
            bucket["no_baseline"] += 1
            continue
        bucket["changes"].append((float(market_cap) - float(baseline)) / float(baseline) * 100.0)
    return stats


def summarise(bucket: dict[str, Any]) -> dict[str, Any]:
    """Turn one accumulator into the numbers the report shows."""
    snapshots = int(bucket["snapshots"])
    dead = int(bucket["dead"])
    changes: list[float] = bucket["changes"]
    market_caps: list[float] = bucket["market_caps"]
    return {
        "snapshots": snapshots,
        "alive": int(bucket["alive"]),
        "dead": dead,
        "no_baseline": int(bucket["no_baseline"]),
        "mortality_pct": dead / snapshots * 100.0 if snapshots else None,
        "median_market_cap": statistics.median(market_caps) if market_caps else None,
        "median_change_pct": statistics.median(changes) if changes else None,
        "mean_change_pct": statistics.mean(changes) if changes else None,
    }


def fmt_percent(value: float | None) -> str:
    """``+12.3%`` / ``-45.6%`` / ``n/a``."""
    return "n/a" if value is None else f"{value:+.1f}%"


def ordered_labels(stats: dict[str, dict[str, Any]], wanted: str | None) -> list[str]:
    """Configured labels first (5m, 15m, 1h, 4h), then any extra label on file."""
    order = [label for label, _ in SNAPSHOT_LABELS if wanted is None or label == wanted]
    order += [label for label in sorted(stats) if label not in order]
    return order


def print_table(rows: list[tuple[str, dict[str, Any]]]) -> None:
    """Left-aligned table with a header rule."""
    body = [[label, *table_row(summary)] for label, summary in rows]
    widths = [max(len(HEADERS[i]), *(len(row[i]) for row in body)) for i in range(len(HEADERS))]
    header = "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(HEADERS))
    print(header)
    print("-" * len(header))
    for row in body:
        print("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))


def table_row(summary: dict[str, Any]) -> list[str]:
    """Format one label's summary for the console table."""
    mortality = summary["mortality_pct"]
    return [
        f"{summary['snapshots']:,}",
        f"{summary['alive']:,}",
        f"{summary['dead']:,}",
        "n/a" if mortality is None else f"{mortality:.1f}%",
        fmt_money(summary["median_market_cap"]),
        fmt_percent(summary["median_change_pct"]),
        fmt_percent(summary["mean_change_pct"]),
    ]


def print_csv(rows: list[tuple[str, dict[str, Any]]]) -> None:
    """CSV with raw numbers, so a spreadsheet can do its own maths."""
    writer = csv.writer(sys.stdout)
    writer.writerow(CSV_HEADERS)
    for label, summary in rows:
        writer.writerow(
            [
                label,
                summary["snapshots"],
                summary["alive"],
                summary["dead"],
                _csv_number(summary["mortality_pct"]),
                _csv_number(summary["median_market_cap"]),
                _csv_number(summary["median_change_pct"]),
                _csv_number(summary["mean_change_pct"]),
            ]
        )


def _csv_number(value: float | None) -> str:
    """Two-decimal number, or an empty cell when unknown."""
    return "" if value is None else f"{value:.2f}"


def build_parser() -> argparse.ArgumentParser:
    """Command line interface of the report."""
    parser = argparse.ArgumentParser(
        prog="report.py",
        description="Median market-cap drift and mortality per snapshot label.",
    )
    parser.add_argument("--db", default=DEFAULT_DB, metavar="PATH", help="SQLite file (default: %(default)s)")
    parser.add_argument("--label", default=None, metavar="LABEL", help="report a single label (5m, 15m, 1h, 4h)")
    parser.add_argument("--csv", action="store_true", help="write CSV instead of the table")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Print the snapshot report for the ledger in ``--db``."""
    args = build_parser().parse_args(argv)
    make_output_robust()

    with Store(args.db) as store:
        try:
            stats = collect(store, args.label)
        except sqlite3.Error as exc:
            print(f"{store.path}: {exc}", file=sys.stderr)
            print("hint: run scanner.py against this database first; it creates the snapshots table.", file=sys.stderr)
            return 1

        rows = [(label, summarise(stats[label])) for label in ordered_labels(stats, args.label) if label in stats]
        if not rows:
            if args.csv:
                print(",".join(CSV_HEADERS))
            else:
                print(f"No snapshots recorded for {args.label or 'any label'} in {store.path} yet.")
                print("Rows appear once stored pools reach their milestone ages (5m, 15m, 1h, 4h).")
            return 0

        if args.csv:
            print_csv(rows)
            return 0

        pairs, _tokens = store.counts()
        dead_total = sum(summary["dead"] for _label, summary in rows)
        print(f"meme-radar snapshot report -- {store.path}")
        print(f"{pairs:,} pairs stored | {store.snapshot_count():,} snapshots | {dead_total:,} dead at their label")
        print()
        print_table(rows)

        missing = sum(summary["no_baseline"] for _label, summary in rows)
        if missing:
            print()
            print(f"note: {missing:,} alive snapshot(s) have no market cap or no first-seen baseline,")
            print("      so they are excluded from the change columns.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
