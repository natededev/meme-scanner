#!/usr/bin/env python3
"""meme-radar signals -- which first-seen traits predict a token's 1h outcome.

Read-only over the ledger written by ``scanner.py``.  Restricted to the two
high-churn Solana launchpads, tokens that were already worth something when we
first saw them, and pairs that had real liquidity.

For each dex, tokens are bucketed by five first-seen traits:

* ``volume_5m``      -- the launch-spike volume,
* ``liquidity_usd``  -- how much was actually in the pool,
* ``market_cap``     -- size at first sight,
* ``age``            -- pool age when we first saw it (first_seen - created),
* ``copycats``       -- how many same-symbol pairs appeared in the same hour.

Every bucket reports how it did at the ``1h`` label: share up over 2x, its lift
against the dex baseline, share up over 5x, share down over 90%, median change,
the share that went stagnant (volume_5m near zero and market cap unchanged), and
how many of its samples were gone.  A sample that was dead at ``1h``, or had no
market cap recorded there, has no change value: it stays in ``n`` and counts as
down over 90%, so every share divides by the sample size.  A dex-wide BASELINE
row anchors the comparison, and any bucket under ``MIN_BUCKET_SAMPLES`` is
flagged THIN.

``--split`` cuts each dex by first-seen time into an earlier and a later half and
prints ``n``, ``up>2x`` and ``down>90%`` for both halves side by side, which
shows whether a pattern holds in both periods or only in one.

Output is plain ASCII so it survives a Windows cp1252 console or Termux.

Usage:
    python signals.py                  # table per dex
    python signals.py --split          # earlier half vs later half
    python signals.py --csv            # machine readable
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import statistics
import sys
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from scanner import DEFAULT_DB, SNAPSHOT_DEAD, __version__, make_output_robust, parse_isoformat

#: Only these dexes are analysed.
DEXES = ("pump-fun", "pumpswap")

#: A pair must clear both bars at first sight to be a sample.
MIN_FIRST_MARKET_CAP = 1000.0
MIN_FIRST_LIQUIDITY = 1500.0

#: Milestone whose outcome every statistic is measured at.
OUTCOME_LABEL = "1h"

#: Buckets with fewer samples than this are flagged THIN.
MIN_BUCKET_SAMPLES = 100

#: "Stagnant" = volume_5m at the label below this, and market cap flat.
NEAR_ZERO_VOLUME = 100.0
UNCHANGED_PCT = 5.0

UP_2X = 100.0
UP_5X = 400.0
DOWN_90 = -90.0

_SAMPLE_SQL = """
SELECT p.pair_address,
       p.token,
       p.first_seen,
       p.pair_created_at,
       p.liquidity_usd AS first_liquidity,
       p.market_cap AS first_market_cap,
       p.volume_5m AS first_volume,
       COALESCE(p.dex_id, '') AS dex_id,
       s.market_cap AS snapshot_market_cap,
       s.volume_5m AS snapshot_volume,
       s.status
FROM pairs AS p
JOIN snapshots AS s ON s.pair_address = p.pair_address AND s.label = ?
ORDER BY p.pair_address
"""


def load_samples(path: str, label: str = OUTCOME_LABEL) -> tuple[list[dict[str, Any]], int]:
    """Eligible pairs plus how many were dropped by the first-seen filters.

    Opens SQLite read-only so the report never runs the scanner's schema.
    """
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(_SAMPLE_SQL, (label,)).fetchall()
    finally:
        conn.close()

    seen = 0
    by_symbol: dict[tuple[str, str], int] = {}
    for row in rows:
        seen += 1
        key = (str(row["token"]), str(row["first_seen"])[:13])  # symbol + clock hour
        by_symbol[key] = by_symbol.get(key, 0) + 1

    samples: list[dict[str, Any]] = []
    for row in rows:
        dex = str(row["dex_id"]).strip().lower()
        if dex not in DEXES:
            continue
        first_cap = row["first_market_cap"]
        first_liq = row["first_liquidity"]
        if first_cap is None or float(first_cap) < MIN_FIRST_MARKET_CAP:
            continue
        if first_liq is None or float(first_liq) < MIN_FIRST_LIQUIDITY:
            continue
        cap = row["snapshot_market_cap"]
        change: float | None = None
        if cap is not None:
            change = (float(cap) - float(first_cap)) / float(first_cap) * 100.0
        key = (str(row["token"]), str(row["first_seen"])[:13])
        samples.append(
            {
                "dex": dex,
                "token": str(row["token"]),
                "first_volume": _number(row["first_volume"]),
                "first_liquidity": float(first_liq),
                "first_market_cap": float(first_cap),
                "age_minutes": _age_minutes(row["first_seen"], row["pair_created_at"]),
                "copycats": by_symbol[key],
                "first_seen": str(row["first_seen"]),
                "change": change,
                "snapshot_volume": _number(row["snapshot_volume"]),
                "dead": str(row["status"]) == SNAPSHOT_DEAD or change is None,
            }
        )
    return samples, seen


def _number(value: Any) -> float | None:
    """Float or None for a nullable REAL column."""
    return None if value is None else float(value)


def _age_minutes(first_seen: Any, created: Any) -> float | None:
    """Minutes between pool creation and our first sighting; None when unknown."""
    seen_at = parse_isoformat(first_seen)
    made_at = parse_isoformat(created)
    if seen_at is None or made_at is None:
        return None
    return max(0.0, (seen_at - made_at).total_seconds() / 60.0)

#: ``(label, attribute or None, low, high)`` per bucketing dimension.
#: ``None`` means the dimension is bucketed by fixed bands instead of quartiles.
DIMENSIONS = (
    ("volume_5m", "first_volume", "vol"),
    ("liquidity", "first_liquidity", "liq"),
    ("market_cap", "first_market_cap", "cap"),
    ("age", "age_minutes", "age"),
    ("copycats", "copycats", "copies"),
)

AGE_BANDS = ((0.0, 15.0, "age 0-15m"), (15.0, 60.0, "age 15-60m"), (60.0, 240.0, "age 1-4h"), (240.0, None, "age 4h+"))
COPYCAT_BANDS = ((1, 1, "1 pair / symbol / hour"), (2, 2, "2 pairs"), (3, 5, "3-5 pairs"), (6, None, "6+ pairs"))


def _quartile_bounds(values: list[float]) -> list[float]:
    """The 25/50/75 cut points of ``values`` (linear interpolation)."""
    ordered = sorted(values)
    cuts = []
    for fraction in (0.25, 0.50, 0.75):
        position = fraction * (len(ordered) - 1)
        low = int(position)
        high = min(low + 1, len(ordered) - 1)
        cuts.append(ordered[low] + (ordered[high] - ordered[low]) * (position - low))
    return cuts


def _band(value: float | None, bounds: list[float], unit: str) -> str:
    """Quartile label for ``value``; ``n/a`` when the trait is unknown."""
    if value is None:
        return f"{unit} n/a"
    names = ("low", "mid", "high", "top")
    index = 0
    while index < len(bounds) and value >= bounds[index]:
        index += 1
    return f"{unit} {names[index]}"


def _fixed_band(value: Any, bands: Sequence[tuple[Any, Any, str]], unit: str) -> str:
    """First band whose ``[low, high]`` contains ``value``; ``n/a`` when unknown."""
    if value is None:
        return f"{unit} n/a"
    for low, high, label in bands:
        if value >= low and (high is None or value <= high):
            return label
    return bands[-1][2]


def bucket_labels(samples: Sequence[dict[str, Any]], attribute: str, unit: str) -> list[str]:
    """One bucket label per sample, for one bucketing dimension."""
    if attribute == "age_minutes":
        return [_fixed_band(s["age_minutes"], AGE_BANDS, "age") for s in samples]
    if attribute == "copycats":
        return [_fixed_band(s["copycats"], COPYCAT_BANDS, "copies") for s in samples]
    values = [s[attribute] for s in samples if s[attribute] is not None]
    bounds = _quartile_bounds(values) if values else []
    return [_band(s[attribute], bounds, unit) for s in samples]


def summarise(samples: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Outcome statistics of one group of samples at ``OUTCOME_LABEL``.

    Every eligible pair is a sample.  A pair whose 1h snapshot is dead or has no
    market cap has no change value, and is counted as down over 90% in every
    share and in the sample size, so the shares always divide by ``n``.
    """
    n = len(samples)
    gone = [s for s in samples if s["change"] is None]
    changes = [s["change"] for s in samples if s["change"] is not None]
    stagnant = [
        s
        for s in samples
        if s["change"] is not None
        and s["snapshot_volume"] is not None
        and s["snapshot_volume"] < NEAR_ZERO_VOLUME
        and abs(s["change"]) <= UNCHANGED_PCT
    ]
    return {
        "n": n,
        "measured": len(changes),
        "gone": len(gone),
        "up_2x": _share(changes, n, UP_2X),
        "up_5x": _share(changes, n, UP_5X),
        "down_90": _share(changes, n, DOWN_90, extra=len(gone)),
        "median": statistics.median(changes) if changes else None,
        "stagnant": len(stagnant) / n * 100.0 if n else None,
    }


def _share(changes: Sequence[float], n: int, threshold: float, extra: int = 0) -> float | None:
    """Percent of all ``n`` samples past ``threshold``; ``extra`` counts as hits.

    ``extra`` carries the gone samples into the down-over-90% bucket, which has no
    change value to test against a threshold.
    """
    if not n:
        return None
    if threshold < 0:
        hits = sum(1 for change in changes if change <= threshold)
    else:
        hits = sum(1 for change in changes if change >= threshold)
    return (hits + extra) / n * 100.0


def fmt_share(value: float | None) -> str:
    """Unsigned percentage, or n/a."""
    return "n/a" if value is None else f"{value:.1f}%"


def fmt_percent(value: float | None) -> str:
    """Signed percentage with an ASCII hyphen, or n/a."""
    return "n/a" if value is None else f"{value:+.1f}%"


def lift(bucket_up_2x: float | None, baseline_up_2x: float | None) -> str:
    """Bucket up-over-2x share divided by the dex baseline; n/a when undefined."""
    if bucket_up_2x is None or not baseline_up_2x:
        return "n/a"
    return f"{bucket_up_2x / baseline_up_2x:.2f}x"


def table_rows(samples: Sequence[dict[str, Any]]) -> list[tuple[str, ...]]:
    """One row per BASELINE and per non-empty bucket, with lift against BASELINE."""
    baseline = summarise(samples)
    rows: list[tuple[str, ...]] = [("BASELINE", "all samples", *_row_cells(baseline, baseline))]
    for dimension, groups in bucket_groups(samples).items():
        for label, group in groups:
            rows.append((dimension, label, *_row_cells(summarise(group), baseline)))
    return rows


def bucket_groups(samples: Sequence[dict[str, Any]]) -> dict[str, list[tuple[str, list[dict[str, Any]]]]]:
    """``dimension -> [(bucket label, samples)]`` across every bucketing dimension."""
    groups: dict[str, list[tuple[str, list[dict[str, Any]]]]] = {}
    for dimension, attribute, unit in DIMENSIONS:
        labels = bucket_labels(samples, attribute, unit)
        collected: dict[str, list[dict[str, Any]]] = {}
        for label, sample in zip(labels, samples):
            collected.setdefault(label, []).append(sample)
        groups[dimension] = [(label, collected[label]) for label in sorted(collected)]
    return groups


def _row_cells(summary: dict[str, Any], baseline: dict[str, Any]) -> tuple[Any, ...]:
    """The metric cells, with a THIN marker under the sample count."""
    thin = " THIN" if summary["n"] < MIN_BUCKET_SAMPLES else ""
    return (
        f"{summary['n']:,}{thin}",
        fmt_share(summary["up_2x"]),
        lift(summary["up_2x"], baseline["up_2x"]),
        fmt_share(summary["up_5x"]),
        fmt_share(summary["down_90"]),
        fmt_percent(summary["median"]),
        fmt_share(summary["stagnant"]),
        f"{summary['gone']:,}",
    )


def split_halves(samples: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Tag every sample ``earlier`` or ``later`` by its first-seen time.

    Sorted by the stored ISO timestamp, so the split is a genuine time cut; an
    odd count puts the extra sample in the later half.
    """
    ordered = sorted(samples, key=lambda sample: sample["first_seen"])
    middle = len(ordered) // 2
    for index, sample in enumerate(ordered):
        sample["half"] = "earlier" if index < middle else "later"
    return ordered


def split_rows(samples: Sequence[dict[str, Any]]) -> list[tuple[str, ...]]:
    """``(dimension, bucket, earlier n/up>2x/down>90%, later n/up>2x/down>90%)``.

    One row per bucket with both halves side by side, so a pattern can be read
    across time without scrolling.
    """
    split_halves(samples)
    rows: list[tuple[str, ...]] = []
    for dimension, label, group in [("BASELINE", "all samples", samples), *_all_buckets(samples)]:
        cells: list[Any] = [dimension, label]
        for half in ("earlier", "later"):
            summary = summarise([s for s in group if s["half"] == half])
            cells += [
                f"{summary['n']:,}{' THIN' if summary['n'] < MIN_BUCKET_SAMPLES else ''}",
                fmt_share(summary["up_2x"]),
                fmt_share(summary["down_90"]),
            ]
        rows.append(tuple(cells))
    return rows


def _all_buckets(samples: Sequence[dict[str, Any]]) -> list[tuple[str, str, list[dict[str, Any]]]]:
    """Flatten ``bucket_groups`` into ``(dimension, label, samples)`` triples."""
    return [
        (dimension, label, group)
        for dimension, groups in bucket_groups(samples).items()
        for label, group in groups
    ]


HEADERS = ("dimension", "bucket", "n", "up>2x", "lift", "up>5x", "down>90%", "median", "stagnant", "gone")
CSV_HEADERS = (
    "dex",
    "dimension",
    "bucket",
    "n",
    "pct_up_2x",
    "lift",
    "pct_up_5x",
    "pct_down_90",
    "median_change_pct",
    "pct_stagnant",
    "gone",
    "thin",
)
SPLIT_HEADERS = (
    "dimension",
    "bucket",
    "early n",
    "early up>2x",
    "early down>90%",
    "late n",
    "late up>2x",
    "late down>90%",
)
SPLIT_CSV_HEADERS = ("dex",) + SPLIT_HEADERS


def print_table(dex: str, rows: Sequence[tuple[str, ...]], headers: Sequence[str] = HEADERS, title: str | None = None) -> None:
    """Render one dex's rows as a plain ASCII table under ``headers``."""
    widths = [max([len(headers[i])] + [len(row[i]) for row in rows]) for i in range(len(headers))]
    rule = "+" + "+".join("-" * (width + 2) for width in widths) + "+"
    if title:
        print(title)
    else:
        print(f"=== dex: {dex} ({rows[0][2] if rows else 0} samples, outcome at {OUTCOME_LABEL}) ===")
    print(rule)
    print("| " + " | ".join(h.ljust(w) for h, w in zip(headers, widths)) + " |")
    print(rule)
    for row in rows:
        print("| " + " | ".join(cell.rjust(w) for cell, w in zip(row, widths)) + " |")
    print(rule)
    print()


def _csv_line(values: Sequence[str]) -> None:
    """One CSV record, newline-terminated exactly like `print_csv` writes them."""
    csv.writer(sys.stdout, lineterminator="\n").writerow(values)


def print_csv(dex: str, rows: Sequence[tuple[str, ...]]) -> None:
    """CSV rows for one dex; the caller writes the header once."""
    writer = csv.writer(sys.stdout, lineterminator="\n")
    for row in rows:
        dimension, bucket, n, up_2x, up_2x_lift, up_5x, down_90, median, stagnant, gone = row
        writer.writerow(
            [
                dex,
                dimension,
                bucket,
                int(str(n).replace(",", "").split()[0]),
                up_2x,
                up_2x_lift,
                up_5x,
                down_90,
                median,
                stagnant,
                int(str(gone).replace(",", "")),
                int("THIN" in str(n)),
            ]
        )


def print_csv_split(dex: str, rows: Sequence[tuple[str, ...]]) -> None:
    """CSV rows for the earlier/later split of one dex."""
    writer = csv.writer(sys.stdout, lineterminator="\n")
    for row in rows:
        writer.writerow([dex, *[_plain(cell) for cell in row]])


def _plain(cell: Any) -> Any:
    """Strip the THIN marker and thousands separators from a cell."""
    text = str(cell).replace(",", "").replace(" THIN", "")
    return text


def build_parser() -> argparse.ArgumentParser:
    """Command line interface of the signal report."""
    parser = argparse.ArgumentParser(
        prog="signals.py",
        description="First-seen traits vs the 1h outcome, per dex.",
    )
    parser.add_argument("--db", default=DEFAULT_DB, metavar="PATH", help="SQLite file (default: %(default)s)")
    parser.add_argument("--csv", action="store_true", help="write CSV instead of the tables")
    parser.add_argument(
        "--split",
        action="store_true",
        help="split each dex by first-seen time into an earlier and a later half, "
        "and print n, up>2x and down>90%% for both halves side by side",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Print the signal tables for the ledger in --db."""
    args = build_parser().parse_args(argv)
    make_output_robust()

    try:
        samples, scanned = load_samples(args.db)
    except sqlite3.Error as exc:
        print(f"{args.db}: {exc}", file=sys.stderr)
        print("hint: run scanner.py against this database first; it creates the snapshots table.", file=sys.stderr)
        return 1

    if not samples:
        if args.csv:
            _csv_line(SPLIT_CSV_HEADERS if args.split else CSV_HEADERS)
        else:
            print(f"No eligible pairs in {args.db} yet.")
            print(
                f"Need dex in ({', '.join(DEXES)}), first-seen market cap >= ${MIN_FIRST_MARKET_CAP:,.0f}, "
                f"liquidity >= ${MIN_FIRST_LIQUIDITY:,.0f}, and a {OUTCOME_LABEL} snapshot."
            )
        return 0

    subsets = {dex: [sample for sample in samples if sample["dex"] == dex] for dex in DEXES}

    if args.csv:
        _csv_line(SPLIT_CSV_HEADERS if args.split else CSV_HEADERS)
        for dex, subset in subsets.items():
            if not subset:
                continue
            if args.split:
                print_csv_split(dex, split_rows(subset))
            else:
                print_csv(dex, table_rows(subset))
        return 0

    print(f"meme-radar signals -- {args.db}")
    print(
        f"{scanned:,} pair(s) with a {OUTCOME_LABEL} snapshot; {len(samples):,} pass "
        f"dex in ({', '.join(DEXES)}) and first-seen cap >= ${MIN_FIRST_MARKET_CAP:,.0f} "
        f"and liquidity >= ${MIN_FIRST_LIQUIDITY:,.0f}"
    )
    print(f"stagnant = {OUTCOME_LABEL} volume_5m < ${NEAR_ZERO_VOLUME:,.0f} and market cap within {UNCHANGED_PCT:.0f}% of first seen")
    print(f"gone = dead at {OUTCOME_LABEL} or no market cap; counted as down>90% and kept in n")
    print(f"lift = bucket up>2x share / that dex's BASELINE up>2x share")
    print(f"THIN marks a bucket with fewer than {MIN_BUCKET_SAMPLES} samples")
    print()
    for dex, subset in subsets.items():
        if not subset:
            print(f"=== dex: {dex} (0 samples) ===")
            print()
            continue
        if args.split:
            split_halves(subset)
            earlier = [s for s in subset if s.get("half") == "earlier"]
            later = [s for s in subset if s.get("half") == "later"]
            print_table(
                dex,
                split_rows(subset),
                headers=SPLIT_HEADERS,
                title=(
                    f"=== dex: {dex} (earlier half to {earlier[-1]['first_seen'] if earlier else 'n/a'}, "
                    f"later half from {later[0]['first_seen'] if later else 'n/a'}) ==="
                ),
            )
            continue
        print_table(dex, table_rows(subset))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
