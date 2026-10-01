#!/usr/bin/env python3
"""sellcheck -- could you actually have sold these tokens for USDC?

Read-only over the ledger written by ``scanner.py``, plus live read-only calls
to Jupiter's public swap quote API.

Jupiter's docs (developers.jup.ag, checked for this script) specify:

* ``GET https://api.jup.ag/swap/v1/quote`` with ``inputMint``, ``outputMint``,
  ``amount`` (raw base units), ``slippageBps`` and optional ``restrictIntermediateTokens``;
* an ``x-api-key`` header on the free tier, which allows 1 request/second and
  60 requests/minute on a 60-second sliding window (the keyless tier is
  0.5 rps / 30 rpm), enforced per organisation;
* a 200 response carrying ``outAmount`` (raw USDC base units) and
  ``priceImpactPct``; errors come back as a non-200 with ``errorMessage``.

The universe is pumpswap pairs first seen at least ``MIN_AGE_HOURS`` ago whose
5m snapshot showed a real market cap and liquidity, but whose 1h liquidity was
missing, under $100, or under 5% of the 5m liquidity.  Those are exactly the
pools a paper backtest would still treat as sellable.

Output is plain ASCII so it survives a Windows cp1252 console or Termux.

Usage:
    python sellcheck.py --api-key KEY
    python sellcheck.py --n 10 --base-url https://api.jup.ag
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sqlite3
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from scanner import DEFAULT_DB, SNAPSHOT_LABELS, make_output_robust, parse_isoformat

#: Only this launchpad is checked.
DEX = "pumpswap"

#: Entry and exit milestones used for the filters.
ENTRY_LABEL = "5m"
EXIT_LABEL = "1h"

#: A pair must clear both bars at its 5m snapshot.
MIN_ENTRY_MARKET_CAP = 1000.0
MIN_ENTRY_LIQUIDITY = 1500.0

#: Only pools seen at least this long ago, so the 1h snapshot has had time to land.
MIN_AGE_HOURS = 6.0

#: Jupiter's USDC mint on Solana.
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

#: Jupiter base URL and quote path.
DEFAULT_BASE_URL = "https://api.jup.ag"
QUOTE_PATH = "/swap/v1/quote"

#: Free tier allows 1 request/second; stay a little under it.
DEFAULT_SLEEP = 1.2

#: Decimals assumed for a token whose real value is not known.
ASSUMED_DECIMALS = 6

#: USDC has 6 decimals, so raw base units are micro-dollars.
USDC_DECIMALS = 6

DEFAULT_SIZE_USD = 20.0
DEFAULT_SAMPLE = 30
DEFAULT_SLIPPAGE_BPS = 100

_CANDIDATE_SQL = """
SELECT p.pair_address,
       p.token,
       p.token_address,
       p.first_seen,
       p.dex_id,
       e.price_usd AS entry_price,
       e.market_cap AS entry_market_cap,
       e.liquidity_usd AS entry_liquidity,
       x.price_usd AS exit_price,
       x.liquidity_usd AS exit_liquidity,
       x.status AS exit_status
FROM pairs AS p
JOIN snapshots AS e ON e.pair_address = p.pair_address AND e.label = ?
LEFT JOIN snapshots AS x ON x.pair_address = p.pair_address AND x.label = ?
ORDER BY p.first_seen
"""


def load_candidates(path: str, now: datetime) -> list[dict[str, Any]]:
    """Pools whose liquidity looked sellable at 5m and gone by 1h.

    Opens SQLite read-only so the check never runs the scanner's schema.  The
    "now" for the age test is taken from the caller so runs are reproducible.
    """
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(_CANDIDATE_SQL, (ENTRY_LABEL, EXIT_LABEL)).fetchall()
    finally:
        conn.close()

    cutoff = now - timedelta(hours=MIN_AGE_HOURS)
    candidates: list[dict[str, Any]] = []
    for row in rows:
        if str(row["dex_id"]).strip().lower() != DEX:
            continue
        first_seen = parse_isoformat(row["first_seen"])
        if first_seen is None or first_seen > cutoff:
            continue
        entry_cap = row["entry_market_cap"]
        entry_liquidity = row["entry_liquidity"]
        entry_price = row["entry_price"]
        if entry_cap is None or float(entry_cap) < MIN_ENTRY_MARKET_CAP:
            continue
        if entry_liquidity is None or float(entry_liquidity) < MIN_ENTRY_LIQUIDITY:
            continue
        if entry_price is None or float(entry_price) <= 0.0:
            continue
        exit_liquidity = row["exit_liquidity"]
        liquidity_gone = (
            exit_liquidity is None
            or float(exit_liquidity) < 100.0
            or float(exit_liquidity) < float(entry_liquidity) * 0.05
        )
        if not liquidity_gone:
            continue
        candidates.append(
            {
                "pair_address": str(row["pair_address"]),
                "symbol": str(row["token"]),
                "mint": str(row["token_address"]) if row["token_address"] else "",
                "entry_price": float(entry_price),
                "entry_liquidity": float(entry_liquidity),
                "exit_liquidity": None if exit_liquidity is None else float(exit_liquidity),
            }
        )
    return candidates


def newest_first_seen(path: str) -> datetime | None:
    """Latest ``first_seen`` in the ledger, used as "now" when none is given."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    try:
        row = conn.execute("SELECT MAX(first_seen) FROM pairs").fetchone()
    finally:
        conn.close()
    if row is None or row[0] is None:
        return None
    return parse_isoformat(row[0])


def amount_for_usd(price_usd: float, size_usd: float, decimals: int) -> int:
    """Raw token units worth about ``size_usd`` at ``price_usd``."""
    return max(1, round(size_usd / price_usd * (10**decimals)))


def quote_url(
    base_url: str,
    mint: str,
    amount: int,
    slippage_bps: int,
    restrict_intermediate: bool = True,
) -> str:
    """Build the Jupiter v1 quote URL for an exact-in sell of ``amount``."""
    query = urllib.parse.urlencode(
        {
            "inputMint": mint,
            "outputMint": USDC_MINT,
            "amount": amount,
            "slippageBps": slippage_bps,
            "swapMode": "ExactIn",
            "restrictIntermediateTokens": "true" if restrict_intermediate else "false",
        }
    )
    return f"{base_url.rstrip('/')}{QUOTE_PATH}?{query}"


def fetch_quote(
    url: str,
    api_key: str | None,
    timeout: float = 20.0,
    opener: Any = urllib.request.urlopen,
) -> dict[str, Any]:
    """One quote call.  Returns a result dict, never raises for HTTP failures.

    A 200 with a route gives ``route=True`` plus ``out_usdc`` and
    ``price_impact_pct``.  A 200 with an empty routePlan, or any 4xx/5xx, gives
    ``route=False`` and an ``error`` string the caller can print.
    """
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    if api_key:
        request.add_header("x-api-key", api_key)
    try:
        with opener(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", "replace")
            status = getattr(response, "status", 200) or 200
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace") if hasattr(exc, "read") else ""
        return {"route": False, "error": f"HTTP {exc.code}", "detail": _error_message(detail), "retry_after": exc.headers.get("Retry-After") if exc.headers else None}
    except urllib.error.URLError as exc:
        return {"route": False, "error": f"network: {exc.reason}", "detail": ""}
    except TimeoutError:
        return {"route": False, "error": "timeout", "detail": ""}

    if status != 200:
        return {"route": False, "error": f"HTTP {status}", "detail": _error_message(body)}
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return {"route": False, "error": "bad json", "detail": body[:120]}
    if not isinstance(payload, dict):
        return {"route": False, "error": "unexpected payload", "detail": str(payload)[:120]}
    route = payload.get("routePlan") or []
    if not route:
        return {"route": False, "error": "no route", "detail": _error_message(body)}
    out_amount = payload.get("outAmount")
    if out_amount is None:
        return {"route": False, "error": "no outAmount", "detail": ""}
    return {
        "route": True,
        "out_usdc": int(out_amount) / (10**USDC_DECIMALS),
        "price_impact_pct": _as_float(payload.get("priceImpactPct")),
        "error": "",
        "detail": "",
    }


def _error_message(body: str) -> str:
    """Pull ``errorMessage`` out of a Jupiter error body when present."""
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        return body[:120].replace("\n", " ")
    if isinstance(payload, dict):
        for key in ("errorMessage", "message", "error"):
            value = payload.get(key)
            if value:
                return str(value)[:120]
    return str(payload)[:120]


def _as_float(value: Any) -> float | None:
    """Float or None for a value Jupiter may send as a string."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


HEADERS = ("symbol", "mint", "route", "expected", "quoted", "ratio", "impact", "note")
CSV_HEADERS = ("symbol", "mint", "route_found", "expected_usd", "quoted_usdc", "ratio", "price_impact_pct", "note")


def fmt_money(value: float | None) -> str:
    """``$19.87`` / ``n/a``."""
    return "n/a" if value is None else f"${value:,.2f}"


def fmt_ratio(value: float | None) -> str:
    """``1.00x`` / ``0.00x`` / ``n/a``."""
    return "n/a" if value is None else f"{value:.2f}x"


def fmt_impact(value: float | None) -> str:
    """Price impact as a percentage, or n/a."""
    return "n/a" if value is None else f"{value * 100:.2f}%"


def result_rows(results: Sequence[dict[str, Any]]) -> list[tuple[str, ...]]:
    """One table row per checked token."""
    return [
        (
            item["symbol"],
            item["mint"][:8] + ".." if len(item["mint"]) > 10 else (item["mint"] or "n/a"),
            "yes" if item["route"] else "NO",
            fmt_money(item["expected"]),
            fmt_money(item["quoted"]),
            fmt_ratio(item["ratio"]),
            fmt_impact(item["impact"]),
            item["note"],
        )
        for item in results
    ]


def print_table(rows: Sequence[tuple[str, ...]]) -> None:
    """Render the per-token table as plain ASCII."""
    if not rows:
        print("(nothing checked)")
        return
    widths = [max([len(HEADERS[i])] + [len(row[i]) for row in rows]) for i in range(len(HEADERS))]
    print("+" + "+".join("-" * (width + 2) for width in widths) + "+")
    print("| " + " | ".join(h.ljust(w) for h, w in zip(HEADERS, widths)) + " |")
    print("+" + "+".join("-" * (width + 2) for width in widths) + "+")
    for row in rows:
        print("| " + " | ".join(cell.rjust(w) for cell, w in zip(row, widths)) + " |")
    print("+" + "+".join("-" * (width + 2) for width in widths) + "+")


def print_csv(rows: Sequence[dict[str, Any]]) -> None:
    """CSV records for every checked token."""
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(CSV_HEADERS)
    for item in rows:
        writer.writerow(
            [
                item["symbol"],
                item["mint"],
                int(bool(item["route"])),
                "" if item["expected"] is None else f"{item['expected']:.2f}",
                "" if item["quoted"] is None else f"{item['quoted']:.4f}",
                "" if item["ratio"] is None else f"{item['ratio']:.4f}",
                "" if item["impact"] is None else f"{item['impact']:.6f}",
                item["note"],
            ]
        )


def check_token(
    candidate: dict[str, Any],
    args: argparse.Namespace,
    opener: Any,
    sleep: Any,
) -> dict[str, Any]:
    """Quote one token's sell and turn the response into a result row."""
    mint = candidate["mint"]
    expected = args.size
    item: dict[str, Any] = {
        "symbol": candidate["symbol"],
        "mint": mint,
        "route": False,
        "expected": expected,
        "quoted": None,
        "ratio": None,
        "impact": None,
        "note": "",
    }
    if not mint:
        item["note"] = "no token address stored"
        return item

    decimals = args.decimals
    amount = amount_for_usd(candidate["entry_price"], args.size, decimals)
    url = quote_url(args.base_url, mint, amount, args.slippage_bps)
    response = fetch_quote(url, args.api_key, args.timeout, opener)

    if response["route"]:
        quoted = float(response["out_usdc"])
        item["route"] = True
        item["quoted"] = quoted
        item["ratio"] = quoted / expected if expected else None
        item["impact"] = response["price_impact_pct"]
        item["note"] = f"{amount} units @ {decimals}dp"
        return item

    item["note"] = response["error"]
    if response.get("detail"):
        item["note"] += f": {response['detail']}"
    # A retryable failure means we never learned whether a route existed.
    if response["error"].startswith(("HTTP 429", "network", "timeout")):
        item["note"] += " (unknown, not counted as no-route)"
        item["note"] = item["note"]
        item["route"] = False
    sleep(args.sleep)
    return item


def build_parser() -> argparse.ArgumentParser:
    """Command line interface of the sell check."""
    parser = argparse.ArgumentParser(
        prog="sellcheck.py",
        description="Can Jupiter still sell pumpswap pools whose liquidity collapsed?",
    )
    parser.add_argument("--db", default=DEFAULT_DB, metavar="PATH", help="SQLite file (default: %(default)s)")
    parser.add_argument("--n", type=int, default=DEFAULT_SAMPLE, metavar="COUNT", help="tokens to sample (default: 30)")
    parser.add_argument("--size", type=float, default=DEFAULT_SIZE_USD, metavar="USD", help="sell size per token (default: $20)")
    parser.add_argument("--api-key", default=None, metavar="KEY", help="Jupiter x-api-key (the free tier requires one)")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, metavar="URL", help=f"API base URL (default: {DEFAULT_BASE_URL})")
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, metavar="SEC", help="delay between calls (default: 1.2, free tier is 1 rps)")
    parser.add_argument("--slippage-bps", type=int, default=DEFAULT_SLIPPAGE_BPS, metavar="BPS", help="slippage tolerance (default: 100)")
    parser.add_argument("--decimals", type=int, default=ASSUMED_DECIMALS, metavar="N", help=f"token decimals when unknown (default: {ASSUMED_DECIMALS})")
    parser.add_argument("--seed", type=int, default=None, metavar="N", help="seed the sampler for a reproducible draw")
    parser.add_argument("--timeout", type=float, default=20.0, metavar="SEC", help="per-request timeout (default: 20)")
    parser.add_argument("--csv", action="store_true", help="write CSV instead of the table")
    parser.add_argument("--mock", default=None, metavar="PATH", help="read canned JSON responses from a file instead of calling Jupiter")
    return parser


def _mock_opener(path: str) -> Any:
    """An opener that replays canned responses from a JSON file.

    Each entry is one response: ``{"status": 200, "body": {...}}`` or
    ``{"error": "HTTP 429"}``.  Used by the tests so nothing hits the network.
    """
    with open(path, encoding="utf-8") as handle:
        responses = json.load(handle)

    state = {"index": 0}

    def opener(request: Any, timeout: float = 0.0) -> Any:
        entry = responses[min(state["index"], len(responses) - 1)]
        state["index"] += 1
        if "error" in entry:
            text = str(entry["error"])
            code = int(text.split()[-1]) if text.split()[-1].isdigit() else 503
            if not text.split()[-1].isdigit():
                raise urllib.error.URLError(text)  # type: ignore[arg-type]
            raise urllib.error.HTTPError(request.full_url, code, text, None, None)  # type: ignore[arg-type]
        body = json.dumps(entry.get("body", {})).encode("utf-8")

        class _Response:
            status = entry.get("status", 200)

            def read(self) -> bytes:
                return body

            def __enter__(self) -> "_Response":
                return self

            def __exit__(self, *exc: object) -> None:
                return None

        return _Response()

    return opener


def main(argv: Sequence[str] | None = None) -> int:
    """Sample collapsed-liquidity pools and ask Jupiter to price a sell."""
    args = build_parser().parse_args(argv)
    make_output_robust()

    try:
        now = newest_first_seen(args.db)
        if now is None:
            print(f"{args.db} holds no pairs yet; run scanner.py first.")
            return 1
        candidates = load_candidates(args.db, now)
    except sqlite3.Error as exc:
        print(f"{args.db}: {exc}", file=sys.stderr)
        print("hint: run scanner.py against this database first; it creates the snapshots table.", file=sys.stderr)
        return 1

    if not candidates:
        print(f"No pools in {args.db} match the filter (dex={DEX}, age >= {MIN_AGE_HOURS:.0f}h, "
              f"5m cap >= ${MIN_ENTRY_MARKET_CAP:,.0f}, 5m liquidity >= ${MIN_ENTRY_LIQUIDITY:,.0f}, "
              "1h liquidity missing/under $100/under 5%).")
        return 0

    rng = random.Random(args.seed)
    sample = candidates if args.n >= len(candidates) else rng.sample(candidates, args.n)
    opener = _mock_opener(args.mock) if args.mock else urllib.request.urlopen

    results: list[dict[str, Any]] = []
    for candidate in sample:
        results.append(check_token(candidate, args, opener, time.sleep))
        if not args.mock and len(results) < len(sample):
            time.sleep(args.sleep)

    if args.csv:
        print_csv(results)
    else:
        print(f"sellcheck -- {args.db}")
        print(f"{len(candidates):,} candidate pool(s); checked {len(results)} at ${args.size:,.2f} each "
              f"(assumed {args.decimals} decimals), slippage {args.slippage_bps} bps")
        print(f"quotes from {args.base_url}{QUOTE_PATH}" + ("  [MOCKED]" if args.mock else ""))
        print()
        print_table(result_rows(results))
        routed = [item for item in results if item["route"]]
        ratios = [item["ratio"] for item in results if item["ratio"] is not None]
        print(f"route found: {len(routed)} of {len(results)}")
        if ratios:
            print(f"median quoted/expected: {statistics.median(ratios):.2f}x")
            best = max(ratios)
            print(f"range: {min(ratios):.2f}x .. {best:.2f}x")
        else:
            print("median quoted/expected: n/a (no route quoted)")
        blocked = sum(1 for item in results if not item["route"] and "unknown" not in item["note"])
        if blocked:
            print(f"{blocked} token(s) had no sellable route")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
