#!/usr/bin/env python3
"""meme-radar -- polls a public DEX API for freshly listed Solana pools.

Every cycle the scanner asks its discovery source for the newest Solana pools,
skips the ones already in its local SQLite ledger, and announces the rest on
stdout together with how many minutes old they were when first seen::

    [NEW] $LIES.LIES  dex=pump-fun  age=1.4m  liq=$2.67K  mcap=$6.73K  ...

Two sources are available through ``--source``:

* ``geckoterminal`` (default) - ``GET /networks/solana/new_pools`` returns the
  newest pools *with* their metrics in a single request.
* ``dexscreener`` - the original implementation: recent token profiles, then
  every token's pools (``/token-profiles/latest/v1`` + ``/token-pairs/v1/...``).

Usage:
    python scanner.py                                    # geckoterminal, 30s loop
    python scanner.py --once                             # single pass
    python scanner.py --source dexscreener --interval 15
    python scanner.py --pages 2 --min-liquidity 5000 --db pairs.db

Rate limits are enforced client side per endpoint family: GeckoTerminal's
public API allows 30 calls/minute, DexScreener 60/minute for token profiles and
300/minute for token pairs.

An HTTP 429 that carries no ``Retry-After`` header cools the *whole host* down
for 20s, then 40s, 80s, doubling up to 120s - and every remaining call of the
pass waits that cooldown out rather than going back for more a second later.
"""

from __future__ import annotations

import argparse
import logging
import math
import random
import signal
import sqlite3
import sys
import threading
import time
from abc import ABC
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

__version__ = "0.3.0"

GECKOTERMINAL_BASE_URL = "https://api.geckoterminal.com/api/v2"
DEXSCREENER_BASE_URL = "https://api.dexscreener.com"
CHAIN_ID = "solana"
DEFAULT_DB = "meme_radar.db"
DEFAULT_INTERVAL = 30.0
DEFAULT_MAX_PAIR_AGE_HOURS = 24.0
DEFAULT_MAX_TOKENS_PER_SCAN = 25
DEFAULT_PAGES = 1
DEFAULT_SOURCE = "geckoterminal"
SOURCES = ("geckoterminal", "dexscreener")
GECKOTERMINAL_CALLS_PER_MINUTE = 30
PROFILES_PER_MINUTE = 60
TOKEN_PAIRS_PER_MINUTE = 300
# A 429 without a Retry-After header cools the *whole host* down: 20s, 40s, 80s,
# doubling up to RATE_LIMIT_MAX_BACKOFF.  The streak is counted per client rather
# than per call, so one rate-limited response slows down every later call in the
# pass instead of each call starting its escalation over again.
RATE_LIMIT_BACKOFF = 20.0
RATE_LIMIT_MAX_BACKOFF = 120.0
# Snapshot milestones: a pair gets one row per label, taken once its age passes it.
SNAPSHOT_LABELS: tuple[tuple[str, int], ...] = (("5m", 300), ("15m", 900), ("1h", 3600), ("4h", 14400))
DEFAULT_SNAPSHOT_GRACE_MINUTES = 15.0
SNAPSHOT_BATCH_SIZE = 30  # GeckoTerminal's multi-pools endpoint accepts up to 30 addresses
SNAPSHOT_ALIVE = "alive"
SNAPSHOT_DEAD = "dead"
USER_AGENT = f"meme-radar/{__version__} (python-httpx; Solana new-pool scanner)"

log = logging.getLogger("meme_radar")


def make_output_robust() -> None:
    """Stop exotic characters from aborting a run on a narrow console code page.

    Emoji-heavy Solana tickers (and a few report glyphs) are not encodable in
    e.g. cp1252; degrading them beats a UnicodeEncodeError mid-pass.
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except (AttributeError, ValueError, OSError):
            pass


def setup_logging(level: str) -> None:
    """Send everything to stdout with a timestamped, single-line format."""
    make_output_robust()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(fmt="%(asctime)s %(levelname)-7s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)

    resolved = getattr(logging, str(level).upper(), logging.INFO)
    root.setLevel(resolved)
    log.setLevel(resolved)

    # httpx/httpcore log one line per request at INFO, which drowns out the
    # pair announcements we actually care about on the console.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def utc_now() -> datetime:
    """Current time as an aware UTC datetime."""
    return datetime.now(timezone.utc)


def isoformat(moment: datetime) -> str:
    """Render a datetime as a compact ISO-8601 UTC string ('2026-09-29T12:31:05Z')."""
    return moment.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_isoformat(value: Any) -> datetime | None:
    """Parse a string produced by :func:`isoformat`; returns None on junk."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def to_float(value: Any) -> float | None:
    """Best-effort numeric coercion; None for junk, booleans, NaN and infinity."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def iso_from_millis(value: Any) -> str | None:
    """Convert an epoch-milliseconds field (pairCreatedAt) to an ISO-8601 UTC string."""
    millis = to_float(value)
    if millis is None:
        return None
    try:
        return isoformat(datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc))
    except (OverflowError, OSError, ValueError):
        return None


def fmt_money(value: float | None) -> str:
    """Format a USD amount compactly for the console: $41.20K, $1.42M, $780.00."""
    if value is None:
        return "n/a"
    magnitude = abs(value)
    for threshold, suffix in ((1_000_000_000.0, "B"), (1_000_000.0, "M"), (1_000.0, "K")):
        if magnitude >= threshold:
            return f"${value / threshold:,.2f}{suffix}"
    return f"${value:,.2f}"


def fmt_age(minutes: float | None) -> str:
    """Render how old a pool was when first seen: '3.2m', or 'unknown'."""
    if minutes is None:
        return "unknown"
    return f"{max(0.0, minutes):.1f}m"


def normalize_isoformat(value: Any) -> str | None:
    """Normalise any ISO-8601 timestamp to the '...Z' form used in the database."""
    parsed = parse_isoformat(value)
    return isoformat(parsed) if parsed is not None else None


def relationship_id(payload: dict[str, Any], name: str) -> str | None:
    """Read ``relationships.<name>.data.id`` out of a JSON:API resource."""
    relationships = payload.get("relationships")
    if not isinstance(relationships, dict):
        return None
    relation = relationships.get(name)
    if not isinstance(relation, dict):
        return None
    data = relation.get("data")
    if not isinstance(data, dict):
        return None
    identifier = data.get("id")
    return identifier if isinstance(identifier, str) and identifier else None


def strip_chain_prefix(identifier: str | None) -> str | None:
    """Turn a GeckoTerminal id like 'solana_<mint>' into '<mint>'."""
    if not identifier or "_" not in identifier:
        return identifier
    return identifier.split("_", 1)[1]


def symbol_from_pool_name(value: Any) -> str:
    """GeckoTerminal names pools '<base symbol> / <quote symbol>'."""
    if not isinstance(value, str):
        return ""
    base_symbol = value.split("/", 1)[0].strip()
    # Some deployers prefix symbols with bidi/zero-width marks; drop them.
    return base_symbol.lstrip("\u200b\u200e\u200f\u202a\u202b\u202c\u202d\u202e\ufeff").strip()


def chunked(items: Sequence[Any], size: int) -> Iterator[list[Any]]:
    """Yield ``items`` as lists of at most ``size`` elements (never empty lists)."""
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= max(1, size):
            yield batch
            batch = []
    if batch:
        yield batch


def resource_address(payload: dict[str, Any]) -> str | None:
    """Pool address of a GeckoTerminal pool resource (``attributes.address``, else the id)."""
    attributes = payload.get("attributes")
    if isinstance(attributes, dict):
        address = attributes.get("address")
        if isinstance(address, str) and address:
            return address
    identifier = payload.get("id")
    return strip_chain_prefix(identifier if isinstance(identifier, str) else None)


class RateLimiter:
    """Spaces out calls so they never exceed ``max_calls_per_minute``.

    ``acquire()`` blocks until the next call is allowed, so we stay inside the
    documented limits instead of provoking 429s; ``penalize()`` pushes the next
    slot further out, which is what we want after a rate-limit response.
    """

    def __init__(self, max_calls_per_minute: int, name: str = "endpoint") -> None:
        self.name = name
        self._min_interval = 60.0 / max(1, int(max_calls_per_minute))
        self._next_allowed_at = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> float:
        """Block until a slot is free and return how long we slept."""
        with self._lock:
            now = time.monotonic()
            wait = self._next_allowed_at - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self._next_allowed_at = now + self._min_interval
        return max(0.0, wait)

    def penalize(self, seconds: float) -> None:
        """Delay the next call by ``seconds`` (used after an HTTP 429)."""
        if seconds <= 0:
            return
        with self._lock:
            self._next_allowed_at = max(self._next_allowed_at, time.monotonic() + seconds)


class ApiClient(ABC):
    """Shared HTTP plumbing for the discovery sources.

    Owns the HTTP client, the retry/backoff logic, ``Retry-After`` handling, the
    host-wide rate-limit cooldown and the per-endpoint rate limiters; subclasses
    only describe how to turn their own payloads into :class:`NewPair` objects.
    """

    name = "api"

    def __init__(
        self,
        *,
        base_url: str,
        timeout: float = 10.0,
        max_attempts: int = 4,
        base_backoff: float = 2.0,
        max_backoff: float = 60.0,
        rate_limit_backoff: float = RATE_LIMIT_BACKOFF,
        rate_limit_max_backoff: float = RATE_LIMIT_MAX_BACKOFF,
    ) -> None:
        self.max_attempts = max(1, int(max_attempts))
        self.base_backoff = max(0.1, float(base_backoff))
        self.max_backoff = max(self.base_backoff, float(max_backoff))
        self.rate_limit_backoff = max(0.0, float(rate_limit_backoff))
        self.rate_limit_max_backoff = max(self.rate_limit_backoff, float(rate_limit_max_backoff))
        self._limiters: list[RateLimiter] = []
        # Rate limiting is a property of the host, not of one request: a 429 sets a
        # cooldown deadline that *every* later call to this client waits out, and a
        # streak counter that keeps doubling until the host starts answering again.
        self._cooldown_until = 0.0
        self._rate_limit_streak = 0
        # Set by main() so long cooldowns do not have to run to completion on shutdown.
        self.stop_event: threading.Event | None = None

        self._client = httpx.Client(
            base_url=base_url,
            timeout=httpx.Timeout(float(timeout), connect=min(float(timeout), 5.0)),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            follow_redirects=True,
        )

    def __enter__(self) -> "ApiClient":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Release the underlying connection pool."""
        self._client.close()

    def limiter(self, name: str, calls_per_minute: int) -> RateLimiter:
        """Register and return a limiter for one endpoint family."""
        limiter = RateLimiter(calls_per_minute, name)
        self._limiters.append(limiter)
        return limiter

    def discover(self, store: "Store") -> list["NewPair"] | None:
        """Return every pair this source currently advertises.

        ``None`` means the feed itself was unavailable; the scanner reports a
        failed pass instead of an empty one in that case.  Concrete subclasses
        override this; a bare client (e.g. for snapshots) has no feed.
        """
        raise NotImplementedError(f"{type(self).__name__} does not provide a discovery feed")

    # -- internals ---------------------------------------------------------

    def _backoff_delay(self, attempt: int, base: float | None = None) -> float:
        """Exponential backoff with jitter: 2s, 4s, 8s ... capped at max_backoff."""
        start = self.base_backoff if base is None else base
        delay = min(start * (2 ** (attempt - 1)), self.max_backoff)
        return delay * random.uniform(0.85, 1.15)

    @staticmethod
    def _retry_after(response: httpx.Response) -> float | None:
        """Parse a Retry-After header (seconds or HTTP-date), capped at 120s."""
        raw = response.headers.get("Retry-After")
        if not raw:
            return None
        raw = raw.strip()
        try:
            seconds = float(raw)
        except ValueError:
            try:
                deadline = datetime.strptime(raw, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
            except ValueError:
                return None
            seconds = (deadline - utc_now()).total_seconds()
        return max(1.0, min(seconds, 120.0))

    def _sleep(self, seconds: float) -> bool:
        """Sleep ``seconds``, returning ``True`` when a stop was requested early.

        Cooldowns can run to two minutes, so they are sliced and checked against
        ``stop_event``: a shutdown should cut them short rather than sit them out.
        """
        deadline = time.monotonic() + max(0.0, seconds)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            if self.stop_event is not None and self.stop_event.is_set():
                return True
            time.sleep(min(remaining, 1.0))

    def _await_cooldown(self, path: str) -> bool:
        """Wait out any host-wide rate-limit cooldown before hitting ``path``.

        This is what keeps a *pass* patient: one rate-limited response parks every
        later call to this host - other endpoints and batches included - instead of
        letting them resume at the limiter's normal spacing a second or two later.
        Returns ``True`` when a stop was requested while waiting.
        """
        remaining = self._cooldown_until - time.monotonic()
        if remaining <= 0:
            return False
        log.info("rate-limit cooldown active - waiting %.1fs before %s", remaining, path)
        return self._sleep(remaining)

    def _rate_limit_delay(self) -> float:
        """Next cooldown length for a 429 without ``Retry-After``: 20s, 40s, 80s ... 120s."""
        self._rate_limit_streak += 1
        return min(
            self.rate_limit_backoff * (2 ** (self._rate_limit_streak - 1)),
            self.rate_limit_max_backoff,
        )

    def _get_json(self, path: str, *, limiter: RateLimiter, params: dict[str, Any] | None = None) -> Any | None:
        """GET ``path`` and decode JSON, retrying transient failures.

        Returns the decoded payload, or ``None`` when the resource could not be
        fetched after ``max_attempts`` (the caller treats that as "no data this
        cycle" and moves on rather than crashing).
        """
        for attempt in range(1, self.max_attempts + 1):
            if self._await_cooldown(path):
                log.info("stop requested - abandoning %s", path)
                return None
            limiter.acquire()
            try:
                response = self._client.get(path, params=params)
            except httpx.RequestError as exc:
                delay = self._backoff_delay(attempt)
                log.warning(
                    "network error on %s (%s: %s) - attempt %d/%d, retrying in %.1fs",
                    path, type(exc).__name__, exc, attempt, self.max_attempts, delay,
                )
                time.sleep(delay)
                continue

            if response.status_code == 429:
                retry_after = self._retry_after(response)
                delay = retry_after if retry_after is not None else self._rate_limit_delay()
                # The cooldown belongs to the host, not to this one request: every
                # later call - other endpoints and the remaining batches of this
                # pass included - waits it out instead of resuming at 2s spacing.
                self._cooldown_until = max(self._cooldown_until, time.monotonic() + delay)
                limiter.penalize(delay)
                log.warning(
                    "rate limited (HTTP 429) on %s - cooling down all %s calls for %.1fs%s (attempt %d/%d)",
                    path, self.name, delay,
                    " (Retry-After)" if retry_after is not None else " (no Retry-After)",
                    attempt, self.max_attempts,
                )
                if self._sleep(delay):
                    log.info("stop requested - abandoning %s", path)
                    return None
                continue

            if 500 <= response.status_code < 600:
                delay = self._backoff_delay(attempt)
                log.warning(
                    "HTTP %d on %s - attempt %d/%d, retrying in %.1fs",
                    response.status_code, path, attempt, self.max_attempts, delay,
                )
                time.sleep(delay)
                continue

            if response.status_code == 404:
                # Normal for a token whose pool is not indexed yet.
                log.debug("HTTP 404 on %s - no data available", path)
                return None

            if response.status_code >= 400:
                log.error("HTTP %d on %s - skipping this resource", response.status_code, path)
                return None

            try:
                payload = response.json()
            except ValueError:
                log.error("invalid JSON from %s - skipping this resource", path)
                return None

            self._rate_limit_streak = 0  # the host is answering again; start fresh next time
            return payload

        log.error("giving up on %s after %d attempts", path, self.max_attempts)
        return None

class DexScreenerSource(ApiClient):
    """Discovery through DexScreener: recent token profiles, then each token's pools."""

    name = "dexscreener"

    def __init__(self, *, max_tokens_per_scan: int = DEFAULT_MAX_TOKENS_PER_SCAN, **kwargs: Any) -> None:
        super().__init__(base_url=DEXSCREENER_BASE_URL, **kwargs)
        self.max_tokens_per_scan = max(1, int(max_tokens_per_scan))
        # DexScreener publishes two different budgets, so two limiters.
        self._profiles_limiter = self.limiter("token-profiles", PROFILES_PER_MINUTE)
        self._pairs_limiter = self.limiter("token-pairs", TOKEN_PAIRS_PER_MINUTE)

    def discover(self, store: "Store") -> list[NewPair] | None:
        """New pools belonging to tokens that recently entered the profile feed."""
        profiles = self.latest_solana_token_profiles()
        if profiles is None:
            return None

        known = store.known_token_addresses()
        candidates = [str(entry["tokenAddress"]) for entry in profiles if str(entry["tokenAddress"]) not in known]
        if len(candidates) > self.max_tokens_per_scan:
            log.debug("capping this pass at %d of %d candidate(s)", self.max_tokens_per_scan, len(candidates))
            candidates = candidates[: self.max_tokens_per_scan]
        log.debug("candidates: %d new token(s) out of %d solana profile(s)", len(candidates), len(profiles))

        pairs: list[NewPair] = []
        for token_address in candidates:
            pools = self.token_pairs(token_address)
            if pools is None:
                # Leave the token unmarked so the next pass retries it.
                continue
            first_seen = isoformat(utc_now())
            store.mark_token_checked(token_address, first_seen)
            for payload in pools:
                pair = NewPair.from_dexscreener(payload, first_seen=first_seen)
                if pair is not None:
                    pairs.append(pair)
        return pairs

    def latest_solana_token_profiles(self) -> list[dict[str, Any]] | None:
        """Solana entries from the *latest token profiles* feed.

        Returns ``None`` when the feed itself could not be fetched, which the
        scanner treats as "nothing to do this cycle" rather than an empty feed.
        """
        payload = self._get_json("/token-profiles/latest/v1", limiter=self._profiles_limiter)
        if payload is None:
            return None
        if not isinstance(payload, list):
            log.error("unexpected token-profiles payload (%s) - ignoring", type(payload).__name__)
            return None

        profiles: list[dict[str, Any]] = []
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("chainId") or "").lower() != CHAIN_ID:
                continue
            token_address = entry.get("tokenAddress")
            if not isinstance(token_address, str) or not token_address:
                continue
            profiles.append(entry)
        log.debug("token-profiles: %d entries, %d on %s", len(payload), len(profiles), CHAIN_ID)
        return profiles

    def token_pairs(self, token_address: str) -> list[dict[str, Any]] | None:
        """Every pool DexScreener knows for ``token_address`` (None on failure)."""
        payload = self._get_json(f"/token-pairs/v1/{CHAIN_ID}/{token_address}", limiter=self._pairs_limiter)
        if payload is None:
            return None
        if not isinstance(payload, list):
            log.error(
                "unexpected token-pairs payload for %s (%s) - ignoring",
                token_address, type(payload).__name__,
            )
            return None
        return [entry for entry in payload if isinstance(entry, dict)]


class GeckoTerminalClient(ApiClient):
    """GeckoTerminal's public API: the new-pools feed and batched pool lookups.

    Both endpoints share one limiter because GeckoTerminal's 30 calls-per-minute
    budget is account-wide rather than per endpoint.
    """

    name = "geckoterminal"

    def __init__(self, *, network: str = CHAIN_ID, **kwargs: Any) -> None:
        super().__init__(base_url=GECKOTERMINAL_BASE_URL, **kwargs)
        self.network = network
        self._api_limiter = self.limiter("geckoterminal", GECKOTERMINAL_CALLS_PER_MINUTE)

    def new_pools(self, page: int = 1) -> list[dict[str, Any]] | None:
        """One page of the newest pools, newest first (None when it failed)."""
        payload = self._get_json(
            f"/networks/{self.network}/new_pools",
            limiter=self._api_limiter,
            params={"page": page},
        )
        if payload is None:
            return None
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            log.error("unexpected new_pools payload on page %d (%s) - ignoring", page, type(payload).__name__)
            return None
        pools = [entry for entry in payload["data"] if isinstance(entry, dict)]
        log.debug("new_pools page %d: %d pool(s)", page, len(pools))
        return pools

    def pools_multi(self, addresses: Sequence[str]) -> MultiPoolResult:
        """Current data for ``addresses``, batched at 30 addresses per request.

        GeckoTerminal answers 200 and simply *omits* pools it does not know (a
        batch of unknown addresses returns ``{"data": []}``), so an address that
        is missing from the response is a dead pool.  Addresses belonging to a
        batch that could not be fetched are reported as ``unresolved`` instead,
        because nothing can be concluded about them.
        """
        wanted = list(dict.fromkeys(addresses))
        found: dict[str, dict[str, Any]] = {}
        unresolved: set[str] = set()

        for batch in chunked(wanted, SNAPSHOT_BATCH_SIZE):
            payload = self._get_json(
                f"/networks/{self.network}/pools/multi/{','.join(batch)}",
                limiter=self._api_limiter,
            )
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
                # Never guess: an unreadable batch says nothing about its pools.
                unresolved.update(batch)
                continue
            for entry in payload["data"]:
                if not isinstance(entry, dict):
                    continue
                address = resource_address(entry)
                if address:
                    found[address] = entry
            log.debug("pools/multi: batch of %d address(es), %d pool(s) known so far", len(batch), len(found))

        return MultiPoolResult(pools=found, unresolved=frozenset(unresolved))

class GeckoTerminalSource(GeckoTerminalClient):
    """Discovery through the ``/networks/{network}/new_pools`` feed.

    The feed already carries the metrics, so no follow-up request is needed per
    pool.  The ``pairs`` primary key deduplicates inside SQLite, so the
    ``tokens`` ledger is not used by this source.
    """

    name = "geckoterminal"

    def __init__(self, *, pages: int = DEFAULT_PAGES, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.pages = max(1, int(pages))

    def discover(self, store: "Store") -> list[NewPair] | None:
        """The pool objects of the newest pages, ready to be stored."""
        pairs: list[NewPair] = []
        for page in range(1, self.pages + 1):
            raw_pools = self.new_pools(page)
            if raw_pools is None:
                if page == 1:
                    return None
                log.warning("page %d unavailable - keeping the %d pool(s) read so far", page, len(pairs))
                break
            first_seen = isoformat(utc_now())
            for payload in raw_pools:
                pair = NewPair.from_geckoterminal(payload, first_seen=first_seen)
                if pair is not None:
                    pairs.append(pair)
        log.debug("new_pools: %d pool(s) collected", len(pairs))
        return pairs


class Snapshotter:
    """Records milestone snapshots for the pairs already in the ledger.

    Every pair gets at most one row per label (``5m``, ``15m``, ``1h``, ``4h``),
    written the first time its age passes that label, using GeckoTerminal's
    batched ``pools/multi`` endpoint (30 addresses per request).  A pool that
    GeckoTerminal no longer returns is recorded as ``dead`` with NULL metrics,
    while a pool whose batch could not be fetched is simply left for the next
    pass.
    """

    def __init__(
        self,
        client: GeckoTerminalClient,
        store: "Store",
        *,
        labels: Sequence[tuple[str, int]] = SNAPSHOT_LABELS,
        grace_minutes: float = DEFAULT_SNAPSHOT_GRACE_MINUTES,
    ) -> None:
        self.client = client
        self.store = store
        self.labels = tuple(labels)
        self.grace = timedelta(minutes=max(0.0, float(grace_minutes)))

    def run_once(self) -> int:
        """Take every milestone that is currently due; returns rows written."""
        now = utc_now()
        pending = self.store.pending_snapshots(self.labels, now, self.grace)
        if not pending:
            log.debug("snapshots: nothing due")
            return 0

        by_label: dict[str, list[str]] = {}
        for address, label in pending:
            by_label.setdefault(label, []).append(address)

        taken_at = isoformat(now)
        written = 0
        for label, addresses in by_label.items():
            unique = list(dict.fromkeys(addresses))
            for batch in chunked(unique, SNAPSHOT_BATCH_SIZE):
                written += self._record(self.client.pools_multi(batch), batch, label, taken_at)
            log.debug("snapshots: label %s -> %d row(s) from %d pair(s)", label, written, len(unique))
        log.info("snapshots: %d row(s) written for %d label(s)", written, len(by_label))
        return written

    def _record(self, result: MultiPoolResult, batch: Sequence[str], label: str, taken_at: str) -> int:
        """Write one row per address in ``batch``, skipping unresolved ones."""
        written = 0
        for address in batch:
            if address in result.unresolved:
                log.debug("snapshots: %s/%s unresolved - retrying next pass", address, label)
                continue
            payload = result.pools.get(address)
            metrics = None if payload is None else PoolMetrics.from_geckoterminal(payload)
            status = SNAPSHOT_DEAD if payload is None else SNAPSHOT_ALIVE
            if self.store.insert_snapshot(address, label, taken_at, metrics, status):
                written += 1
                log.debug("snapshots: %s/%s recorded as %s", address, label, status)
        return written


@dataclass(frozen=True)
class NewPair:
    """A pair the scanner had not seen before, as fetched from DexScreener."""

    pair_address: str
    token: str
    first_seen: str
    liquidity_usd: float | None
    market_cap: float | None
    volume_5m: float | None
    token_address: str | None = None
    pair_created_at: str | None = None
    dex_id: str | None = None

    @classmethod
    def from_dexscreener(cls, payload: dict[str, Any], first_seen: str) -> "NewPair | None":
        """Build an entry from one DexScreener pair object (None if unusable)."""
        pair_address = payload.get("pairAddress")
        if not isinstance(pair_address, str) or not pair_address:
            return None

        raw_base = payload.get("baseToken")
        base_token: dict[str, Any] = raw_base if isinstance(raw_base, dict) else {}
        raw_liquidity = payload.get("liquidity")
        liquidity: dict[str, Any] = raw_liquidity if isinstance(raw_liquidity, dict) else {}
        raw_volume = payload.get("volume")
        volume: dict[str, Any] = raw_volume if isinstance(raw_volume, dict) else {}

        # marketCap can be null for brand new pools; fdv is the closest fallback.
        market_cap = to_float(payload.get("marketCap"))
        if market_cap is None:
            market_cap = to_float(payload.get("fdv"))

        symbol = str(base_token.get("symbol") or base_token.get("name") or "").strip()

        return cls(
            pair_address=pair_address,
            token=symbol or "?",
            first_seen=first_seen,
            liquidity_usd=to_float(liquidity.get("usd")),
            market_cap=market_cap,
            volume_5m=to_float(volume.get("m5")),
            token_address=str(base_token.get("address") or "").strip() or None,
            pair_created_at=iso_from_millis(payload.get("pairCreatedAt")),
            dex_id=str(payload.get("dexId") or "").strip() or None,
        )

    @classmethod
    def from_geckoterminal(cls, payload: dict[str, Any], first_seen: str) -> "NewPair | None":
        """Build an entry from one GeckoTerminal pool resource (None if unusable).

        Field mapping: ``liquidity_usd`` <- ``reserve_in_usd``,
        ``market_cap`` <- ``market_cap_usd`` (falling back to ``fdv_usd``),
        ``volume_5m`` <- ``volume_usd.m5`` and ``pair_created_at`` <-
        ``pool_created_at``.
        """
        attributes = payload.get("attributes")
        if not isinstance(attributes, dict):
            return None

        address = resource_address(payload)
        if not address:
            return None

        # GeckoTerminal does not inline token symbols, but the pool name is
        # "<base symbol> / <quote symbol>".
        symbol = symbol_from_pool_name(attributes.get("name"))
        metrics = PoolMetrics.from_geckoterminal(payload)

        return cls(
            pair_address=address,
            token=symbol or "?",
            first_seen=first_seen,
            liquidity_usd=metrics.liquidity_usd,
            market_cap=metrics.market_cap,
            volume_5m=metrics.volume_5m,
            token_address=strip_chain_prefix(relationship_id(payload, "base_token")),
            pair_created_at=normalize_isoformat(attributes.get("pool_created_at")),
            dex_id=relationship_id(payload, "dex"),
        )

    @property
    def created_at(self) -> datetime | None:
        """``pair_created_at`` as a datetime, or None when unknown."""
        return parse_isoformat(self.pair_created_at)

    def age_minutes(self) -> float | None:
        """How old the pool was, in minutes, when the scanner first saw it."""
        created = self.created_at
        seen = parse_isoformat(self.first_seen)
        if created is None or seen is None:
            return None
        return (seen - created).total_seconds() / 60.0

    def describe(self) -> str:
        """Single-line console summary, including the pool's age when first seen."""
        ticker = self.token if self.token.startswith("$") else f"${self.token}"
        return (
            f"[NEW] {ticker}  dex={self.dex_id or 'unknown'}  age={fmt_age(self.age_minutes())}  "
            f"liq={fmt_money(self.liquidity_usd)}  mcap={fmt_money(self.market_cap)}  "
            f"vol5m={fmt_money(self.volume_5m)}  pair={self.pair_address}  "
            f"created={self.pair_created_at or 'unknown'}  first_seen={self.first_seen}"
        )


@dataclass(frozen=True)
class PoolMetrics:
    """Current numbers for a pool, as returned by GeckoTerminal.

    Used for the milestone snapshots and reused by :meth:`NewPair.from_geckoterminal`
    so the field mapping lives in exactly one place.
    """

    price_usd: float | None = None
    liquidity_usd: float | None = None
    market_cap: float | None = None
    volume_5m: float | None = None

    @classmethod
    def from_geckoterminal(cls, payload: dict[str, Any]) -> "PoolMetrics":
        """Read price, liquidity, market cap and 5m volume out of a pool resource."""
        attributes = payload.get("attributes")
        attributes = attributes if isinstance(attributes, dict) else {}
        raw_volume = attributes.get("volume_usd")
        volume: dict[str, Any] = raw_volume if isinstance(raw_volume, dict) else {}

        # marketCap is null for many fresh pools; fdv is the closest fallback.
        market_cap = to_float(attributes.get("market_cap_usd"))
        if market_cap is None:
            market_cap = to_float(attributes.get("fdv_usd"))

        return cls(
            price_usd=to_float(attributes.get("base_token_price_usd")),
            liquidity_usd=to_float(attributes.get("reserve_in_usd")),
            market_cap=market_cap,
            volume_5m=to_float(volume.get("m5")),
        )


@dataclass(frozen=True)
class MultiPoolResult:
    """Outcome of a batched ``pools/multi`` lookup.

    ``unresolved`` holds the addresses whose batch could not be fetched; they are
    *unknown*, not dead, so the caller must retry them instead of recording a
    death.
    """

    pools: dict[str, dict[str, Any]]
    unresolved: frozenset[str] = frozenset()

    def is_dead(self, address: str) -> bool:
        """True when GeckoTerminal answered for ``address`` but did not return it."""
        return address not in self.pools and address not in self.unresolved


SCHEMA = """
CREATE TABLE IF NOT EXISTS pairs (
    pair_address    TEXT PRIMARY KEY,
    token           TEXT NOT NULL,
    first_seen      TEXT NOT NULL,
    liquidity_usd   REAL,
    market_cap      REAL,
    volume_5m       REAL,
    token_address   TEXT,
    pair_created_at TEXT,
    dex_id          TEXT
);

CREATE INDEX IF NOT EXISTS idx_pairs_first_seen    ON pairs (first_seen DESC);
CREATE INDEX IF NOT EXISTS idx_pairs_token_address ON pairs (token_address);

CREATE TABLE IF NOT EXISTS tokens (
    token_address TEXT PRIMARY KEY,
    first_seen    TEXT NOT NULL,
    checks        INTEGER NOT NULL DEFAULT 1
);

-- One milestone row per pair and label; status flips to 'dead' when the pool
-- disappears from GeckoTerminal before the snapshot is taken.
CREATE TABLE IF NOT EXISTS snapshots (
    pair_address  TEXT NOT NULL,
    label         TEXT NOT NULL,
    taken_at      TEXT NOT NULL,
    price_usd     REAL,
    liquidity_usd REAL,
    market_cap    REAL,
    volume_5m     REAL,
    status        TEXT NOT NULL DEFAULT 'alive',
    PRIMARY KEY (pair_address, label)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_label ON snapshots (label, status);
"""


class Store:
    """SQLite ledger of the tokens examined and the pairs recorded so far."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), timeout=5.0)
        self._conn.row_factory = sqlite3.Row
        # WAL keeps reads possible while the scanner writes; a busy timeout
        # absorbs the rare contention with a second process.
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    # -- tokens ------------------------------------------------------------

    def known_token_addresses(self) -> set[str]:
        """Token addresses already examined (with or without a stored pair)."""
        rows = self._conn.execute("SELECT token_address FROM tokens")
        return {str(row["token_address"]) for row in rows}

    def mark_token_checked(self, token_address: str, checked_at: str) -> None:
        """Remember that this token was examined; bump its counter if it was seen before."""
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO tokens (token_address, first_seen, checks) VALUES (?, ?, 1)
                ON CONFLICT(token_address) DO UPDATE SET checks = checks + 1
                """,
                (token_address, checked_at),
            )

    # -- pairs -------------------------------------------------------------

    def insert_pair(self, pair: NewPair) -> bool:
        """Store ``pair``; returns False when that pair address was already known."""
        with self._conn:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO pairs (
                    pair_address, token, first_seen, liquidity_usd,
                    market_cap, volume_5m, token_address, pair_created_at, dex_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pair.pair_address,
                    pair.token,
                    pair.first_seen,
                    pair.liquidity_usd,
                    pair.market_cap,
                    pair.volume_5m,
                    pair.token_address,
                    pair.pair_created_at,
                    pair.dex_id,
                ),
            )
        return cursor.rowcount > 0

    def counts(self) -> tuple[int, int]:
        """``(pairs, tokens)`` currently stored."""
        pairs = self._conn.execute("SELECT COUNT(*) AS n FROM pairs").fetchone()["n"]
        tokens = self._conn.execute("SELECT COUNT(*) AS n FROM tokens").fetchone()["n"]
        return int(pairs), int(tokens)

    # -- snapshots ---------------------------------------------------------

    def pending_snapshots(
        self,
        labels: Sequence[tuple[str, int]],
        now: datetime,
        grace: timedelta,
    ) -> list[tuple[str, str]]:
        """``(pair_address, label)`` pairs whose milestone has just come due.

        A label becomes due once the pool's age passes it and stays due for
        ``grace``; anything discovered later than that is skipped rather than
        backfilled with mislabelled data.  Labels that already have a row are
        never returned again.
        """
        if not labels:
            return []

        oldest = isoformat(now - timedelta(seconds=max(seconds for _, seconds in labels) + grace.total_seconds()))
        newest = isoformat(now - timedelta(seconds=min(seconds for _, seconds in labels)))
        rows = self._conn.execute(
            """
            SELECT pair_address, pair_created_at FROM pairs
            WHERE pair_created_at IS NOT NULL AND pair_created_at >= ? AND pair_created_at <= ?
            """,
            (oldest, newest),
        ).fetchall()
        if not rows:
            return []

        # One row per pair and label, so this set stays small enough to load.
        recorded = {
            (str(row["pair_address"]), str(row["label"]))
            for row in self._conn.execute("SELECT pair_address, label FROM snapshots")
        }

        pending: list[tuple[str, str]] = []
        for row in rows:
            created = parse_isoformat(row["pair_created_at"])
            if created is None:
                continue
            address = str(row["pair_address"])
            for label, seconds in labels:
                due_at = created + timedelta(seconds=seconds)
                if due_at <= now <= due_at + grace and (address, label) not in recorded:
                    pending.append((address, label))
        return pending

    def insert_snapshot(
        self,
        pair_address: str,
        label: str,
        taken_at: str,
        metrics: PoolMetrics | None,
        status: str = SNAPSHOT_ALIVE,
    ) -> bool:
        """Record one milestone row; False when that (pair, label) already exists.

        ``metrics`` is None for a dead pool, which stores NULLs for its numbers.
        """
        with self._conn:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO snapshots (
                    pair_address, label, taken_at, price_usd, liquidity_usd, market_cap, volume_5m, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pair_address,
                    label,
                    taken_at,
                    None if metrics is None else metrics.price_usd,
                    None if metrics is None else metrics.liquidity_usd,
                    None if metrics is None else metrics.market_cap,
                    None if metrics is None else metrics.volume_5m,
                    status,
                ),
            )
        return cursor.rowcount > 0

    def snapshot_rows(self) -> list[sqlite3.Row]:
        """Every snapshot joined with the first-seen baseline of its pair."""
        return self._conn.execute(
            """
            SELECT s.pair_address, s.label, s.taken_at, s.status,
                   s.price_usd, s.liquidity_usd, s.market_cap AS snapshot_market_cap,
                   s.volume_5m,
                   p.token, p.market_cap AS first_market_cap,
                   p.liquidity_usd AS first_liquidity_usd, p.first_seen, p.pair_created_at
            FROM snapshots AS s
            LEFT JOIN pairs AS p ON p.pair_address = s.pair_address
            ORDER BY s.label, s.taken_at
            """
        ).fetchall()

    def snapshot_count(self) -> int:
        """How many snapshot rows are stored."""
        return int(self._conn.execute("SELECT COUNT(*) AS n FROM snapshots").fetchone()["n"])


class Scanner:
    """Ties a discovery source and the SQLite ledger together."""

    def __init__(
        self,
        source: ApiClient,
        store: Store,
        *,
        interval: float = DEFAULT_INTERVAL,
        max_pair_age_hours: float = DEFAULT_MAX_PAIR_AGE_HOURS,
        min_liquidity: float = 0.0,
        snapshotter: "Snapshotter | None" = None,
    ) -> None:
        self.source = source
        self.store = store
        self.snapshotter = snapshotter
        self.interval = max(1.0, float(interval))
        self.max_pair_age = timedelta(hours=float(max_pair_age_hours)) if max_pair_age_hours > 0 else None
        self.min_liquidity = max(0.0, float(min_liquidity))

    def scan_once(self) -> int | None:
        """Run one discovery pass.

        Returns the number of new pools stored, or ``None`` when the source was
        unavailable - in that case nothing is recorded and the next pass simply
        asks the source again.
        """
        pairs = self.source.discover(self.store)
        if pairs is None:
            log.warning("%s feed unavailable - retrying next pass", self.source.name)
            return None

        discovered = 0
        for pair in pairs:
            if not self._wanted(pair):
                continue
            if self.store.insert_pair(pair):
                discovered += 1
                print(pair.describe(), flush=True)

        log.debug("%s offered %d pool(s), %d were new", self.source.name, len(pairs), discovered)
        self.take_snapshots()
        return discovered

    def take_snapshots(self) -> int:
        """Record the milestones that are due; a no-op without a snapshotter.

        Snapshots run after discovery so a slow (rate-limited) snapshot pass can
        never delay seeing brand new pools.
        """
        if self.snapshotter is None:
            return 0
        try:
            return self.snapshotter.run_once()
        except Exception:  # a snapshot hiccup must not lose the discovery pass
            log.exception("snapshot pass failed - continuing")
            return 0

    def _wanted(self, pair: NewPair) -> bool:
        """Apply the liquidity and pool-age filters."""
        if self.min_liquidity > 0 and (pair.liquidity_usd or 0.0) < self.min_liquidity:
            return False
        if self.max_pair_age is not None:
            created = pair.created_at
            if created is not None and utc_now() - created > self.max_pair_age:
                return False
        return True

    def run(self, stop_event: threading.Event) -> None:
        """Poll until ``stop_event`` is set, keeping a steady cadence."""
        log.info(
            "polling %s via %s every %.0fs (db=%s, snapshots=%s)",
            CHAIN_ID,
            self.source.name,
            self.interval,
            self.store.path,
            "on" if self.snapshotter else "off",
        )
        while not stop_event.is_set():
            started_at = time.monotonic()
            try:
                discovered = self.scan_once()
            except Exception:  # a bad pass must never kill the loop
                log.exception("unexpected error during a pass - continuing")
            else:
                if discovered is not None:
                    log.info("pass complete: %d new pair(s)", discovered)
            stop_event.wait(max(0.0, self.interval - (time.monotonic() - started_at)))
        log.info("stopped")


def build_parser() -> argparse.ArgumentParser:
    """Command line interface of the scanner."""
    parser = argparse.ArgumentParser(
        prog="scanner.py",
        description="Poll a public DEX API for newly listed Solana pools and store them in SQLite.",
    )
    parser.add_argument("--source", default=DEFAULT_SOURCE, choices=SOURCES, help="discovery source (default: %(default)s)")
    parser.add_argument("--db", default=DEFAULT_DB, metavar="PATH", help="SQLite file (default: %(default)s)")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL, metavar="SECONDS", help="seconds between passes (default: %(default)s)")
    parser.add_argument("--once", action="store_true", help="run a single pass and exit")
    parser.add_argument("--pages", type=int, default=DEFAULT_PAGES, metavar="N", help="geckoterminal: new_pools pages per pass, 20 pools each (default: %(default)s)")
    parser.add_argument("--min-liquidity", type=float, default=0.0, metavar="USD", help="ignore pools below this liquidity (default: %(default)s)")
    parser.add_argument("--max-pair-age-hours", type=float, default=DEFAULT_MAX_PAIR_AGE_HOURS, metavar="HOURS", help="ignore pools older than this; 0 disables (default: %(default)s)")
    parser.add_argument("--max-tokens-per-scan", type=int, default=DEFAULT_MAX_TOKENS_PER_SCAN, metavar="N", help="dexscreener: token lookups per pass (default: %(default)s)")
    parser.add_argument("--snapshots", action=argparse.BooleanOptionalAction, default=True, help="record the 5m/15m/1h/4h milestone snapshots (default: %(default)s)")
    parser.add_argument("--snapshot-grace-minutes", type=float, default=DEFAULT_SNAPSHOT_GRACE_MINUTES, metavar="MINUTES", help="drop a milestone if it is only noticed this long after it was due (default: %(default)s)")
    parser.add_argument("--timeout", type=float, default=10.0, metavar="SECONDS", help="HTTP timeout (default: %(default)s)")
    parser.add_argument("--max-attempts", type=int, default=4, metavar="N", help="attempts per request (default: %(default)s)")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"), help="console verbosity (default: %(default)s)")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def install_signal_handlers(stop_event: threading.Event) -> None:
    """Turn Ctrl+C / SIGTERM into a graceful stop after the current pass."""

    def request_stop(signum: int, _frame: object) -> None:
        try:
            name = signal.Signals(signum).name
        except ValueError:
            name = f"signal {signum}"
        log.info("received %s - finishing the current pass", name)
        stop_event.set()

    for attribute in ("SIGINT", "SIGTERM"):
        handled = getattr(signal, attribute, None)
        if handled is None:
            continue
        try:
            signal.signal(handled, request_stop)
        except (ValueError, OSError, RuntimeError):
            log.debug("could not install the %s handler", attribute)


def make_source(args: argparse.Namespace) -> ApiClient:
    """Build the discovery source selected with ``--source``."""
    common: dict[str, Any] = {"timeout": args.timeout, "max_attempts": args.max_attempts}
    if args.source == "dexscreener":
        return DexScreenerSource(max_tokens_per_scan=args.max_tokens_per_scan, **common)
    return GeckoTerminalSource(pages=args.pages, **common)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments, wire the objects together and run the scanner."""
    args = build_parser().parse_args(argv)
    setup_logging(args.log_level)
    log.info("meme-radar %s starting (source=%s)", __version__, args.source)

    stop_event = threading.Event()
    install_signal_handlers(stop_event)

    store = Store(args.db)
    source = make_source(args)
    source.stop_event = stop_event  # lets a long rate-limit cooldown end on shutdown
    snapshot_client: ApiClient | None = None
    try:
        snapshotter: Snapshotter | None = None
        if args.snapshots:
            if isinstance(source, GeckoTerminalClient):
                # Share the client (and its rate-limit budget) with discovery.
                snapshot_client = source
            else:
                snapshot_client = GeckoTerminalClient(timeout=args.timeout, max_attempts=args.max_attempts)
                snapshot_client.stop_event = stop_event
            snapshotter = Snapshotter(snapshot_client, store, grace_minutes=args.snapshot_grace_minutes)

        pairs, tokens = store.counts()
        log.info("ledger %s already holds %d pair(s) from %d token(s)", store.path, pairs, tokens)
        if snapshotter is not None:
            log.info(
                "snapshots enabled: %s (grace %.0f min, %d addresses per request)",
                "/".join(label for label, _ in snapshotter.labels),
                args.snapshot_grace_minutes,
                SNAPSHOT_BATCH_SIZE,
            )

        scanner = Scanner(
            source,
            store,
            interval=args.interval,
            max_pair_age_hours=args.max_pair_age_hours,
            min_liquidity=args.min_liquidity,
            snapshotter=snapshotter,
        )
        if args.once:
            discovered = scanner.scan_once()
            if discovered is None:
                log.error("single pass failed: the %s feed was unavailable", source.name)
                return 1
            log.info("single pass complete: %d new pair(s)", discovered)
            return 0
        scanner.run(stop_event)
    except KeyboardInterrupt:
        log.info("interrupted - shutting down")
        return 130
    finally:
        if snapshot_client is not None and snapshot_client is not source:
            snapshot_client.close()
        source.close()
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())







