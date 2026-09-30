#!/usr/bin/env python3
"""Verification of scanner.py's HTTP 429 backoff (companion to the audit).

Everything runs on a *virtual* clock: ``scanner.time`` and ``ApiClient._sleep``
are mocked, so no test ever really waits, while the intended sleep durations
are recorded and printed exactly as the production code requests them.

Run:  python test_429_backoff.py        (or: pytest test_429_backoff.py)

Checks, one per audit question:
  1. a 429 without Retry-After backs off 20s, 40s, 80s, 120s on attempts 1-4
  2. Retry-After is honored (seconds + HTTP-date) and clamped to [1s, 120s]
  3. a 429 on new_pools delays the following pools/multi call
     (plus: a live cooldown left by an interrupted sleep parks the next call)
  4. a pass whose retries fail ends cleanly, keeps stored data, and leaves
     unresolved snapshot addresses unresolved (never recorded as 'dead')
  5. one successful response resets the escalation back to 20s
  6. the client-side 30 calls/min limiter still paces every request
"""
from __future__ import annotations

import logging
import sys
import threading
import time as _real_time
import traceback
from collections import deque
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable

import httpx

import scanner as sc

# Endpoint markers used to route scripted responses.
RATE = "new_pools"
MULTI = "pools/multi"

# Quiet scanner's own warnings so the recorded evidence is readable.
logging.getLogger("meme_radar").setLevel(logging.CRITICAL)


# --------------------------------------------------------------------------
# virtual clock
# --------------------------------------------------------------------------
class VirtualClock:
    """monotonic() + sleep() replacement; records who slept and how long."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[tuple[str, float]] = []  # (caller function, seconds)
        self.requests: list[tuple[str, float]] = []  # (url path, virtual time)

    def slept(self, who: str | None = None) -> list[float]:
        """Recorded sleep durations, optionally filtered by caller name."""
        return [s for w, s in self.sleeps if who is None or w == who]

    def times_for(self, needle: str) -> list[float]:
        """Virtual timestamps of every request whose path contains ``needle``."""
        return [t for path, t in self.requests if needle in path]

    def stamp(self, needle: str) -> float:
        """Virtual timestamp of the first request containing ``needle``."""
        hits = self.times_for(needle)
        if not hits:
            raise AssertionError(f"no request recorded for {needle!r}")
        return hits[0]


class FakeTimeModule:
    """Stands in for the ``time`` module inside ``scanner`` only."""

    def __init__(self, clock: VirtualClock) -> None:
        self._clock = clock

    def monotonic(self) -> float:
        return self._clock.now

    def sleep(self, seconds: float) -> None:
        caller = sys._getframe(1).f_code.co_name
        self._clock.sleeps.append((caller, float(seconds)))
        self._clock.now += float(seconds)

    def __getattr__(self, name: str) -> Any:
        return getattr(_real_time, name)


class VirtualTime:
    """Context manager: virtual ``scanner.time`` + mocked ``ApiClient._sleep``."""

    def __init__(self) -> None:
        self.clock = VirtualClock()
        self._saved_time: Any = None
        self._saved_sleep: Any = None

    def __enter__(self) -> VirtualTime:
        clock = self.clock
        self._saved_time = sc.time
        self._saved_sleep = sc.ApiClient._sleep
        sc.time = FakeTimeModule(clock)

        def fake_sleep(_self: Any, seconds: float) -> bool:
            """Mock of ApiClient._sleep: record the *requested* duration.

            Mirrors the real contract: returns True when stop was requested
            (without letting the virtual clock advance), False otherwise.
            """
            caller = sys._getframe(1).f_code.co_name
            seconds = float(seconds)
            clock.sleeps.append((caller, seconds))
            if _self.stop_event is not None and _self.stop_event.is_set():
                return True
            clock.now += seconds
            return False

        sc.ApiClient._sleep = fake_sleep
        return self

    def __exit__(self, *_exc: object) -> None:
        sc.ApiClient._sleep = self._saved_sleep
        sc.time = self._saved_time


# --------------------------------------------------------------------------
# scripted transport
# --------------------------------------------------------------------------
def rl(retry_after: str | None = None) -> tuple[int, dict[str, str] | None, Any]:
    """A bare 429, optionally carrying a Retry-After header."""
    headers = {"Retry-After": retry_after} if retry_after is not None else None
    return (429, headers, None)


OK = (200, None, {"data": []})


class Script:
    """Routes requests to queued canned responses; records request times."""

    def __init__(self, clock: VirtualClock) -> None:
        self.clock = clock
        self._routes: dict[str, deque] = {}

    def queue(self, endpoint: str, *responses: tuple) -> Script:
        self._routes.setdefault(endpoint, deque(responses))
        return self

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.clock.requests.append((path, self.clock.now))
        for endpoint, queue in self._routes.items():
            if endpoint in path:
                status, headers, payload = queue.popleft() if queue else OK
                kwargs: dict[str, Any] = {"headers": headers} if headers else {}
                if payload is not None:
                    return httpx.Response(status, json=payload, **kwargs)
                return httpx.Response(status, **kwargs)
        return httpx.Response(200, json={"data": []})


def make_source(clock: VirtualClock, script: Script, **kwargs: Any) -> sc.GeckoTerminalSource:
    """GeckoTerminalSource whose HTTP client answers from ``script``."""
    source = sc.GeckoTerminalSource(max_attempts=kwargs.pop("max_attempts", 4), **kwargs)
    source._client.close()
    source._client = httpx.Client(
        base_url=sc.GECKOTERMINAL_BASE_URL,
        transport=httpx.MockTransport(script),
    )
    return source


def fmt(values: list[float]) -> str:
    return ", ".join(f"{v:g}s" for v in values)


def gaps(times: list[float]) -> list[float]:
    return [b - a for a, b in zip(times, times[1:])]


def assert_spacing(times: list[float], minimum: float = 2.0) -> list[float]:
    """Every consecutive request must be >= `minimum` apart (30 calls/min)."""
    result = gaps(times)
    for gap in result:
        assert gap >= minimum - 1e-9, f"requests only {gap}s apart (< {minimum}s)"
    return result


# --------------------------------------------------------------------------
# 1. no Retry-After -> 20 / 40 / 80 / 120
# --------------------------------------------------------------------------
def test_backoff_doubles_20_40_80_120() -> None:
    with VirtualTime() as vt:
        clock = vt.clock
        script = Script(clock).queue(RATE, rl(), rl(), rl(), rl())
        source = make_source(clock, script)

        assert source.new_pools(1) is None  # all four attempts rate limited

        waits = clock.slept("_get_json")
        times = clock.times_for(RATE)
        print(f"[1] attempts 1-4 without Retry-After: slept {fmt(waits)}")
        print(f"    requests at t={fmt(times)}")
        assert waits == [20.0, 40.0, 80.0, 120.0], f"unexpected delays: {waits}"
        assert len(times) == 4, f"expected 4 requests, saw {len(times)}"
        # every retry must have waited out the full cooldown before firing again
        assert gaps(times)[:3] == [20.0, 40.0, 80.0], gaps(times)
        assert_spacing(times)


# --------------------------------------------------------------------------
# 2. Retry-After honored (seconds / HTTP-date), clamped to [1, 120]
# --------------------------------------------------------------------------
def test_retry_after_honored() -> None:
    # a) plain seconds, replaces the doubling entirely
    with VirtualTime() as vt:
        script = Script(vt.clock).queue(RATE, rl("7"), OK)
        make_source(vt.clock, script).new_pools(1)
        waits = vt.clock.slept("_get_json")
        print(f"[2] Retry-After: 7      -> slept {fmt(waits)} (not 20s)")
        assert waits == [7.0], waits

    # b) server asking for more than the cap -> clamped to 120s
    with VirtualTime() as vt:
        script = Script(vt.clock).queue(RATE, rl("999"), OK)
        make_source(vt.clock, script).new_pools(1)
        waits = vt.clock.slept("_get_json")
        print(f"[2] Retry-After: 999    -> slept {fmt(waits)} (clamped to 120s)")
        assert waits == [120.0], waits

    # c) server asking for less than 1s -> raised to 1s
    with VirtualTime() as vt:
        script = Script(vt.clock).queue(RATE, rl("0"), OK)
        make_source(vt.clock, script).new_pools(1)
        waits = vt.clock.slept("_get_json")
        print(f"[2] Retry-After: 0      -> slept {fmt(waits)} (clamped to 1s)")
        assert waits == [1.0], waits

    # d) HTTP-date form
    with VirtualTime() as vt:
        when = sc.utc_now() + timedelta(seconds=45)
        header = when.strftime("%a, %d %b %Y %H:%M:%S GMT")
        script = Script(vt.clock).queue(RATE, rl(header), OK)
        make_source(vt.clock, script).new_pools(1)
        waits = vt.clock.slept("_get_json")
        print(f"[2] Retry-After: HTTP-date (+45s) -> slept {fmt(waits)}")
        assert len(waits) == 1 and 43.0 <= waits[0] <= 45.0, waits


# --------------------------------------------------------------------------
# 3. a 429 on new_pools delays the following pools/multi call
# --------------------------------------------------------------------------
def test_429_delays_following_pools_multi() -> None:
    # baseline: no rate limiting at all
    with VirtualTime() as base:
        script = Script(base.clock).queue(RATE, OK).queue(MULTI, OK)
        source = make_source(base.clock, script)
        source.new_pools(1)
        source.pools_multi(["AddrA", "AddrB"])
        baseline = base.clock.stamp(MULTI)
        assert base.clock.slept("_get_json") == []
        assert_spacing(base.clock.times_for(RATE) + [baseline])

    # the same two calls, but new_pools gets 429 three times before 200
    with VirtualTime() as vt:
        clock = vt.clock
        script = Script(clock).queue(RATE, rl(), rl(), rl(), OK).queue(MULTI, OK)
        source = make_source(clock, script)
        source.new_pools(1)
        waits = clock.slept("_get_json")
        source.pools_multi(["AddrA", "AddrB"])
        multi_at = clock.stamp(MULTI)
        cooldown_end = sum(waits)  # deadlines: 20s, 60s, 140s

        print(f"[3] new_pools 429x3 -> slept {fmt(waits)}")
        print(f"    baseline pools/multi at t={baseline:g}s, "
              f"after 429s at t={multi_at:g}s (cooldown ended t={cooldown_end:g}s)")
        assert waits == [20.0, 40.0, 80.0], waits
        assert multi_at >= cooldown_end, (
            f"pools/multi fired at t={multi_at} before cooldown end {cooldown_end}"
        )
        assert multi_at >= baseline + 100, (
            f"pools/multi only delayed by {multi_at - baseline}s"
        )
        assert_spacing(clock.times_for(RATE) + [multi_at])
        # Observation: in the normal flow the 429 branch itself slept out the
        # cooldown (scanner.py:441), so by the time the next call starts the
        # deadline has already passed and _await_cooldown has nothing left.
        assert clock.slept("_await_cooldown") == []


def test_live_cooldown_parks_next_call() -> None:
    """A 429 sleep cut short by stop_event leaves a live cooldown deadline.

    The next call must wait it out via _await_cooldown (scanner.py:412).
    """
    with VirtualTime() as vt:
        clock = vt.clock
        script = Script(clock).queue(RATE, rl()).queue(MULTI, OK)
        source = make_source(clock, script)
        source.stop_event = threading.Event()
        source.stop_event.set()

        assert source.new_pools(1) is None  # 429 -> stop seen -> abandons
        deadline = clock.now + 20.0  # cooldown was set but never slept

        source.stop_event.clear()
        assert clock.slept("_await_cooldown") == []
        source.pools_multi(["AddrA", "AddrB"])
        parked = clock.slept("_await_cooldown")
        multi_at = clock.stamp(MULTI)
        print(f"[3] interrupted 429 sleep -> next call parked for {fmt(parked)}")
        print(f"    pools/multi fired at t={multi_at:g}s (deadline t={deadline:g}s)")
        assert parked == [20.0], parked
        assert multi_at >= deadline


# --------------------------------------------------------------------------
# 4. failed pass is clean, keeps data, never marks unresolved as dead
# --------------------------------------------------------------------------
def test_failed_pass_is_clean_and_keeps_data() -> None:
    with VirtualTime() as vt, TemporaryDirectory() as tmp:
        clock = vt.clock
        db_path = Path(tmp) / "ledger.db"
        with sc.Store(str(db_path)) as store:
            pair = sc.NewPair(
                pair_address="Pair111",
                token="$TEST",
                first_seen="2026-09-29T00:00:00Z",
                liquidity_usd=1234.5,
                market_cap=5678.0,
                volume_5m=99.0,
                token_address="Token111",
                pair_created_at="2026-09-29T00:00:00Z",
                dex_id="raydium",
            )
            assert store.insert_pair(pair)
            before = store.counts()

            script = Script(clock).queue(RATE, rl(), rl(), rl(), rl())
            source = make_source(clock, script)
            scanner_ = sc.Scanner(source, store)
            result = scanner_.scan_once()

            print(f"[4] pass with 429x4 -> scan_once()={result!r}; "
                  f"store counts {before} -> {store.counts()}")
            assert result is None  # clean None, no exception raised
            assert store.counts() == before  # previously stored data untouched
            assert clock.slept("_get_json") == [20.0, 40.0, 80.0, 120.0]


def test_unresolved_snapshots_stay_unresolved() -> None:
    with VirtualTime() as vt, TemporaryDirectory() as tmp:
        clock = vt.clock
        script = Script(clock).queue(MULTI, rl(), rl(), rl(), rl())
        source = make_source(clock, script)
        batch = ["Addr1", "Addr2", "Addr3"]
        result = source.pools_multi(batch)

        with sc.Store(str(Path(tmp) / "ledger.db")) as store:
            written = sc.Snapshotter(source, store)._record(
                result, batch, "5m", "2026-09-29T00:00:00Z"
            )
            rows = store._conn.execute(
                "SELECT COUNT(*) AS n FROM snapshots"
            ).fetchone()["n"]

        print(f"[4] pools/multi 429x4 -> pools={dict(result.pools)}, "
              f"unresolved={sorted(result.unresolved)}, snapshot rows written={written}")
        assert dict(result.pools) == {}
        assert set(result.unresolved) == set(batch)
        assert written == 0  # nothing recorded: in particular no 'dead' rows
        assert rows == 0


# --------------------------------------------------------------------------
# 5. one success resets the escalation
# --------------------------------------------------------------------------
def test_streak_resets_after_success() -> None:
    with VirtualTime() as vt:
        script = Script(vt.clock).queue(RATE, rl(), rl(), OK, rl(), OK)
        source = make_source(vt.clock, script)
        source.new_pools(1)  # 429, 429, 200  -> 20s, 40s
        source.new_pools(1)  # 429, 200       -> must be 20s again, not 80s
        waits = vt.clock.slept("_get_json")
        print(f"[5] sleeps across two calls: {fmt(waits)}")
        assert waits == [20.0, 40.0, 20.0], waits


# --------------------------------------------------------------------------
# 6. the 30 calls/min limiter keeps pacing requests
# --------------------------------------------------------------------------
def test_limiter_keeps_30_calls_per_minute() -> None:
    with VirtualTime() as vt:
        clock = vt.clock
        script = Script(clock).queue(RATE, OK, OK, OK)
        source = make_source(clock, script)
        source.new_pools(1)
        source.new_pools(1)
        source.new_pools(1)
        times = clock.times_for(RATE)
        step = assert_spacing(times, 2.0)
        print(f"[6] three plain requests at t={fmt(times)}; gaps {fmt(step)}")
        assert all(abs(g - 2.0) < 1e-6 for g in step), step

    # after a 429 the penalized slot never comes early either
    with VirtualTime() as vt:
        clock = vt.clock
        script = Script(clock).queue(RATE, rl("10"), OK, OK)
        source = make_source(clock, script)
        source.new_pools(1)  # 429 + Retry-After: 10 -> sleep 10s
        source.new_pools(1)  # plain 200
        source.new_pools(1)  # plain 200
        times = clock.times_for(RATE)
        step = assert_spacing(times, 2.0)
        print(f"[6] after 429 (Retry-After: 10) requests at t={fmt(times)}; "
              f"gaps {fmt(step)}")
        assert clock.slept("_get_json") == [10.0]
        assert all(g >= 2.0 - 1e-9 for g in step), step


CHECKS: list[Callable[[], None]] = [
    test_backoff_doubles_20_40_80_120,
    test_retry_after_honored,
    test_429_delays_following_pools_multi,
    test_live_cooldown_parks_next_call,
    test_failed_pass_is_clean_and_keeps_data,
    test_unresolved_snapshots_stay_unresolved,
    test_streak_resets_after_success,
    test_limiter_keeps_30_calls_per_minute,
]


def main() -> int:
    failures = 0
    for check in CHECKS:
        try:
            check()
        except AssertionError as exc:
            failures += 1
            print(f"FAIL {check.__name__}: {exc}")
        except Exception:
            failures += 1
            print(f"ERROR {check.__name__}:")
            traceback.print_exc()
    print("-" * 60)
    print(f"{len(CHECKS) - failures}/{len(CHECKS)} checks passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())





