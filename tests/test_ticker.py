"""The ticker: the loop that turns a due schedule into a run.

Everything here runs the real :class:`Ticker` against a stand-in schedules API built from
``httpx.MockTransport``, so the code under test is the whole loop — request shapes, status
handling, the breaker — and only the network is fake.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from agent_schedules.config.settings import TickerSettings
from agent_schedules.ticker import Breaker, Ticker

NOW = datetime(2026, 3, 1, 9, 0, tzinfo=UTC)


def _schedule(schedule_id: str, next_fire_at: datetime | None = NOW) -> dict:
    return {
        "schedule_id": schedule_id,
        "name": schedule_id,
        "enabled": True,
        "next_fire_at": next_fire_at.isoformat() if next_fire_at else None,
    }


def _ticker(handler, **overrides) -> tuple[Ticker, TickerSettings]:
    config = TickerSettings(**{"interval_seconds": 0.01, **overrides})
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://schedules",
        headers={"X-API-Key": config.api_key},
    )
    return Ticker(http, config), config


async def test_a_due_schedule_is_fired_for_its_own_next_fire_at() -> None:
    """The instant matters: it is half the idempotency key, so a ticker that omitted it
    would let two replicas on one tick create two runs."""
    seen: list[tuple[str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/schedules/due":
            assert request.headers["X-API-Key"] == "dev-key"
            return httpx.Response(200, json=[_schedule("sch_1"), _schedule("sch_2")])
        seen.append((request.url.path, __import__("json").loads(request.content)))
        return httpx.Response(201, json={"run_id": "run_1", "created": True})

    ticker, _ = _ticker(handler)
    fired, skipped = await ticker.tick(now=NOW)

    assert (fired, skipped) == (2, 0)
    assert seen == [
        ("/v1/schedules/sch_1/fire", {"at": NOW.isoformat()}),
        ("/v1/schedules/sch_2/fire", {"at": NOW.isoformat()}),
    ]


async def test_nothing_due_fires_nothing() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/schedules/due", "must not fire when nothing is due"
        return httpx.Response(200, json=[])

    ticker, _ = _ticker(handler)
    assert await ticker.tick(now=NOW) == (0, 0)


@pytest.mark.parametrize(
    ("status", "why"),
    [
        (409, "raced a pause"),
        (422, "fire time the API refused"),
        (502, "the run could not be created"),
    ],
)
async def test_a_declined_fire_is_the_schedules_business_not_the_tickers(
    status: int, why: str
) -> None:
    """These three are already durable on the schedule row — the failure counter and the
    auto-pause. Retrying them here would double-count; counting them against the breaker
    would stop the ticker over one broken schedule."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/schedules/due":
            return httpx.Response(200, json=[_schedule("sch_bad")])
        return httpx.Response(status, json={"message": why})

    ticker, _ = _ticker(handler)
    assert await ticker.tick(now=NOW) == (0, 1)
    assert not ticker._breaker.is_open


async def test_the_breaker_opens_after_repeated_tick_failures_and_stops_calling() -> None:
    """A schedules API that is down must not become a tight retry loop against it."""
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("schedules API is down")

    ticker, config = _ticker(handler, breaker_threshold=3, breaker_cooldown_seconds=600)
    for _ in range(config.breaker_threshold):
        assert await ticker.tick(now=NOW) == (0, 0)
    assert calls == 3 and ticker._breaker.is_open

    # Open: further ticks inside the cooldown make no call at all.
    for _ in range(5):
        await ticker.tick(now=NOW + timedelta(seconds=1))
    assert calls == 3


async def test_the_breaker_admits_one_trial_after_cooldown_and_closes_on_success() -> None:
    state = {"up": False, "calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        if not state["up"]:
            raise httpx.ConnectError("still down")
        return httpx.Response(200, json=[])

    ticker, _ = _ticker(handler, breaker_threshold=2, breaker_cooldown_seconds=60)
    await ticker.tick(now=NOW)
    await ticker.tick(now=NOW)
    assert ticker._breaker.is_open and state["calls"] == 2

    state["up"] = True
    after_cooldown = NOW + timedelta(seconds=61)
    await ticker.tick(now=after_cooldown)

    assert state["calls"] == 3
    assert not ticker._breaker.is_open, "a successful trial must close the breaker"


async def test_a_failed_trial_restarts_the_cooldown_rather_than_calling_every_tick() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("down")

    ticker, _ = _ticker(handler, breaker_threshold=1, breaker_cooldown_seconds=60)
    await ticker.tick(now=NOW)
    assert calls == 1 and ticker._breaker.is_open

    await ticker.tick(now=NOW + timedelta(seconds=61))  # trial, fails
    assert calls == 2
    await ticker.tick(now=NOW + timedelta(seconds=70))  # inside the new cooldown
    assert calls == 2, "a failed trial must not admit a call on every subsequent tick"


async def test_a_crashing_tick_does_not_kill_the_loop() -> None:
    """The loop's job is to still be running at the next interval."""
    ticks = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal ticks
        ticks += 1
        if ticks == 1:
            raise RuntimeError("something unexpected inside a tick")
        if ticks >= 3:
            third_tick.set()
        return httpx.Response(200, json=[])

    ticker, _ = _ticker(handler)
    third_tick = asyncio.Event()

    stop = asyncio.Event()
    task = asyncio.create_task(ticker.run_forever(stop))
    await asyncio.wait_for(third_tick.wait(), timeout=5)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    assert ticks >= 3


async def test_run_forever_stops_promptly_when_asked() -> None:
    """A container stop is a SIGTERM; the loop must not wait out a whole interval."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    ticker, _ = _ticker(handler, interval_seconds=30)
    stop = asyncio.Event()
    task = asyncio.create_task(ticker.run_forever(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=2)  # not 30


def test_breaker_allows_calls_while_closed() -> None:
    breaker = Breaker(threshold=3, cooldown_seconds=60)
    assert breaker.allows(NOW)
    breaker.record_failure(NOW)
    assert breaker.allows(NOW), "one failure is not an outage"


async def test_the_heartbeat_advances_on_every_tick_including_failed_ones(tmp_path) -> None:
    """The failure a ticker actually has is a hung loop, not a dead process — the
    orchestrator already restarts a dead one. So the probe asks "did the loop come round",
    which means a tick whose dependency was down must still beat."""
    from agent_schedules.config.settings import Settings
    from agent_schedules.ticker import alive

    beat = tmp_path / "ticker.heartbeat"

    def down(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("schedules API is down")

    ticker, _ = _ticker(down, heartbeat_path=str(beat))
    assert not beat.exists()

    stop = asyncio.Event()
    task = asyncio.create_task(ticker.run_forever(stop))
    for _ in range(200):
        if beat.exists():
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    assert beat.exists(), "a tick whose dependency failed must still prove the loop turned"
    settings = Settings(ticker=ticker._config)
    assert alive(settings) is True


def test_a_missing_or_stale_heartbeat_reads_as_dead(tmp_path) -> None:
    import time as _time

    from agent_schedules.config.settings import Settings, TickerSettings
    from agent_schedules.ticker import alive

    beat = tmp_path / "ticker.heartbeat"
    config = TickerSettings(heartbeat_path=str(beat), heartbeat_max_age_seconds=60)
    settings = Settings(ticker=config)

    assert alive(settings) is False, "never started is not alive"

    beat.write_text(str(_time.time() - 3600))
    assert alive(settings) is False, "an hour-old beat is a hung loop"

    beat.write_text(str(_time.time()))
    assert alive(settings) is True

    beat.write_text("not a number")
    assert alive(settings) is False, "an unreadable beat must not read as healthy"
