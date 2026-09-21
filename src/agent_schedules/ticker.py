"""The loop that fires due schedules.

``firing.py`` says it plainly: *there is no ticker in this process*. This module is that
ticker, as its own process — it drives the same two public endpoints anyone else would,
``GET /v1/schedules/due`` and ``POST /v1/schedules/{id}/fire``, and holds no state and no
database connection of its own.

Two properties make the loop safe to run more than once:

* A fire is idempotent on ``(schedule_id, fire_time)``, so two replicas racing on one tick
  produce one run — as long as each passes the schedule's own ``next_fire_at``, which this
  does.
* Every per-schedule failure is already recorded durably by the API (the failure counter and
  the auto-pause), so a ticker that crashes mid-batch loses nothing but the rest of the batch,
  and the next tick picks those up because they are still due.

What the loop adds on top is restraint: one slow or dead schedules API must not become a
tight retry loop against a service that is already struggling, which is what the circuit
breaker below is for.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import httpx
import structlog

from agent_schedules.config.settings import Settings, TickerSettings, get_settings
from agent_schedules.observability.logging import configure_logging

log = structlog.get_logger(__name__)

#: Fire outcomes that are the schedule's business, not the ticker's, and must not count
#: against the breaker: the tick itself worked.
_EXPECTED_FIRE_STATUS = frozenset({409, 422, 502})


@dataclass
class Breaker:
    """Stop calling a dependency that is failing, and let one call through to find out when
    it has recovered.

    Deliberately counting *ticks*, not individual fires: a single schedule whose run cannot
    be created is a schedule problem the API already records, while a whole tick failing is
    the schedules API being unreachable.
    """

    threshold: int
    cooldown_seconds: float
    failures: int = field(default=0, init=False)
    opened_at: datetime | None = field(default=None, init=False)

    @property
    def is_open(self) -> bool:
        return self.opened_at is not None

    def allows(self, now: datetime) -> bool:
        """Whether a call may go out now. An open breaker admits one trial after cooldown."""
        if self.opened_at is None:
            return True
        # Half-open after the cooldown: exactly one tick goes through, and it either closes
        # the breaker on success or re-opens it on failure.
        return (now - self.opened_at).total_seconds() >= self.cooldown_seconds

    def record_success(self) -> None:
        if self.opened_at is not None:
            log.info("ticker.breaker_closed", after_failures=self.failures)
        self.failures = 0
        self.opened_at = None

    def record_failure(self, now: datetime) -> None:
        self.failures += 1
        if self.failures >= self.threshold and self.opened_at is None:
            self.opened_at = now
            log.warning(
                "ticker.breaker_opened",
                failures=self.failures,
                cooldown_seconds=self.cooldown_seconds,
            )
        elif self.opened_at is not None:
            # A failed trial restarts the cooldown rather than admitting a call every tick.
            self.opened_at = now


class Ticker:
    """One pass over the due schedules, on a timer."""

    def __init__(self, http: httpx.AsyncClient, config: TickerSettings) -> None:
        self._http = http
        self._config = config
        self._breaker = Breaker(config.breaker_threshold, config.breaker_cooldown_seconds)

    async def tick(self, *, now: datetime | None = None) -> tuple[int, int]:
        """Fire everything due. Returns ``(fired, skipped)``.

        Raises nothing: a tick that fails is recorded on the breaker and reported, because
        the loop's job is to still be running at the next interval.
        """
        now = now or datetime.now(UTC)
        if not self._breaker.allows(now):
            return (0, 0)
        try:
            due = await self._due()
        except (httpx.HTTPError, httpx.InvalidURL) as exc:
            self._breaker.record_failure(now)
            log.warning("ticker.due_failed", error=str(exc), failures=self._breaker.failures)
            return (0, 0)
        self._breaker.record_success()

        fired = skipped = 0
        for schedule in due:
            if await self._fire(schedule):
                fired += 1
            else:
                skipped += 1
        if due:
            log.info("ticker.tick", due=len(due), fired=fired, skipped=skipped)
        return (fired, skipped)

    async def _due(self) -> list[dict]:
        response = await self._http.get(
            "/v1/schedules/due", params={"limit": self._config.batch_size}
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, list) else []

    async def _fire(self, schedule: dict) -> bool:
        """Fire one schedule for its own ``next_fire_at``.

        Passing that instant rather than omitting it is what makes two tickers on one tick
        produce one run: the idempotency key is ``(schedule_id, fire_time)``.
        """
        schedule_id = schedule.get("schedule_id", "")
        body = {"at": schedule["next_fire_at"]} if schedule.get("next_fire_at") else {}
        try:
            response = await self._http.post(f"/v1/schedules/{schedule_id}/fire", json=body)
        except httpx.HTTPError as exc:
            log.warning("ticker.fire_failed", schedule_id=schedule_id, error=str(exc))
            return False
        if response.is_success:
            log.info("ticker.fired", schedule_id=schedule_id, at=body.get("at"))
            return True
        if response.status_code in _EXPECTED_FIRE_STATUS:
            # 409 raced a pause, 422 is a fire time the API refused, 502 is a run that could
            # not be created — all already durable on the schedule row. Nothing to retry here.
            log.info(
                "ticker.fire_declined",
                schedule_id=schedule_id,
                status=response.status_code,
                detail=response.text[:200],
            )
            return False
        log.warning("ticker.fire_unexpected", schedule_id=schedule_id, status=response.status_code)
        return False

    def beat(self) -> None:
        """Record that the loop came round. Never raises: a ticker must not die of a full
        disk on its liveness file when the work itself is still going through."""
        try:
            path = Path(self._config.heartbeat_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(str(time.time()))
        except OSError as exc:
            log.warning("ticker.heartbeat_failed", error=str(exc))

    async def run_forever(self, stop: asyncio.Event) -> None:
        """Tick until asked to stop, then finish the tick in flight and return."""
        log.info(
            "ticker.started",
            interval_seconds=self._config.interval_seconds,
            url=self._config.url,
        )
        while not stop.is_set():
            try:
                await self.tick()
            except Exception:  # the loop has to outlive any single tick
                log.exception("ticker.tick_crashed")
            # After the tick, not before, and after a failed one too: the question the probe
            # answers is "is the loop still going round", not "is the dependency healthy".
            self.beat()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self._config.interval_seconds)
        log.info("ticker.stopped")


def _client(config: TickerSettings) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=config.url.rstrip("/"),
        timeout=config.timeout_seconds,
        headers={"X-API-Key": config.api_key},
    )


async def run(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    configure_logging(
        level=settings.observability.log_level, json_output=settings.observability.log_json
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # A container stop is a SIGTERM. Without this the process dies mid-fire and the
        # orchestrator waits out its whole grace period for every restart.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    async with _client(settings.ticker) as http:
        await Ticker(http, settings.ticker).run_forever(stop)


def main() -> None:
    if "--probe" in sys.argv[1:]:
        probe()
    asyncio.run(run())


if __name__ == "__main__":
    main()


def alive(settings: Settings | None = None) -> bool:
    """Whether the loop came round recently enough. Used by the container healthcheck."""
    config = (settings or get_settings()).ticker
    try:
        beat = float(Path(config.heartbeat_path).read_text())
    except (OSError, ValueError):
        return False
    return (time.time() - beat) <= config.heartbeat_max_age_seconds


def probe() -> None:
    """``python -m agent_schedules.ticker --probe`` — exit 0 while the loop is turning."""
    raise SystemExit(0 if alive() else 1)
