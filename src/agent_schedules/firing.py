"""Firing a schedule: the one seam between this service and agent-runs.

The seam is a single call, ``POST /v1/runs``, idempotent on ``(schedule_id, fire_time)``.
Nothing here reads or writes run state — this service learns that a run was created and
nothing about how it went. If it ever needs more than that, the boundary is in the wrong
place and the answer is to ask agent-runs, not to grow a second runs table.

There is no ticker in this process. A loop inside the API would fire N times with N
replicas, and the schedules that most need firing are the ones on the busiest deployment.
The ticker is whatever calls ``GET /v1/schedules/due`` and then ``POST /fire``; this module
is what makes doing that twice harmless.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import structlog

from agent_schedules.clients.runs import RunsClient, RunsUnavailable
from agent_schedules.domain.models import (
    FireResult,
    FireTimeOutOfRange,
    Schedule,
    ScheduleNotFiring,
    ScheduleNotFound,
    idempotency_key,
)
from agent_schedules.store.schedules import FailurePolicy, ScheduleStore

log = structlog.get_logger(__name__)

#: How far ahead of ``now`` a caller may claim to be firing for.
#:
#: A fire is always *for* a tick that has already come due, so the honest answer is "not at
#: all"; the tolerance exists only because a ticker replica may read ``/due`` against a
#: clock a second or two ahead of this service's and pass back the ``next_fire_at`` it saw.
#: Anything beyond that is a typo, a local-time-as-UTC instant, or a skewed replica — and
#: without the bound each of those rewrote ``next_fire_at`` past the mistake and retired the
#: schedule until then, answering 200 and reporting ``enabled: true`` the entire time.
MAX_FIRE_SKEW = timedelta(minutes=1)


class FireFailed(Exception):
    """agent-runs would not take the run. Carries the schedule as it was left.

    The schedule matters to the caller: it says how many consecutive failures this makes and
    whether that tripped the auto-pause, which is the difference between "retry next tick"
    and "a human has to look at this".
    """

    def __init__(self, schedule: Schedule, cause: RunsUnavailable) -> None:
        super().__init__(f"schedule {schedule.schedule_id} could not fire: {cause}")
        self.schedule = schedule
        self.cause = cause


class Firing:
    """Fires schedules. Holds the two things a fire needs: the table, and the seam."""

    def __init__(self, store: ScheduleStore, runs: RunsClient, *, policy: FailurePolicy) -> None:
        self._store = store
        self._runs = runs
        self._policy = policy

    async def fire(
        self, tenant_id: str, schedule_id: str, *, at: datetime | None, now: datetime
    ) -> FireResult:
        """Create the run for one fire of one schedule.

        ``at`` is the instant being fired *for*: the same schedule and the same ``at``
        produce the same key, and agent-runs hands back the run it already created. Omitting
        it fires for the schedule's own ``next_fire_at`` while that is still due — so a
        ticker that forgot to pass it lands on the same key as one that did — and for
        ``now`` otherwise, which is what a manual "run it again" means.

        ``now`` is a parameter, not a clock read, so that "what happens when the tick is
        late" is a test rather than a stakeout.

        Raises :class:`ScheduleNotFound`, :class:`ScheduleNotFiring` for a disabled or paused
        schedule, :class:`FireTimeOutOfRange` for an ``at`` that has not arrived, and
        :class:`FireFailed` when agent-runs refuses. The failure is recorded on the schedule
        before it is raised; the caller must commit it.
        """
        # claim() holds a row lock for the rest of the transaction, so the outbound HTTP
        # call happens under it. That is deliberate: serializing two replicas on one tick is
        # exactly what the lock is for, and the client's timeout bounds how long it costs.
        schedule = await self._store.claim(tenant_id, schedule_id)
        if schedule is None:
            raise ScheduleNotFound(schedule_id)
        if not schedule.enabled:
            raise ScheduleNotFiring(schedule_id)

        fire_time = _fire_time(schedule, at=at, now=now)
        key = idempotency_key(schedule_id, fire_time)
        try:
            run_id = await self._runs.create_run(schedule, fire_time=fire_time, idempotency_key=key)
        except RunsUnavailable as exc:
            left = await self._store.record_failure(
                tenant_id,
                schedule_id,
                error=exc.error.model_dump(mode="json"),
                policy=self._policy,
                now=now,
            )
            log.warning(
                "schedule.fire_failed",
                schedule_id=schedule_id,
                consecutive_failures=left.consecutive_failures,
                auto_paused=not left.enabled,
                error=exc.error.code,
            )
            raise FireFailed(left, exc) from exc

        fired = await self._store.record_success(
            tenant_id, schedule_id, fire_time=fire_time, run_id=run_id, now=now
        )
        log.info(
            "schedule.fired",
            schedule_id=schedule_id,
            run_id=run_id,
            next_fire_at=fired.next_fire_at,
        )
        return FireResult(
            schedule_id=schedule_id,
            run_id=run_id,
            fire_time=fire_time,
            idempotency_key=key,
            schedule=fired,
        )


def _fire_time(schedule: Schedule, *, at: datetime | None, now: datetime) -> datetime:
    if at is not None:
        if at > now + MAX_FIRE_SKEW:
            raise FireTimeOutOfRange(at, now)
        return at
    due = schedule.next_fire_at
    return due if due is not None and due <= now else now
