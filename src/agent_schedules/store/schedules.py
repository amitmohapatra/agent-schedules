"""Reading and writing schedules. Everything that touches the table is here.

The firing path is the interesting one: :meth:`ScheduleStore.claim` takes a row lock that
is held for the rest of the transaction, and the success/failure bookkeeping happens under
that same lock. Concurrency is the whole reason — two ticker replicas waking on the same
tick is the normal case, not the exception, and the idempotency key alone only dedupes the
*run*, not this table's counters.

One rule runs through all of it: ``next_fire_at`` only ever moves *forward past now*. It is
the clock of an unattended loop, so a value in the past means "due", and a value further in
the past means "due, repeatedly, for every tick in between" — each one a different
idempotency key, so each one a real run nothing deduplicates. No caller-supplied instant is
allowed to become that clock unfiltered.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from agent_schedules.domain.cadence import next_fire_at
from agent_schedules.domain.models import (
    DuplicateSchedule,
    Schedule,
    ScheduleCreate,
    ScheduleUpdate,
)
from agent_schedules.store.tables import ScheduleRow

#: A backoff ceiling, so a generous ``max_consecutive_failures`` cannot push the next retry
#: of a doubling backoff past the point where anyone would still call it a retry.
MAX_RETRY_BACKOFF = timedelta(hours=1)


@dataclass(frozen=True)
class FailurePolicy:
    """What this deployment does about a fire agent-runs would not take.

    One object rather than two loose numbers so the two halves cannot drift apart: how many
    strikes there are is meaningless without how far apart they are spaced.
    """

    max_consecutive_failures: int
    retry_backoff: timedelta


def _to_schedule(row: ScheduleRow) -> Schedule:
    return Schedule(
        schedule_id=row.schedule_id,
        tenant_id=row.tenant_id,
        agent_id=row.agent_id,
        name=row.name,
        cadence=row.cadence,
        timezone=row.timezone,
        input=row.input,
        on_behalf_of=row.on_behalf_of,
        created_by=row.created_by,
        enabled=row.enabled,
        next_fire_at=row.next_fire_at,
        last_fired_at=row.last_fired_at,
        last_run_id=row.last_run_id,
        consecutive_failures=row.consecutive_failures,
        last_error=row.last_error,
        retry_after=row.retry_after,
        metadata=row.schedule_metadata or {},
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _reschedule(row: ScheduleRow, *, after: datetime) -> None:
    """Recompute ``next_fire_at`` from a reference time supplied by the caller."""
    row.next_fire_at = next_fire_at(row.cadence, after=after, timezone=row.timezone)


def _is_still_owed(row: ScheduleRow, *, at: datetime) -> bool:
    """Whether the armed occurrence is one this schedule can still honour at ``at``.

    Late is not the same as stale. A daily schedule that came due at midnight and was
    paused by a three-minute dependency wobble should still run this morning's briefing
    when it resumes — that occurrence has not been superseded. One that was paused for a
    month has missed it: the tick it is armed for is older than the following occurrence,
    and every tick since is a run nobody wants, so it starts again from the next one.
    """
    pending = row.next_fire_at
    if pending is None:
        return False
    if pending > at:
        return True
    following = next_fire_at(row.cadence, after=pending, timezone=row.timezone)
    return following is not None and following > at


def _rearm(row: ScheduleRow, *, at: datetime) -> None:
    """Arm the next fire, keeping an occurrence that is merely late rather than stale."""
    if not _is_still_owed(row, at=at):
        _reschedule(row, after=at)


def _backoff(base: timedelta, failures: int) -> timedelta:
    """A doubling delay: the dependency that just failed gets longer to come back."""
    return min(base * 2 ** max(failures - 1, 0), MAX_RETRY_BACKOFF)


class ScheduleStore:
    """Every schedule operation, in one place."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self, spec: ScheduleCreate, *, at: datetime, created_by: str | None = None
    ) -> Schedule:
        """Create a schedule whose first fire is the next occurrence after ``at``.

        ``created_by`` is passed in rather than read off ``spec``: it is the calling
        credential's own principal, and the create body has no say in it.

        Raises :class:`DuplicateSchedule` if the tenant already uses that name — a retried
        create must not leave two schedules quietly firing the same job forever.
        """
        row = ScheduleRow(
            schedule_id=spec.schedule_id or f"sch_{uuid.uuid4().hex}",
            tenant_id=spec.tenant_id,
            agent_id=spec.agent_id,
            name=spec.name,
            cadence=spec.cadence,
            timezone=spec.timezone,
            input=spec.input,
            on_behalf_of=spec.on_behalf_of,
            created_by=created_by,
            enabled=spec.enabled,
            consecutive_failures=0,
            schedule_metadata=spec.metadata or None,
        )
        _reschedule(row, after=at)
        self._session.add(row)
        try:
            await self._session.flush()
        except IntegrityError as exc:
            # The pre-check would be a race; the unique index is the real guard, so the
            # conflict is caught here rather than guessed at above.
            await self._session.rollback()
            raise DuplicateSchedule(spec.tenant_id, spec.name) from exc
        return _to_schedule(row)

    async def get(self, tenant_id: str, schedule_id: str) -> Schedule | None:
        row = await self._row(tenant_id, schedule_id)
        return _to_schedule(row) if row else None

    async def list(
        self,
        tenant_id: str,
        *,
        enabled: bool | None = None,
        agent_id: str | None = None,
        limit: int = 50,
    ) -> list[Schedule]:
        query = select(ScheduleRow).where(ScheduleRow.tenant_id == tenant_id)
        filters: dict[Any, Any] = {ScheduleRow.enabled: enabled, ScheduleRow.agent_id: agent_id}
        for column, value in filters.items():
            if value is not None:
                query = query.where(column == value)
        query = query.order_by(ScheduleRow.created_at.desc()).limit(min(limit, 500))
        return [_to_schedule(r) for r in (await self._session.scalars(query)).all()]

    async def due(self, tenant_id: str, *, at: datetime, limit: int = 100) -> list[Schedule]:
        """Enabled schedules whose time has come at ``at``, soonest first.

        Disabled schedules keep their ``next_fire_at`` — pausing is not forgetting — so the
        ``enabled`` filter here is what makes a pause actually stop the firing.

        ``retry_after`` is the other filter. A schedule that just failed a retryable fire is
        still armed for the same tick, but handing it back on the next poll makes the retry
        interval the *ticker's*, not the schedule's: three strikes becomes three ticks, and
        a dependency having a bad minute auto-pauses everything pointed at it.
        """
        query = (
            select(ScheduleRow)
            .where(
                ScheduleRow.tenant_id == tenant_id,
                ScheduleRow.enabled.is_(True),
                ScheduleRow.next_fire_at.is_not(None),
                ScheduleRow.next_fire_at <= at,
                or_(ScheduleRow.retry_after.is_(None), ScheduleRow.retry_after <= at),
            )
            .order_by(ScheduleRow.next_fire_at)
            .limit(min(limit, 500))
        )
        return [_to_schedule(r) for r in (await self._session.scalars(query)).all()]

    async def update(
        self, tenant_id: str, schedule_id: str, change: ScheduleUpdate, *, at: datetime
    ) -> Schedule | None:
        """Apply the fields the caller actually sent.

        ``exclude_unset`` rather than ``exclude_none``: ``input: null`` is a caller clearing
        the instruction, and treating it as "unchanged" would ignore a deliberate edit.
        """
        row = await self._row(tenant_id, schedule_id)
        if row is None:
            return None
        fields = change.model_dump(exclude_unset=True)
        metadata = fields.pop("metadata", None)
        if metadata is not None:
            row.schedule_metadata = {**(row.schedule_metadata or {}), **metadata}
        # Keyed on what actually *changed*, not on what was merely present. This is a PUT,
        # so the ordinary client sends the whole representation back; rearming because
        # "cadence" appeared in the body meant renaming a schedule at 23:59 silently threw
        # away tonight's run, and answered 200 while doing it.
        retimed = any(
            name in fields and fields[name] != getattr(row, name)
            for name in ("cadence", "timezone")
        )
        re_enabled = fields.get("enabled") is True and not row.enabled
        for name, value in fields.items():
            setattr(row, name, value)
        if retimed:
            # A new cadence or zone makes the stored next_fire_at the answer to the old
            # question, so it is recomputed from scratch.
            _reschedule(row, after=at)
        elif re_enabled:
            # Re-enabling has to look forward, or the schedule fires its backlog — but an
            # occurrence it can still honour is kept rather than thrown away.
            _rearm(row, at=at)
        row.updated_at = datetime.now(UTC)
        await self._session.flush()
        return _to_schedule(row)

    async def set_enabled(
        self, tenant_id: str, schedule_id: str, *, enabled: bool, at: datetime
    ) -> Schedule | None:
        """Pause or resume.

        Resuming resets ``consecutive_failures``: a human saying "try again" is the signal
        the auto-pause was waiting for, and leaving the counter at its cap would re-pause the
        schedule on the very next hiccup. Resuming looks forward from ``at`` rather than
        replaying every tick missed while paused — but it keeps an occurrence that is only
        late, because the common resume is a human clearing a short dependency outage and
        the fire that outage swallowed is exactly the one they came to rescue.
        """
        row = await self._row(tenant_id, schedule_id)
        if row is None:
            return None
        row.enabled = enabled
        if enabled:
            row.consecutive_failures = 0
            row.last_error = None
            row.retry_after = None
            _rearm(row, at=at)
        row.updated_at = datetime.now(UTC)
        await self._session.flush()
        return _to_schedule(row)

    async def delete(self, tenant_id: str, schedule_id: str) -> bool:
        row = await self._row(tenant_id, schedule_id)
        if row is None:
            return False
        await self._session.delete(row)
        await self._session.flush()
        return True

    async def claim(self, tenant_id: str, schedule_id: str) -> Schedule | None:
        """Lock the row for the rest of this transaction and return it.

        The lock — not the idempotency key — is what stops two ticker replicas from both
        counting a failure or both advancing ``next_fire_at`` for one tick.
        """
        row = await self._row(tenant_id, schedule_id, lock=True)
        return _to_schedule(row) if row else None

    async def record_success(
        self, tenant_id: str, schedule_id: str, *, fire_time: datetime, run_id: str, now: datetime
    ) -> Schedule:
        """Mark a fire that agent-runs accepted, and arm the next one."""
        row = await self._require_row(tenant_id, schedule_id)
        row.last_fired_at = fire_time
        row.last_run_id = run_id
        # Reset, not decrement: "three failures then a success" is a healthy schedule, and a
        # decrementing counter would pause it on the next single blip.
        row.consecutive_failures = 0
        row.last_error = None
        row.retry_after = None
        # From ``now``, never from behind it. ``fire_time`` is which tick was just fired,
        # and it is frequently in the past — a late ticker, an outage, an operator firing
        # the occurrence that a failure ate. Anchoring the next occurrence on it leaves the
        # schedule still due the moment it finishes, so the ticker fires the next backlogged
        # tick, and the next: a week of downtime becomes 168 runs of a stale instruction,
        # none of them deduplicated, because every tick is its own idempotency key. Firing a
        # missed tick is a catch-up; it is not a licence to replay the calendar.
        _reschedule(row, after=max(fire_time, now))
        row.updated_at = now
        await self._session.flush()
        return _to_schedule(row)

    async def record_failure(
        self,
        tenant_id: str,
        schedule_id: str,
        *,
        error: dict[str, Any],
        policy: FailurePolicy,
        now: datetime,
    ) -> Schedule:
        """Record a fire agent-runs would not take, and auto-pause once it keeps happening.

        Auto-pause exists because nobody is watching: a schedule whose target agent was
        deleted would otherwise hammer a struggling dependency forever, on a timer, for a
        user who never sees the errors. Pausing turns an endless failure into one a person
        can find and resume.

        ``retryable`` decides *which* endless failure this is, and the two deserve opposite
        answers. A refusal that will never change its mind — an unknown agent, a revoked key
        — is already permanent on the first try, so burning two more ticks to prove it only
        delays the alert. A dependency that is merely down or slow gets the strikes it was
        promised, spaced by a backoff, so that "three strikes" is a real interval rather than
        three turns of whatever loop happens to be polling.
        """
        row = await self._require_row(tenant_id, schedule_id)
        row.consecutive_failures += 1
        row.last_error = error
        permanent = not error.get("retryable", False)
        if permanent or row.consecutive_failures >= policy.max_consecutive_failures:
            row.enabled = False
            row.retry_after = None
        else:
            row.retry_after = now + _backoff(policy.retry_backoff, row.consecutive_failures)
        row.updated_at = now
        await self._session.flush()
        return _to_schedule(row)

    async def _row(
        self, tenant_id: str, schedule_id: str, *, lock: bool = False
    ) -> ScheduleRow | None:
        query = select(ScheduleRow).where(
            ScheduleRow.tenant_id == tenant_id, ScheduleRow.schedule_id == schedule_id
        )
        if lock:
            query = query.with_for_update()
        return await self._session.scalar(query)

    async def _require_row(self, tenant_id: str, schedule_id: str) -> ScheduleRow:
        row = await self._row(tenant_id, schedule_id)
        if row is None:
            # Only reachable if a caller skipped claim(); loudly, because a silent no-op
            # here would look like a successful fire that recorded nothing.
            raise LookupError(f"schedule {schedule_id} vanished mid-fire")
        return row
