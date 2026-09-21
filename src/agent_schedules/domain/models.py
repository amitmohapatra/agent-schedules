"""What a schedule is, and what may change about one.

A *schedule* is a standing instruction: "run this agent, as this identity, this often".
It is deliberately not a frozen context. What is stored is the instruction — the agent, the
input, the identity — and every fire rebuilds the rest from scratch, because a weekly
schedule made in March and firing in September must pick up everything the agent has
learned since, not replay March's world.

``on_behalf_of`` is the field this whole service exists to protect. It is set once, when a
person is present to authorize it, and is never accepted again: not on an update, not on a
fire request. A schedule whose identity can be edited is a privilege-escalation primitive
with a nice REST API in front of it.

Set once is not the same as set freely. The value is checked against the caller's
credential at creation (see :class:`agent_schedules.config.settings.Credential`), because a
model that merely refuses to *change* the identity still hands out any identity to whoever
asks first: create a schedule as ``user_root``, fire it, and every guard on update and fire
has been walked around rather than through.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from agent_schedules.domain.cadence import validate_cadence, validate_timezone


def _now() -> datetime:
    return datetime.now(UTC)


class ScheduleCreate(BaseModel):
    """Create a schedule. ``on_behalf_of`` is required and has no default on purpose."""

    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    agent_id: str
    name: str = Field(min_length=1, max_length=200)
    cadence: str
    timezone: str = "UTC"
    #: The instruction the agent is given, rebuilt into a fresh context on every fire.
    input: Any = None
    #: Who the fired runs execute as. Captured while a person is present to grant it, and
    #: refused unless the calling credential may act as that principal.
    on_behalf_of: str = Field(min_length=1)
    #: Note what is *not* here: ``created_by``. An admin may well create a schedule that
    #: runs as someone else and an audit needs both halves of that sentence — but a
    #: self-asserted author is not an audit trail, it is a signature field on a forgery.
    #: It is taken from the credential instead, and ``extra="forbid"`` makes sending one a
    #: 422 rather than a quiet overwrite.
    enabled: bool = True
    schedule_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("cadence")
    @classmethod
    def _check_cadence(cls, value: str) -> str:
        return validate_cadence(value)

    @field_validator("timezone")
    @classmethod
    def _check_timezone(cls, value: str) -> str:
        return validate_timezone(value)


class ScheduleUpdate(BaseModel):
    """Change a schedule.

    The trap this model closes: there is no ``on_behalf_of`` and no ``tenant_id`` field, and
    ``extra="forbid"`` turns sending either into a 422 rather than a silent no-op. Rotating
    the identity of an existing schedule is indistinguishable from stealing it.

    ``agent_id`` and ``input`` *are* editable, because an owner changing what their own
    schedule does is the ordinary case. What stops that from being the same escalation by
    another route is authorization, not the model: the route refuses an update from a
    credential that may not act as the schedule's stored ``on_behalf_of``. Otherwise
    repointing someone else's schedule at another agent, keeping their identity, would do
    everything rotating the identity would have.
    """

    model_config = ConfigDict(extra="forbid")

    agent_id: str | None = None
    name: str | None = Field(default=None, min_length=1, max_length=200)
    cadence: str | None = None
    timezone: str | None = None
    input: Any = None
    enabled: bool | None = None
    metadata: dict[str, Any] | None = None

    @field_validator("cadence")
    @classmethod
    def _check_cadence(cls, value: str | None) -> str | None:
        return validate_cadence(value) if value is not None else None

    @field_validator("timezone")
    @classmethod
    def _check_timezone(cls, value: str | None) -> str | None:
        return validate_timezone(value) if value is not None else None


class Schedule(BaseModel):
    """A schedule as stored."""

    model_config = ConfigDict(extra="forbid")

    schedule_id: str
    tenant_id: str
    agent_id: str
    name: str
    cadence: str
    timezone: str = "UTC"
    input: Any = None
    on_behalf_of: str
    created_by: str | None = None
    enabled: bool = True
    next_fire_at: datetime | None = None
    last_fired_at: datetime | None = None
    last_run_id: str | None = None
    #: Consecutive *fire* failures — this service never learns whether a run succeeded, so
    #: this counts "agent-runs would not take the run", nothing more.
    consecutive_failures: int = 0
    last_error: dict[str, Any] | None = None
    #: Set while a retryable fire failure is backing off: the schedule stays armed for the
    #: same tick but is not handed to the ticker again until this instant passes.
    retry_after: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


class FireRequest(BaseModel):
    """Fire now. Optionally at a stated instant.

    Deliberately tiny, and deliberately without ``on_behalf_of``, ``agent_id`` or ``input``:
    everything a fired run executes as comes from the stored row. A fire request that could
    name its own identity would make every schedule a loaded gun pointed at its owner.

    ``at`` is the instant being fired *for*, and it carries the whole idempotency story: a
    ticker passes the schedule's ``next_fire_at`` so that two replicas racing on one tick
    produce one run. Omitting it falls back to that same instant while the schedule is still
    due, so a forgetful ticker usually agrees with a careful one — but once the tick has
    been fired the schedule has moved on and a bare fire means "now". For a manual fire that
    is exactly right: two deliberate clicks are two runs.

    ``at`` is an instant that has *arrived*, and the firing path bounds it against the
    clock. An instant in the future is not a tick anybody missed; accepting one used to
    rewrite the schedule past it and silently retire the schedule for as long as the caller
    fat-fingered — with ``enabled: true`` and no error the whole time.
    """

    model_config = ConfigDict(extra="forbid")

    at: datetime | None = None

    @field_validator("at")
    @classmethod
    def _check_at(cls, value: datetime | None) -> datetime | None:
        return require_aware(value, field="at") if value is not None else None


class FireResult(BaseModel):
    """What one fire did, including the key that makes repeating it harmless."""

    model_config = ConfigDict(extra="forbid")

    schedule_id: str
    run_id: str
    fire_time: datetime
    idempotency_key: str
    schedule: Schedule


class ScheduleNotFound(Exception):
    """No such schedule for this tenant."""

    def __init__(self, schedule_id: str) -> None:
        super().__init__(f"no schedule {schedule_id}")
        self.schedule_id = schedule_id


class ScheduleNotFiring(Exception):
    """The schedule exists but is disabled, so it must not fire."""

    def __init__(self, schedule_id: str) -> None:
        super().__init__(f"schedule {schedule_id} is disabled and will not fire")
        self.schedule_id = schedule_id


class FireTimeOutOfRange(Exception):
    """``at`` names an instant that has not arrived, so there is no tick to fire for."""

    def __init__(self, at: datetime, now: datetime) -> None:
        super().__init__(
            f"fire time {at.isoformat()} is in the future (now {now.isoformat()}): a fire is "
            "for a tick that has already come due, and firing past one retires the schedule"
        )
        self.at, self.now = at, now


class DuplicateSchedule(Exception):
    """A schedule with that name already exists for this tenant."""

    def __init__(self, tenant_id: str, name: str) -> None:
        super().__init__(f"tenant {tenant_id} already has a schedule named {name!r}")
        self.tenant_id, self.name = tenant_id, name


def require_aware(value: datetime, *, field: str) -> datetime:
    """Refuse a naive instant.

    An instant without an offset means whatever the reader's clock happens to be set to, and
    this service turns instants into idempotency keys. "2026-09-21T09:00" from a laptop in
    Berlin and the same string from a UTC container are two keys for one tick, i.e. two runs.
    """
    if value.tzinfo is None:
        raise ValueError(f"{field} must carry a UTC offset; a naive instant is ambiguous")
    return value


def idempotency_key(schedule_id: str, fire_time: datetime) -> str:
    """The key agent-runs deduplicates on: one schedule, one instant, one run.

    ``fire_time`` is normalized to UTC first. Without that, a ticker in Berlin and a ticker
    in UTC would spell the same instant two ways, hash to two keys, and run Monday twice.
    """
    return f"{schedule_id}@{fire_time.astimezone(UTC).isoformat()}"


__all__ = [
    "DuplicateSchedule",
    "FireRequest",
    "FireResult",
    "FireTimeOutOfRange",
    "Schedule",
    "ScheduleCreate",
    "ScheduleNotFiring",
    "ScheduleNotFound",
    "ScheduleUpdate",
    "idempotency_key",
    "require_aware",
]
