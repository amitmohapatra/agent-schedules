"""Schedule routes. Owning when a run should start — never what the run then does.

Authorization here has two halves, and both are needed. The credential fixes the tenant, so
no header can reach across tenants. Within a tenant it also fixes *which principals the
caller may make runs execute as*, which is what keeps every write on this router from being
a way around the identity rules the models enforce: creating a schedule as someone else,
or repointing theirs at another agent, would otherwise do everything editing ``on_behalf_of``
would have done, only with a 201 instead of a 422.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

import structlog
from fastapi import APIRouter, HTTPException, Query, Response

from agent_schedules.api.deps import Caller, Fire, Session, Tenant
from agent_schedules.domain.models import (
    DuplicateSchedule,
    FireRequest,
    FireResult,
    FireTimeOutOfRange,
    Schedule,
    ScheduleCreate,
    ScheduleNotFiring,
    ScheduleNotFound,
    ScheduleUpdate,
    require_aware,
)
from agent_schedules.firing import FireFailed
from agent_schedules.store.schedules import ScheduleStore

log = structlog.get_logger(__name__)

_NO_CONTENT = 204
_UNPROCESSABLE = 422
_CONFLICT = 409
_NOT_FOUND = 404
_FORBIDDEN = 403
_BAD_GATEWAY = 502

router = APIRouter(prefix="/v1/schedules", tags=["schedules"])


@router.post("", status_code=201)
async def create(spec: ScheduleCreate, db: Session, who: Caller) -> Schedule:
    """Create a schedule, armed for its next occurrence.

    ``on_behalf_of`` is checked against the credential here, at the one moment it is ever
    accepted. Refusing to *change* it later is worth nothing on its own: a caller who can
    name any identity at creation can simply make a new schedule as anyone, fire it, and
    have this service — the most privileged component in the stack — run it for them.
    """
    if spec.tenant_id != who.tenant_id:
        raise HTTPException(_FORBIDDEN, "tenant_id does not match the authenticated tenant")
    if not who.may_schedule_for(spec.on_behalf_of):
        raise HTTPException(
            _FORBIDDEN,
            f"this credential may not schedule runs on behalf of {spec.on_behalf_of!r}",
        )
    try:
        schedule = await ScheduleStore(db).create(
            spec, at=datetime.now(UTC), created_by=who.principal
        )
    except DuplicateSchedule as exc:
        # 409 rather than 200-with-the-existing-row: two creates of one name are far more
        # often a mistake than a retry, and silently returning the old one hides an edit
        # the caller believed had landed.
        raise HTTPException(_CONFLICT, str(exc)) from exc
    await db.commit()
    log.info(
        "schedule.created",
        schedule_id=schedule.schedule_id,
        cadence=schedule.cadence,
        created_by=schedule.created_by,
        next_fire_at=schedule.next_fire_at,
    )
    return schedule


@router.get("")
async def listing(
    db: Session,
    tenant_id: Tenant,
    enabled: bool | None = None,
    agent_id: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> list[Schedule]:
    """This tenant's schedules, newest first."""
    return await ScheduleStore(db).list(tenant_id, enabled=enabled, agent_id=agent_id, limit=limit)


# Declared before /{schedule_id}: FastAPI matches in declaration order, and the other way
# round every request for the due list is answered with "no schedule 'due'".
@router.get("/due")
async def due(
    db: Session,
    tenant_id: Tenant,
    at: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[Schedule]:
    """What is due at a given instant, soonest first. ``at`` defaults to now.

    A ticker reads this and then fires each one *with that schedule's own next_fire_at*,
    which is what keeps two tickers on one tick down to a single run.
    """
    return await ScheduleStore(db).due(tenant_id, at=_instant(at), limit=limit)


@router.get("/{schedule_id}")
async def get(schedule_id: str, db: Session, tenant_id: Tenant) -> Schedule:
    schedule = await ScheduleStore(db).get(tenant_id, schedule_id)
    if schedule is None:
        raise HTTPException(_NOT_FOUND, f"no schedule {schedule_id}")
    return schedule


@router.put("/{schedule_id}")
async def update(schedule_id: str, change: ScheduleUpdate, db: Session, who: Caller) -> Schedule:
    """Edit a schedule. ``on_behalf_of`` is not among the things that can be edited."""
    await _may_administer(schedule_id, db, who)
    schedule = await ScheduleStore(db).update(
        who.tenant_id, schedule_id, change, at=datetime.now(UTC)
    )
    if schedule is None:
        raise HTTPException(_NOT_FOUND, f"no schedule {schedule_id}")
    await db.commit()
    log.info("schedule.updated", schedule_id=schedule_id, next_fire_at=schedule.next_fire_at)
    return schedule


@router.delete("/{schedule_id}", status_code=_NO_CONTENT)
async def delete(schedule_id: str, db: Session, who: Caller) -> Response:
    await _may_administer(schedule_id, db, who)
    if not await ScheduleStore(db).delete(who.tenant_id, schedule_id):
        raise HTTPException(_NOT_FOUND, f"no schedule {schedule_id}")
    await db.commit()
    log.info("schedule.deleted", schedule_id=schedule_id)
    return Response(status_code=_NO_CONTENT)


@router.post("/{schedule_id}/pause")
async def pause(schedule_id: str, db: Session, who: Caller) -> Schedule:
    """Stop firing, keep the record."""
    return await _set_enabled(schedule_id, db, who, enabled=False)


@router.post("/{schedule_id}/resume")
async def resume(schedule_id: str, db: Session, who: Caller) -> Schedule:
    """Start firing again, from the next occurrence it can still honour."""
    return await _set_enabled(schedule_id, db, who, enabled=True)


@router.post("/{schedule_id}/fire")
async def fire(
    schedule_id: str, db: Session, who: Caller, firing: Fire, body: FireRequest | None = None
) -> FireResult:
    """Fire now: create the run in agent-runs for this schedule's instant.

    This is both the manual trigger and what a ticker calls. Repeating it for the same
    instant is safe; repeating it for a *different* instant is a second run, which is why a
    ticker passes ``at``.
    """
    await _may_administer(schedule_id, db, who)
    request = body or FireRequest()
    try:
        result = await firing.fire(who.tenant_id, schedule_id, at=request.at, now=datetime.now(UTC))
    except ScheduleNotFound as exc:
        raise HTTPException(_NOT_FOUND, str(exc)) from exc
    except FireTimeOutOfRange as exc:
        # 422 and nothing written. The alternative — take it, and arm the schedule for the
        # occurrence after it — is how a single mistyped year silently retires a schedule
        # that goes on reporting itself healthy.
        raise HTTPException(_UNPROCESSABLE, str(exc)) from exc
    except ScheduleNotFiring as exc:
        # 409, not 400: the request is well formed, the schedule is simply not firing. A
        # ticker that raced a pause gets a clear "no" instead of an accidental run.
        raise HTTPException(_CONFLICT, str(exc)) from exc
    except FireFailed as exc:
        # Commit first. The failure counter and the auto-pause are the durable half of
        # "failed loudly"; losing them to the error path is how a broken schedule retries
        # forever without anyone ever seeing a third strike.
        await db.commit()
        raise HTTPException(
            _BAD_GATEWAY,
            {
                "message": str(exc),
                "consecutive_failures": exc.schedule.consecutive_failures,
                "auto_paused": not exc.schedule.enabled,
                "error": exc.cause.error.model_dump(mode="json"),
            },
        ) from exc
    await db.commit()
    return result


async def _may_administer(schedule_id: str, db: Session, who: Caller) -> Schedule:
    """The stored schedule, if this credential may act as the identity it runs as.

    Read before the write rather than folded into it, which is safe here because the one
    field it authorizes against — ``on_behalf_of`` — is the one field no API can change.

    404 before 403 on purpose: "that schedule exists but is not yours" is itself an answer
    about another user's schedules.
    """
    schedule = await ScheduleStore(db).get(who.tenant_id, schedule_id)
    if schedule is None:
        raise HTTPException(_NOT_FOUND, f"no schedule {schedule_id}")
    if not who.may_schedule_for(schedule.on_behalf_of):
        raise HTTPException(
            _FORBIDDEN,
            f"this credential may not act on a schedule that runs as {schedule.on_behalf_of!r}",
        )
    return schedule


async def _set_enabled(schedule_id: str, db: Session, who: Caller, *, enabled: bool) -> Schedule:
    await _may_administer(schedule_id, db, who)
    schedule = await ScheduleStore(db).set_enabled(
        who.tenant_id, schedule_id, enabled=enabled, at=datetime.now(UTC)
    )
    if schedule is None:
        raise HTTPException(_NOT_FOUND, f"no schedule {schedule_id}")
    await db.commit()
    log.info("schedule.enabled_changed", schedule_id=schedule_id, enabled=enabled)
    return schedule


def _instant(at: datetime | None) -> datetime:
    """A query instant, defaulted to now and refused if it carries no offset."""
    if at is None:
        return datetime.now(UTC)
    try:
        return require_aware(at, field="at")
    except ValueError as exc:
        raise HTTPException(_UNPROCESSABLE, str(exc)) from exc
