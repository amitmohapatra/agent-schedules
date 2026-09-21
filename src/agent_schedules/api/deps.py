"""Request-scoped dependencies: a session, who is calling, and the seam to agent-runs."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from agent_schedules.clients.runs import RunsClient
from agent_schedules.config.settings import Credential, Settings
from agent_schedules.firing import Firing
from agent_schedules.store.schedules import FailurePolicy, ScheduleStore

_UNAUTHORIZED = 401
_FORBIDDEN = 403


async def session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sessions() as s:
        yield s


def caller(
    request: Request,
    x_tenant_id: Annotated[str | None, Header()] = None,
    x_api_key: Annotated[str | None, Header()] = None,
) -> Credential:
    """Who is calling, resolved from the credential rather than from a header.

    The API key is checked on every request in every mode. It used to be checked only when
    ``auth_mode == "trusted_dev"``, which meant the startup check's own advice — use another
    mode outside dev — turned authentication off, and a typo in the mode name did the same
    thing silently.

    The credential, not ``X-Tenant-Id``, decides the tenant. A header can still be sent, but
    only to *agree*: a key issued to one tenant naming another is a 403, not a way to read a
    stranger's schedules and the on-behalf-of identities they fire as.
    """
    cfg: Settings = request.app.state.settings
    credential = cfg.service.api_keys.get(x_api_key) if x_api_key else None
    if credential is None:
        raise HTTPException(_UNAUTHORIZED, "unknown api key")
    if x_tenant_id is not None and x_tenant_id != credential.tenant_id:
        raise HTTPException(
            _FORBIDDEN, "X-Tenant-Id is not the tenant this credential is issued to"
        )
    return credential


Caller = Annotated[Credential, Depends(caller)]


def tenant(who: Caller) -> str:
    """The calling tenant.

    Every query in the store is scoped by this. It is a dependency rather than a parameter
    so that no route can forget it — a schedules service that leaks across tenants leaks
    the on-behalf-of identities those schedules fire as.
    """
    return who.tenant_id


def runs_client(request: Request) -> RunsClient:
    """The shared client to agent-runs.

    A dependency rather than a module global so a test can substitute a transport, and so
    the connection pool is built once at startup instead of per request.
    """
    return request.app.state.runs


Session = Annotated[AsyncSession, Depends(session)]
Tenant = Annotated[str, Depends(tenant)]
Runs = Annotated[RunsClient, Depends(runs_client)]


def firing(request: Request, db: Session, runs: Runs) -> Firing:
    """A fire wired to this deployment's auto-pause policy.

    Assembled here rather than in the route so the route cannot accidentally hard-code the
    failure threshold, and so a test substitutes one object instead of three.
    """
    cfg: Settings = request.app.state.settings
    policy = FailurePolicy(
        max_consecutive_failures=cfg.service.max_consecutive_failures,
        retry_backoff=timedelta(seconds=cfg.service.retry_backoff_seconds),
    )
    return Firing(ScheduleStore(db), runs, policy=policy)


Fire = Annotated[Firing, Depends(firing)]
