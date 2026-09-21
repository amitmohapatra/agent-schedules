"""Fixtures. A real PostgreSQL when one is reachable, skipped with a reason when not —
never silently passed against a stand-in that cannot reproduce a race.

agent-runs is the one thing that *is* faked, through ``httpx.MockTransport``, so the real
:class:`RunsClient` — headers, error mapping, the refusal to invent a run id — is still the
code under test. The fake deduplicates on ``(tenant, idempotency_key)`` exactly as agent-runs
does; a fake that always created a new run would let a broken idempotency key pass.
"""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from itertools import count
from typing import Any

import httpx
import pytest
from httpx import ASGITransport, AsyncClient, MockTransport
from sqlalchemy import text

from agent_schedules.api import deps
from agent_schedules.api.app import create_app
from agent_schedules.clients.runs import RunsClient
from agent_schedules.config.settings import (
    Credential,
    DatabaseSettings,
    ServiceSettings,
    Settings,
)

ADMIN_URL = os.environ.get(
    "SCHEDULES_TEST_ADMIN_URL", "postgresql://memory:memory@localhost:5432/postgres"
)
DB_NAME = os.environ.get("SCHEDULES_TEST_DB", "agent_schedules_tests")
DB_URL = f"postgresql+psycopg://memory:memory@localhost:5432/{DB_NAME}"

#: Credentials are the authority in this service: a key names its tenant, who it is, and
#: who it may schedule runs as. The three here are deliberately different shapes.
#:
#: ``dev-key`` is a service credential — the ticker, which has to be able to fire any
#: schedule in acme whoever owns it. ``globex-key`` is the same for the other tenant, and is
#: a *different secret* on purpose: one key that worked for both tenants would have made
#: every isolation test in this suite pass while the header, not the credential, decided who
#: the caller was. ``narrow-key`` is an ordinary user who may only act as themself.
CREDENTIALS = {
    "dev-key": Credential(tenant_id="acme", principal="user_ada", may_act_as=["*"]),
    "globex-key": Credential(tenant_id="globex", principal="user_ada", may_act_as=["*"]),
    "narrow-key": Credential(tenant_id="acme", principal="user_bob"),
}

H = {"X-Api-Key": "dev-key", "X-Tenant-Id": "acme"}

_names = count(1)


def _pg_ready() -> bool:
    import psycopg

    try:
        with psycopg.connect(ADMIN_URL, autocommit=True, connect_timeout=2) as conn:
            exists = conn.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,)
            ).fetchone()
            if not exists:
                conn.execute(f'CREATE DATABASE "{DB_NAME}"')
        return True
    except Exception:
        return False


PG = _pg_ready()


class FakeRuns:
    """A stand-in agent-runs that behaves like the real one where it matters.

    ``refuse`` is how a test breaks it: ``"unreachable"`` raises a transport error the way a
    dead host does, ``"error"`` answers 503 the way a restarting one does, and ``"rejected"``
    answers 400 the way it does for a request that will never be acceptable.
    """

    def __init__(self) -> None:
        self.runs: dict[tuple[str, str], dict[str, Any]] = {}
        self.calls: list[dict[str, Any]] = []
        self.refuse: str | None = None

    def handle(self, request: httpx.Request) -> httpx.Response:
        if self.refuse == "unreachable":
            raise httpx.ConnectError("connection refused", request=request)
        if self.refuse == "error":
            return httpx.Response(503, json={"detail": "agent-runs is restarting"})
        if self.refuse == "rejected":
            # A refusal that will still be a refusal next tick: an agent that no longer
            # exists. Normalized to retryable=False, which is a different policy from 503.
            return httpx.Response(400, json={"detail": "no such agent"})
        body = json.loads(request.content)
        self.calls.append({"body": body, "headers": dict(request.headers)})
        key = (request.headers["x-tenant-id"], body["idempotency_key"])
        if key not in self.runs:
            self.runs[key] = {"run_id": f"run_{len(self.runs) + 1}", **body}
        return httpx.Response(201, json=self.runs[key])

    def bodies(self) -> list[dict[str, Any]]:
        return [call["body"] for call in self.calls]


@pytest.fixture
def runs() -> FakeRuns:
    return FakeRuns()


@pytest.fixture
async def client(runs: FakeRuns) -> AsyncIterator[AsyncClient]:
    if not PG:
        pytest.skip(f"no PostgreSQL at {ADMIN_URL}")
    settings = Settings(
        database=DatabaseSettings(url=DB_URL),
        service=ServiceSettings(api_keys=CREDENTIALS),
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        # Each test starts from an empty table: an auto-pause test that inherits another
        # test's failure count passes for the wrong reason.
        async with app.state.engine.begin() as conn:
            await conn.execute(text("TRUNCATE agent_schedules"))
        async with AsyncClient(
            transport=MockTransport(runs.handle), base_url="http://runs"
        ) as upstream:
            app.dependency_overrides[deps.runs_client] = lambda: RunsClient(
                upstream, api_key="dev-key"
            )
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport, base_url="http://schedules", headers=H
            ) as c:
                yield c


@pytest.fixture
async def narrow(client: AsyncClient) -> AsyncIterator[AsyncClient]:
    """An ordinary user of the same tenant, holding a credential with no delegation.

    Same tenant as ``client``, so nothing about tenant scoping can explain a refusal: what
    this client cannot do, it cannot do because the credential says who it is allowed to be.
    """
    async with AsyncClient(
        transport=ASGITransport(app=client._transport.app),
        base_url="http://schedules",
        headers={"X-Api-Key": "narrow-key", "X-Tenant-Id": "acme"},
    ) as c:
        yield c


@pytest.fixture
async def other_tenant(client: AsyncClient) -> AsyncIterator[AsyncClient]:
    """A second tenant against the same app.

    A whole client rather than per-request headers: httpx merges request headers *into* the
    client's defaults, so an override arrives as a second spelling of the same header and
    the server reads whichever comes first. Tenant isolation is the thing under test here,
    so the test must not depend on that resolution order.
    """
    async with AsyncClient(
        transport=ASGITransport(app=client._transport.app),
        base_url="http://schedules",
        headers={"X-Api-Key": "globex-key", "X-Tenant-Id": "globex"},
    ) as c:
        yield c


def scheduled(**over: Any) -> dict[str, Any]:
    """A create body. The name is unique per call because (tenant, name) is unique — a test
    that wants the collision asks for it by passing ``name``."""
    body = {
        "tenant_id": "acme",
        "agent_id": "briefing",
        "name": f"schedule-{next(_names)}",
        "cadence": "daily",
        "timezone": "UTC",
        "on_behalf_of": "user_ada",
    }
    body.update(over)
    return body
