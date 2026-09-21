"""Who the caller is, and what that lets them be.

This service creates runs as users who are not present to approve them, which makes the two
questions here the whole security model: *which tenant* a request speaks for, and *which
identity* it may make a run execute as. Both used to be answered by strings in the request —
an ``X-Tenant-Id`` header and an ``on_behalf_of`` field — with the credential, when it was
checked at all, saying nothing about either.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from agent_schedules.config.settings import (
    Credential,
    DatabaseSettings,
    RunsSettings,
    ServiceSettings,
    Settings,
)
from tests.conftest import DB_URL, scheduled

PROD_KEYS = {"prod-key": Credential(tenant_id="acme", principal="user_ada", may_act_as=["*"])}


def _production() -> Settings:
    """A configuration the startup check accepts outside dev."""
    return Settings(
        database=DatabaseSettings(url=DB_URL),
        service=ServiceSettings(
            environment="prod", auth_mode="api_key", api_keys=dict(PROD_KEYS)
        ),
        runs=RunsSettings(api_key="a-real-prod-key"),
    )


async def _anonymous(client: AsyncClient, **headers: str) -> AsyncIterator[AsyncClient]:
    """A client with no API key of its own — the fixture client always carries one."""
    return AsyncClient(
        transport=ASGITransport(app=client._transport.app),
        base_url="http://schedules",
        headers=headers,
    )


# --------------------------------------------------------------------- the credential check


def test_an_unknown_auth_mode_is_refused_before_the_service_starts() -> None:
    """``auth_mode`` was a free-form string next to a check that compared it to one literal,
    so every misspelling was a silent "authentication off" — including in dev, where the
    dev key then accepted any value at all."""
    with pytest.raises(ValidationError):
        ServiceSettings(auth_mode="trusted-dev")
    with pytest.raises(ValidationError):
        ServiceSettings(auth_mode="oidc")


def test_a_production_configuration_cannot_ship_the_credentials_from_the_repo() -> None:
    settings = _production()
    settings.check()  # the shape a real deployment has: accepted

    settings.service.api_keys["dev-key"] = Credential(tenant_id="acme", principal="user_ada")
    with pytest.raises(ValueError, match="dev credential"):
        settings.check()


def test_a_deployment_without_credentials_is_refused_rather_than_left_open() -> None:
    settings = _production()
    settings.service.api_keys = {}
    with pytest.raises(ValueError, match="api_keys is empty"):
        settings.check()


async def test_authentication_does_not_depend_on_the_auth_mode(client) -> None:
    """The fail-open this service shipped with: the credential check was gated on
    ``auth_mode == "trusted_dev"``, and the startup check refuses that mode outside dev — so
    every configuration an operator was pushed toward authenticated nobody at all, with a
    bare ``X-Tenant-Id`` header as the only claim of identity."""
    app = client._transport.app
    app.state.settings = _production()
    app.state.settings.check()  # not a configuration anything would complain about

    anonymous = await _anonymous(client, **{"X-Tenant-Id": "acme"})
    async with anonymous:
        assert (await anonymous.get("/v1/schedules")).status_code == 401
        assert (await anonymous.post("/v1/schedules", json=scheduled())).status_code == 401
    assert (await client.get("/v1/schedules", headers={"X-Api-Key": "nope"})).status_code == 401
    assert (
        await client.get("/v1/schedules", headers={"X-Api-Key": "prod-key"})
    ).status_code == 200


async def test_the_tenant_comes_from_the_credential_not_from_a_header(client) -> None:
    """A key knows which tenant it was issued to, so the header is a courtesy."""
    created = (await client.post("/v1/schedules", json=scheduled())).json()
    anonymous = await _anonymous(client, **{"X-Api-Key": "dev-key"})
    async with anonymous:
        listed = await anonymous.get("/v1/schedules")
    assert listed.status_code == 200
    assert [s["schedule_id"] for s in listed.json()] == [created["schedule_id"]]


# ------------------------------------------------------------------ the credential's tenant


async def test_a_credential_cannot_speak_for_another_tenant(client, other_tenant, runs) -> None:
    """The flat key list was the bug: any valid key plus a chosen header was any tenant, so
    one customer could read another's schedules, see the identities they run as, and fire
    them — and no test could catch it, because both tenants held the same secret."""
    theirs = (
        await other_tenant.post("/v1/schedules", json=scheduled(tenant_id="globex"))
    ).json()

    borrowed = {"X-Tenant-Id": "globex"}
    assert (await client.get("/v1/schedules", headers=borrowed)).status_code == 403
    assert (
        await client.get(f"/v1/schedules/{theirs['schedule_id']}", headers=borrowed)
    ).status_code == 403
    assert (
        await client.post(f"/v1/schedules/{theirs['schedule_id']}/fire", headers=borrowed)
    ).status_code == 403
    assert (
        await client.post(f"/v1/schedules/{theirs['schedule_id']}/pause", headers=borrowed)
    ).status_code == 403
    assert runs.calls == []


async def test_each_tenants_credential_is_its_own(client, other_tenant) -> None:
    """Stated from the other side, because the suite used to hand both tenants ``dev-key``
    and then prove isolation by trusting the header it also let the caller choose."""
    reaching_back = await other_tenant.get("/v1/schedules", headers={"X-Tenant-Id": "acme"})
    assert reaching_back.status_code == 403
    wrong_key = await client.get("/v1/schedules", headers={"X-Api-Key": "globex-key"})
    assert wrong_key.status_code == 403


# --------------------------------------------------------------- the credential's principal


async def test_a_schedule_cannot_be_created_as_a_principal_the_caller_may_not_be(
    narrow, runs
) -> None:
    """The way around every other control on this service: ``on_behalf_of`` is refused on
    update and refused on fire, but it was taken verbatim at creation — so naming
    ``user_root`` on a brand-new schedule and firing it produced exactly the run that the
    update and fire models exist to prevent, with a 201 in front of it."""
    refused = await narrow.post("/v1/schedules", json=scheduled(on_behalf_of="user_root"))
    assert refused.status_code == 403
    assert runs.calls == []

    mine = await narrow.post("/v1/schedules", json=scheduled(on_behalf_of="user_bob"))
    assert mine.status_code == 201
    fired = await narrow.post(f"/v1/schedules/{mine.json()['schedule_id']}/fire")
    assert fired.status_code == 200
    assert runs.bodies()[0]["on_behalf_of"] == "user_bob"


async def test_created_by_is_written_from_the_credential_not_from_the_body(narrow) -> None:
    """``created_by`` is the half of the audit sentence that says who arranged this. A
    caller-supplied one is a signature on a forgery — and it is the only identity this
    service forwards into a run's metadata."""
    refused = await narrow.post(
        "/v1/schedules", json=scheduled(on_behalf_of="user_bob", created_by="user_root")
    )
    assert refused.status_code == 422

    created = (
        await narrow.post("/v1/schedules", json=scheduled(on_behalf_of="user_bob"))
    ).json()
    assert created["created_by"] == "user_bob"


async def test_a_caller_cannot_repoint_a_schedule_that_runs_as_someone_else(
    client, narrow, runs
) -> None:
    """Binding ``on_behalf_of`` at creation is not enough on its own: an update may still
    change ``agent_id`` and ``input``, so pointing a colleague's schedule at another agent
    with another instruction would run whatever the attacker likes as *them*, which is the
    escalation the identity rules were written to stop."""
    victim = (
        await client.post(
            "/v1/schedules", json=scheduled(on_behalf_of="user_ada", input={"topic": "inbox"})
        )
    ).json()
    sid = victim["schedule_id"]

    repoint = {"agent_id": "shell", "input": {"command": "exfiltrate"}}
    assert (await narrow.put(f"/v1/schedules/{sid}", json=repoint)).status_code == 403
    assert (await narrow.post(f"/v1/schedules/{sid}/fire")).status_code == 403
    assert (await narrow.post(f"/v1/schedules/{sid}/pause")).status_code == 403
    assert (await narrow.delete(f"/v1/schedules/{sid}")).status_code == 403
    assert runs.calls == []

    untouched = (await client.get(f"/v1/schedules/{sid}")).json()
    assert untouched["agent_id"] == "briefing"
    assert untouched["input"] == {"topic": "inbox"}
    assert untouched["enabled"] is True
