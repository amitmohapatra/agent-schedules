"""Schedules end to end over HTTP, against a real PostgreSQL and a faked agent-runs.

Every test here is a failure mode an unattended scheduler has to survive: two tickers on
one tick, a request trying to name its own identity, a dependency that is down, a schedule
that fails forever with nobody watching, one tenant reaching for another's schedules.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from httpx import AsyncClient
from sqlalchemy import text

from tests.conftest import scheduled


async def test_a_schedule_is_created_armed_for_its_next_occurrence(client) -> None:
    created = (await client.post("/v1/schedules", json=scheduled(input={"topic": "inbox"}))).json()
    assert created["enabled"] is True
    assert created["consecutive_failures"] == 0
    assert created["next_fire_at"] is not None
    assert created["input"] == {"topic": "inbox"}

    fetched = (await client.get(f"/v1/schedules/{created['schedule_id']}")).json()
    assert fetched["schedule_id"] == created["schedule_id"]


async def test_the_same_schedule_and_fire_time_never_create_two_runs(client, runs) -> None:
    """Two ticker replicas waking on the same tick is the normal case. The key is derived
    from (schedule_id, fire_time), so the second call gets the run the first one made."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    at = _last_tick().isoformat()

    first = (await client.post(f"/v1/schedules/{sid}/fire", json={"at": at})).json()
    second = (await client.post(f"/v1/schedules/{sid}/fire", json={"at": at})).json()

    assert first["run_id"] == second["run_id"]
    assert first["idempotency_key"] == second["idempotency_key"]
    assert len(runs.runs) == 1


async def test_a_bare_fire_uses_the_tick_it_is_due_for_not_the_wall_clock(client, runs) -> None:
    """A ticker that omits ``at`` still fires for the instant the schedule was due, so it
    agrees with one that passes ``at`` explicitly. If it stamped its own wall clock instead,
    the two would spell the same tick differently and agent-runs would see two runs."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    due_at = await _make_due(client, sid)

    bare = (await client.post(f"/v1/schedules/{sid}/fire")).json()
    assert datetime.fromisoformat(bare["fire_time"]) == due_at

    explicit = await client.post(f"/v1/schedules/{sid}/fire", json={"at": due_at.isoformat()})
    assert explicit.json()["run_id"] == bare["run_id"]
    assert len(runs.runs) == 1


async def test_a_fired_run_carries_the_schedules_own_identity(client, runs) -> None:
    """The point of the whole service: the run executes as the identity captured when a
    person was present, and agent-runs is told which schedule asked."""
    schedule = (await client.post("/v1/schedules", json=scheduled(on_behalf_of="user_ada"))).json()
    await client.post(f"/v1/schedules/{schedule['schedule_id']}/fire")

    body = runs.bodies()[0]
    assert body["on_behalf_of"] == "user_ada"
    assert body["tenant_id"] == "acme"
    assert body["metadata"]["schedule_id"] == schedule["schedule_id"]


async def test_a_fire_sends_todays_instruction_not_the_one_frozen_at_creation(client, runs) -> None:
    """What is stored is the instruction, and every fire rebuilds the request from it. A
    schedule that carried a frozen context would still be asking September's question in
    March, having learned nothing in between."""
    schedule = (await client.post("/v1/schedules", json=scheduled(input={"topic": "inbox"}))).json()
    sid = schedule["schedule_id"]
    await client.put(f"/v1/schedules/{sid}", json={"input": {"topic": "calendar"}})

    await client.post(f"/v1/schedules/{sid}/fire")
    assert runs.bodies()[-1]["input"] == {"topic": "calendar"}


async def test_on_behalf_of_is_required(client) -> None:
    """No default, no inference. A schedule without an identity is a run nobody authorized,
    and picking one for the caller is how it becomes "root" by accident."""
    body = scheduled()
    body.pop("on_behalf_of")
    assert (await client.post("/v1/schedules", json=body)).status_code == 422


async def test_an_empty_on_behalf_of_is_refused(client) -> None:
    assert (await client.post("/v1/schedules", json=scheduled(on_behalf_of=""))).status_code == 422


async def test_an_update_cannot_widen_on_behalf_of(client) -> None:
    """Rotating the identity of an existing schedule is indistinguishable from stealing it,
    so the field is not part of the update model at all."""
    schedule = (await client.post("/v1/schedules", json=scheduled())).json()
    sid = schedule["schedule_id"]

    refused = await client.put(f"/v1/schedules/{sid}", json={"on_behalf_of": "user_root"})
    assert refused.status_code == 422
    assert (await client.get(f"/v1/schedules/{sid}")).json()["on_behalf_of"] == "user_ada"


async def test_a_fire_request_cannot_widen_on_behalf_of(client, runs) -> None:
    """The fire body has one field. Anything else — an identity, an agent, an input — is a
    422, and the run is still built from the stored row."""
    schedule = (await client.post("/v1/schedules", json=scheduled())).json()
    sid = schedule["schedule_id"]

    refused = await client.post(
        f"/v1/schedules/{sid}/fire", json={"on_behalf_of": "user_root", "agent_id": "shell"}
    )
    assert refused.status_code == 422
    assert runs.calls == []


async def test_a_paused_schedule_does_not_fire(client, runs) -> None:
    """Pausing has to stop the firing, not merely hide the schedule from the due list."""
    schedule = (await client.post("/v1/schedules", json=scheduled())).json()
    sid = schedule["schedule_id"]
    assert (await client.post(f"/v1/schedules/{sid}/pause")).json()["enabled"] is False

    refused = await client.post(f"/v1/schedules/{sid}/fire")
    assert refused.status_code == 409
    assert runs.calls == []


async def test_a_schedule_created_disabled_does_not_fire(client, runs) -> None:
    """Same rule, reached the other way: enabled=false at creation is not a draft that
    quietly runs anyway."""
    schedule = (await client.post("/v1/schedules", json=scheduled(enabled=False))).json()
    refused = await client.post(f"/v1/schedules/{schedule['schedule_id']}/fire")
    assert refused.status_code == 409
    assert runs.calls == []


async def test_a_paused_schedule_is_not_due(client) -> None:
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    await client.post(f"/v1/schedules/{sid}/pause")

    later = (datetime.now(UTC) + timedelta(days=30)).isoformat()
    due = (await client.get("/v1/schedules/due", params={"at": later})).json()
    assert [s["schedule_id"] for s in due] == []


async def test_resuming_looks_forward_instead_of_replaying_the_backlog(client) -> None:
    """A schedule paused for a month must not fire 720 times on resume."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    await client.post(f"/v1/schedules/{sid}/pause")

    resumed = (await client.post(f"/v1/schedules/{sid}/resume")).json()
    assert resumed["enabled"] is True
    assert datetime.fromisoformat(resumed["next_fire_at"]) > datetime.now(UTC)


async def test_repeated_failures_auto_pause_the_schedule(client, runs) -> None:
    """Nobody is watching an unattended schedule fail. Three strikes turns an endless retry
    against a broken dependency into something a person can find and resume."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    runs.refuse = "error"

    for attempt in range(1, 4):
        failed = await client.post(f"/v1/schedules/{sid}/fire", json={"at": _instant(attempt)})
        assert failed.status_code == 502

    paused = (await client.get(f"/v1/schedules/{sid}")).json()
    assert paused["consecutive_failures"] == 3
    assert paused["enabled"] is False

    runs.refuse = None
    still_refused = await client.post(f"/v1/schedules/{sid}/fire")
    assert still_refused.status_code == 409


async def test_a_successful_fire_resets_the_failure_counter(client, runs) -> None:
    """ "Two failures then a success" is a healthy schedule; carrying the count forward would
    pause it on the next unrelated blip."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]

    runs.refuse = "error"
    for attempt in range(1, 3):
        await client.post(f"/v1/schedules/{sid}/fire", json={"at": _instant(attempt)})
    assert (await client.get(f"/v1/schedules/{sid}")).json()["consecutive_failures"] == 2

    runs.refuse = None
    fired = await client.post(f"/v1/schedules/{sid}/fire", json={"at": _instant(3)})
    assert fired.status_code == 200

    healthy = (await client.get(f"/v1/schedules/{sid}")).json()
    assert healthy["consecutive_failures"] == 0
    assert healthy["last_error"] is None
    assert healthy["enabled"] is True


async def test_resuming_clears_the_failure_count_that_caused_the_auto_pause(client, runs) -> None:
    """Otherwise the resumed schedule is already at its cap and re-pauses on the first
    hiccup, which reads to the user as "resume did nothing"."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    runs.refuse = "error"
    for attempt in range(1, 4):
        await client.post(f"/v1/schedules/{sid}/fire", json={"at": _instant(attempt)})

    resumed = (await client.post(f"/v1/schedules/{sid}/resume")).json()
    assert resumed["consecutive_failures"] == 0
    assert resumed["last_error"] is None


async def test_when_agent_runs_is_unreachable_the_fire_fails_loudly_and_is_recorded(
    client, runs
) -> None:
    """The silent-drop case. A fire that swallowed a dead dependency would leave a schedule
    that looks healthy, has no run, and no record that anything went wrong."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    runs.refuse = "unreachable"

    failed = await client.post(f"/v1/schedules/{sid}/fire")
    assert failed.status_code == 502
    detail = failed.json()["detail"]
    assert detail["consecutive_failures"] == 1
    assert detail["auto_paused"] is False

    recorded = (await client.get(f"/v1/schedules/{sid}")).json()
    assert recorded["consecutive_failures"] == 1
    assert recorded["last_error"]["category"] == "DEPENDENCY"
    assert recorded["last_error"]["code"] == "ConnectError"
    # The failed fire must not look like a delivered one.
    assert recorded["last_fired_at"] is None
    assert recorded["last_run_id"] is None


async def test_a_failed_fire_does_not_advance_the_schedule(client, runs) -> None:
    """If a failure moved next_fire_at on, the tick would be skipped rather than retried."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    runs.refuse = "unreachable"

    await client.post(f"/v1/schedules/{sid}/fire")
    assert (await client.get(f"/v1/schedules/{sid}")).json()["next_fire_at"] == schedule[
        "next_fire_at"
    ]


async def test_a_successful_fire_advances_the_schedule(client) -> None:
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    at = _last_tick()

    fired = (await client.post(f"/v1/schedules/{sid}/fire", json={"at": at.isoformat()})).json()
    assert datetime.fromisoformat(fired["schedule"]["last_fired_at"]) == at
    assert fired["schedule"]["last_run_id"] == fired["run_id"]
    assert datetime.fromisoformat(fired["schedule"]["next_fire_at"]) == at + timedelta(hours=1)


async def test_a_per_minute_cadence_is_refused_at_creation(client) -> None:
    """The cap is a product decision, enforced where a schedule is born rather than by
    hoping the ticker skips it."""
    refused = await client.post("/v1/schedules", json=scheduled(cadence="*/5 * * * *"))
    assert refused.status_code == 422
    assert "per-minute" in refused.text


async def test_a_per_minute_cadence_cannot_be_smuggled_in_by_an_update(client) -> None:
    schedule = (await client.post("/v1/schedules", json=scheduled())).json()
    refused = await client.put(
        f"/v1/schedules/{schedule['schedule_id']}", json={"cadence": "* * * * *"}
    )
    assert refused.status_code == 422


async def test_an_unknown_timezone_is_refused_at_creation(client) -> None:
    assert (
        await client.post("/v1/schedules", json=scheduled(timezone="Mars/Olympus_Mons"))
    ).status_code == 422


async def test_changing_the_cadence_rearms_the_schedule(client) -> None:
    """The stored next_fire_at answers the old cadence's question. Left alone it would fire
    the new schedule once more at the old time before ever honouring the new one."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="daily"))).json()
    sid = schedule["schedule_id"]

    updated = (await client.put(f"/v1/schedules/{sid}", json={"cadence": "0 9 * * 3"})).json()
    rearmed = datetime.fromisoformat(updated["next_fire_at"])
    # Wednesday 09:00, whatever day the test happens to run on.
    assert (rearmed.weekday(), rearmed.hour, rearmed.minute) == (2, 9, 0)


async def test_a_manual_schedule_has_no_next_fire_but_can_still_be_fired(client, runs) -> None:
    """ "Manual" means the ticker never picks it up, not that the button is disabled."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="manual"))).json()
    assert schedule["next_fire_at"] is None

    fired = await client.post(f"/v1/schedules/{schedule['schedule_id']}/fire")
    assert fired.status_code == 200
    assert len(runs.runs) == 1


async def test_due_returns_only_what_has_come_due(client) -> None:
    """The ticker's query. A schedule whose time has not arrived must not appear in it, or
    every poll fires everything."""
    overdue = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    later = (await client.post("/v1/schedules", json=scheduled(cadence="weekly"))).json()
    await _make_due(client, overdue["schedule_id"])

    due = [s["schedule_id"] for s in (await client.get("/v1/schedules/due")).json()]
    assert due == [overdue["schedule_id"]]
    assert later["schedule_id"] not in due


async def test_due_is_not_read_as_a_schedule_id(client) -> None:
    """Route order, which is silent when it is wrong: /due declared after /{schedule_id}
    would answer every ticker poll with 404."""
    assert (await client.get("/v1/schedules/due")).status_code == 200


async def test_a_naive_due_instant_is_refused(client) -> None:
    """An instant without an offset means whatever the caller's clock says, and this service
    turns instants into idempotency keys."""
    assert (
        await client.get("/v1/schedules/due", params={"at": "2026-09-21T09:00:00"})
    ).status_code == 422


async def test_listing_filters_by_agent_and_by_enabled(client) -> None:
    mine = (await client.post("/v1/schedules", json=scheduled(agent_id="briefing"))).json()
    other = (await client.post("/v1/schedules", json=scheduled(agent_id="billing"))).json()
    await client.post(f"/v1/schedules/{other['schedule_id']}/pause")

    briefing = (await client.get("/v1/schedules", params={"agent_id": "briefing"})).json()
    assert [s["schedule_id"] for s in briefing] == [mine["schedule_id"]]

    paused = (await client.get("/v1/schedules", params={"enabled": False})).json()
    assert [s["schedule_id"] for s in paused] == [other["schedule_id"]]


async def test_a_retried_create_does_not_leave_two_schedules_firing(client) -> None:
    """A client that retries a create it never saw the answer to would otherwise end up with
    two schedules quietly firing the same job forever."""
    body = scheduled(name="nightly digest")
    assert (await client.post("/v1/schedules", json=body)).status_code == 201
    again = await client.post("/v1/schedules", json=body)
    assert again.status_code == 409
    assert len((await client.get("/v1/schedules")).json()) == 1


async def test_two_tenants_may_use_the_same_schedule_name(client, other_tenant) -> None:
    """The name is unique per tenant. A global constraint would let one tenant's "nightly"
    block another's."""
    body = scheduled(name="nightly")
    assert (await client.post("/v1/schedules", json=body)).status_code == 201
    theirs = await other_tenant.post("/v1/schedules", json={**body, "tenant_id": "globex"})
    assert theirs.status_code == 201


async def test_one_tenant_cannot_read_or_fire_another_tenants_schedule(
    client, other_tenant, runs
) -> None:
    """Reaching across tenants here does not just leak a record, it borrows the identity
    that record fires as."""
    mine = (await client.post("/v1/schedules", json=scheduled())).json()
    sid = mine["schedule_id"]

    assert (await other_tenant.get(f"/v1/schedules/{sid}")).status_code == 404
    assert (await other_tenant.post(f"/v1/schedules/{sid}/fire")).status_code == 404
    assert (await other_tenant.delete(f"/v1/schedules/{sid}")).status_code == 404
    assert runs.calls == []


async def test_creating_a_schedule_for_another_tenant_is_refused(client) -> None:
    """The body must not be able to widen what the credential allows."""
    assert (
        await client.post("/v1/schedules", json=scheduled(tenant_id="globex"))
    ).status_code == 403


async def test_a_missing_api_key_is_rejected(client) -> None:
    # httpx merges request headers over the client's defaults rather than replacing them, so
    # a key cannot be removed per-request — it has to be explicitly wrong instead.
    assert (await client.get("/v1/schedules", headers={"X-Api-Key": "nope"})).status_code == 401


async def test_a_deleted_schedule_stops_existing(client) -> None:
    schedule = (await client.post("/v1/schedules", json=scheduled())).json()
    sid = schedule["schedule_id"]
    assert (await client.delete(f"/v1/schedules/{sid}")).status_code == 204
    assert (await client.get(f"/v1/schedules/{sid}")).status_code == 404
    assert (await client.delete(f"/v1/schedules/{sid}")).status_code == 404


async def test_health_reports_the_database(client) -> None:
    assert (await client.get("/health/live")).json() == {"status": "ok"}
    assert (await client.get("/health/ready")).json() == {"status": "ok"}


async def test_readiness_does_not_depend_on_agent_runs(client, runs) -> None:
    """A dependency's outage must not pull this service out of rotation: schedules can still
    be listed, paused and edited while agent-runs is down."""
    runs.refuse = "unreachable"
    assert (await client.get("/health/ready")).json() == {"status": "ok"}
    assert (await client.get("/v1/schedules")).status_code == 200


async def test_a_fire_for_a_stale_instant_does_not_rewind_the_schedule(client, runs) -> None:
    """``at`` is the tick being fired for, not a new clock for the schedule.

    Rescheduling from a caller-supplied instant meant one request with an old ``at`` moved
    next_fire_at into the past, and a schedule in the past is due on every poll — each tick
    a different idempotency key, so each tick a genuinely new run executing as the owner,
    for as many hours as the caller rewound.
    """
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    stale = _last_tick() - timedelta(days=30)

    fired = await client.post(f"/v1/schedules/{sid}/fire", json={"at": stale.isoformat()})
    assert fired.status_code == 200
    assert datetime.fromisoformat(fired.json()["schedule"]["next_fire_at"]) > datetime.now(UTC)

    # A correct ticker now has nothing to do: the schedule is armed for a future tick.
    assert (await client.get("/v1/schedules/due")).json() == []
    for _ in range(3):
        assert await _tick(client) == []
    assert len(runs.runs) == 1


async def test_a_missed_tick_is_fired_once_rather_than_replayed_hour_by_hour(client, runs) -> None:
    """The outage case, with no bad input at all: whatever was down comes back and the
    schedule is hours overdue. It owes one run, not one run per hour it was unreachable."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    await _arm(client, sid, _last_tick() - timedelta(hours=6))

    assert len(await _tick(client)) == 1
    # Caught up in one fire, not six.
    assert await _tick(client) == []
    assert len(runs.runs) == 1


async def test_a_fire_for_an_instant_that_has_not_arrived_is_refused(client, runs) -> None:
    """The mirror of the rewind, and the quieter one: a future ``at`` used to be accepted,
    arm the schedule for the occurrence after it, and retire the schedule for years while
    every health field — enabled, consecutive_failures, last_error — still said it was
    fine."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="daily"))).json()
    sid = schedule["schedule_id"]
    future = (datetime.now(UTC) + timedelta(days=365 * 5)).isoformat()

    refused = await client.post(f"/v1/schedules/{sid}/fire", json={"at": future})
    assert refused.status_code == 422
    assert runs.calls == []

    untouched = (await client.get(f"/v1/schedules/{sid}")).json()
    assert untouched["next_fire_at"] == schedule["next_fire_at"]
    assert untouched["last_fired_at"] is None


async def test_a_fire_tolerates_a_ticker_whose_clock_is_a_little_ahead(client) -> None:
    """The bound is on typos and skewed backfills, not on a replica that read /due a second
    before this service's clock got there."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    just_ahead = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()

    fired = await client.post(
        f"/v1/schedules/{schedule['schedule_id']}/fire", json={"at": just_ahead}
    )
    assert fired.status_code == 200


async def test_an_update_that_changes_no_timing_field_keeps_the_pending_occurrence(
    client,
) -> None:
    """This is a PUT, so a client sending the whole representation back is the normal case.
    Rearming because "cadence" was *present* rather than *changed* meant renaming a schedule
    a minute before it was due silently threw the run away — and answered 200."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    due_at = await _make_due(client, sid)

    renamed = await client.put(
        f"/v1/schedules/{sid}",
        json={"name": "renamed nightly", "cadence": "hourly", "timezone": "UTC", "enabled": True},
    )
    assert renamed.status_code == 200
    assert datetime.fromisoformat(renamed.json()["next_fire_at"]) == due_at
    assert [s["schedule_id"] for s in (await client.get("/v1/schedules/due")).json()] == [sid]
    assert len(await _tick(client)) == 1


async def test_a_refusal_that_will_not_change_its_mind_pauses_the_schedule_at_once(
    client, runs
) -> None:
    """A 4xx from agent-runs — an agent that no longer exists, a revoked grant — is already
    permanent on the first try. Spending two more ticks proving it only delays the alert,
    and the normalized error has said which kind of failure this is all along."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    runs.refuse = "rejected"

    failed = await client.post(f"/v1/schedules/{sid}/fire")
    assert failed.status_code == 502
    assert failed.json()["detail"]["auto_paused"] is True

    paused = (await client.get(f"/v1/schedules/{sid}")).json()
    assert paused["enabled"] is False
    assert paused["consecutive_failures"] == 1
    assert paused["last_error"]["retryable"] is False


async def test_a_retryable_failure_backs_off_instead_of_retrying_on_the_tickers_clock(
    client, runs
) -> None:
    """Three strikes has to mean three tries spaced by the schedule's own policy. Handing
    the schedule straight back to the next poll made the retry interval the ticker's — so a
    dependency having a slow minute auto-paused everything pointed at it."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    await _make_due(client, sid)
    runs.refuse = "error"

    assert (await client.post(f"/v1/schedules/{sid}/fire")).status_code == 502

    backing_off = (await client.get(f"/v1/schedules/{sid}")).json()
    assert backing_off["enabled"] is True
    assert backing_off["retry_after"] is not None
    assert (await client.get("/v1/schedules/due")).json() == []

    later = (datetime.now(UTC) + timedelta(minutes=30)).isoformat()
    waited = (await client.get("/v1/schedules/due", params={"at": later})).json()
    assert [s["schedule_id"] for s in waited] == [sid]


async def test_resuming_keeps_an_occurrence_it_can_still_honour(client) -> None:
    """The fire a short outage swallowed is the one the human clicking resume came for.
    Rearming from ``now`` answered 200 and dropped it, and nothing anywhere recorded that a
    run the schedule owed had been skipped."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="daily"))).json()
    sid = schedule["schedule_id"]
    missed = await _arm(client, sid, datetime.now(UTC).replace(microsecond=0) - timedelta(hours=1))
    await client.post(f"/v1/schedules/{sid}/pause")

    resumed = (await client.post(f"/v1/schedules/{sid}/resume")).json()
    assert datetime.fromisoformat(resumed["next_fire_at"]) == missed
    assert len(await _tick(client)) == 1


async def test_resuming_still_looks_forward_past_an_occurrence_it_has_missed(client) -> None:
    """The other half of the same rule: an occurrence that a later one has already
    superseded is gone. Keeping it would put a month of hourly ticks back on the table."""
    schedule = (await client.post("/v1/schedules", json=scheduled(cadence="hourly"))).json()
    sid = schedule["schedule_id"]
    await _arm(client, sid, _last_tick() - timedelta(days=30))
    await client.post(f"/v1/schedules/{sid}/pause")

    resumed = (await client.post(f"/v1/schedules/{sid}/resume")).json()
    assert datetime.fromisoformat(resumed["next_fire_at"]) > datetime.now(UTC)


def _last_tick() -> datetime:
    """The most recent hourly boundary — an instant that has actually arrived.

    Fire times are relative to the clock rather than hard-coded, because ``at`` names a tick
    that has *come due*: a fixed calendar instant is in the past or the future depending on
    what day the suite runs, and one in the future is now refused.
    """
    return datetime.now(UTC).replace(minute=0, second=0, microsecond=0)


def _instant(attempt: int) -> str:
    """A distinct past fire time per attempt, so a retry is a new tick rather than the same
    one deduplicated away."""
    return (_last_tick() - timedelta(hours=attempt)).isoformat()


async def _make_due(client: AsyncClient, schedule_id: str) -> datetime:
    """Put the schedule into the state a ticker finds it in — overdue — without waiting an
    hour for the clock. Written straight to the row on purpose: no API moves next_fire_at
    backwards, and one that did would be a way to replay a tick."""
    return await _arm(
        client, schedule_id, (datetime.now(UTC) - timedelta(minutes=5)).replace(microsecond=0)
    )


async def _tick(client: AsyncClient) -> list[str]:
    """One iteration of the ticker loop the service documents: read /due, then fire each
    schedule with its own next_fire_at. Returns the runs that iteration created."""
    due = (await client.get("/v1/schedules/due")).json()
    fired = []
    for schedule in due:
        response = await client.post(
            f"/v1/schedules/{schedule['schedule_id']}/fire",
            json={"at": schedule["next_fire_at"]},
        )
        if response.status_code == 200:
            fired.append(response.json()["run_id"])
    return fired


async def _arm(client: AsyncClient, schedule_id: str, at: datetime) -> datetime:
    """Put a schedule's clock where a test needs it, straight on the row — no API moves
    next_fire_at backwards, which is the property several of these tests exist to keep."""
    async with client._transport.app.state.engine.begin() as conn:
        await conn.execute(
            text("UPDATE agent_schedules SET next_fire_at = :due WHERE schedule_id = :sid"),
            {"due": at, "sid": schedule_id},
        )
    return at
