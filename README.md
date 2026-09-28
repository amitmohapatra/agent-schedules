# agent-schedules

Schedules that fire agent runs on behalf of a user who is not present.

"Summarise my inbox every weekday at 8am" is a schedule; each firing is a run in
[agent-runs](https://github.com/amitmohapatra/agent-runs). Keeping the two apart means a
schedule can be edited, paused or deleted without touching the history of what it already
produced.

## Model

| Concept | Meaning |
| --- | --- |
| Schedule | The standing intent: cron expression, timezone, the agent to run, the payload |
| Due | A schedule whose next fire time has passed and which has not yet been claimed |
| Firing | One claim of a due schedule, which becomes one run |

Claiming is what makes multiple tickers safe: a due schedule is handed to exactly one
caller, and the next fire time advances in the same transaction.

## The wire

The `Scheduler` port in [`trellis-contracts`](https://github.com/amitmohapatra/agent-contracts)
(`create` · `get` · `list_for_tenant` · `set_enabled` · `delete`) is the seam a harness is written
against. **This service is the HTTP implementation of that seam; `trellis-harness` ships no client
for it** — the shipped `Scheduler` there is `TemporalScheduler` — so a deployment that runs this
service drives it from its own control plane and every firing arrives in the harness as an ordinary
run. That is a deliberate gap, not an omission: see the harness's `docs/limitations.md`.

Each firing creates a run in [agent-runs](https://github.com/amitmohapatra/agent-runs), which is the
wire the harness *does* speak (`POST /v1/runs`, `/transition`, `/resume`,
`GET /v1/runs?status=PAUSED`).

```mermaid
sequenceDiagram
  participant T as Ticker (one or many)
  participant S as agent-schedules
  participant R as agent-runs
  participant H as Harness worker
  Note over T,S: X-Api-Key fixes the tenant; X-Tenant-Id must agree with it or 403
  T->>S: GET /v1/schedules/due?at=…&limit=… — soonest first
  S-->>T: the due schedules, each with its own next_fire_at
  T->>S: POST /v1/schedules/{id}/fire {at: that next_fire_at}
  S->>S: claim: the same (schedule, at) is the same idempotency key
  S->>R: POST /v1/runs {agent_id, on_behalf_of, input, idempotency_key}
  R-->>S: 201 Run (or the run it already created for that key)
  S-->>T: FireResult {schedule_id, run_id, fire_time, idempotency_key, schedule}
  H->>R: executes the run, then transitions it
  Note over S,R: a schedule may be edited, paused or deleted<br/>without touching the history of what it already produced
```

| Route | Purpose | Worth knowing |
|---|---|---|
| `POST /v1/schedules` | create one, armed for its next occurrence | `on_behalf_of` is **required** and is checked against the credential here, at the one moment it is ever accepted; `created_by` comes from the credential and sending one is a `422` |
| `GET /v1/schedules` | this tenant's schedules, newest first | filters: `enabled`, `agent_id`; `limit` 1–500 |
| `GET /v1/schedules/due` | what is due at an instant, soonest first | `at` defaults to now; declared before `/{schedule_id}` on purpose, or every request for the due list would look up a schedule called "due" |
| `GET /v1/schedules/{id}` | one schedule | |
| `PUT /v1/schedules/{id}` | edit it | `on_behalf_of` is **not** editable: a caller who could repoint someone else's schedule would have everything editing the identity would have given them |
| `DELETE /v1/schedules/{id}` | remove it | `204`; the runs it already produced stay in agent-runs |
| `POST /v1/schedules/{id}/pause` · `/resume` | stop firing, keep the record · start again from the next occurrence it can still honour | |
| `POST /v1/schedules/{id}/fire` | fire now — the manual trigger, and what a ticker calls | repeating it for the **same** instant is harmless; for a *different* instant it is a second run, which is why a ticker passes `at` |

Two rules that make multiple tickers safe:

* **claiming advances the clock in the same transaction.** A due schedule is handed to exactly one
  caller and `next_fire_at` moves with it, so two tickers on one tick produce one run.
* **the idempotency key is `(schedule_id, fire_time)`**, and agent-runs is keyed on it too — so a
  retried fire returns the run that already exists rather than creating a twin. `at` is bounded a
  couple of seconds ahead of now; anything further is refused, because a local-time-as-UTC instant
  or a skewed replica would otherwise rewrite `next_fire_at` past the mistake.

## Cadence

A bucket name — `hourly`, `daily`, `weekly`, `weekdays`, `manual` — or a cron expression that fires
**at most once an hour**. The floor is measured from real consecutive occurrences rather than read
off the minute field, because `croniter` accepts `* * * * *` and six-field expressions with a
seconds column: an unattended loop firing faster than hourly is a retry storm nobody is watching.
`timezone` is an IANA name the host's tz database knows, and validation probes occurrences from a
fixed UTC instant so that whether a cadence is accepted never depends on when the check runs.

## Run it

```bash
uv sync
uv run uvicorn agent_schedules.api.app:app --port 8096
```

## Tests

```bash
uv run pytest
```
