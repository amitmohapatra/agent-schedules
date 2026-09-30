# agent-schedules (merged into agent-runs)

This repository no longer contains code. As of agent-runs 0.2.0, schedules live in
[agent-runs](../agent-runs): the `/v1/schedules` routes (create, list, get, patch, delete,
pause, resume, fire) and the `agent-runs-ticker` process that fires them.

## Why

A schedule does one thing: at its tick, it queues a run on behalf of the person who set it.
As a separate service it had to reach agent-runs over HTTP with an inter-service key, keep
its own database, and poll its own API from a separate ticker, all to write one row into
another service's table. Merged, a fire inserts the `QUEUED` run in the same transaction
that advances the schedule, idempotent on `(schedule_id, fire_time)`; there is no second
service, credential or network hop to fail, and one ticker also handles worker lease expiry
and interrupt escalation.

The behaviour carried over unchanged: cadences (named buckets or cron, at most hourly, in the
schedule's own time zone), `on_behalf_of` bound to the creating credential and never
editable, auto-pause after repeated failed fires with backoff between retries, and no replay
of missed ticks. See the agent-runs README and `docs/api.md`.

Existing schedule rows are not migrated automatically: recreate them through
`POST /v1/schedules` on agent-runs.
