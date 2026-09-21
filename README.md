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

## Run it

```bash
uv sync
uv run uvicorn agent_schedules.api.app:app --port 8096
```

## Tests

```bash
uv run pytest
```
