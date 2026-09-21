"""The only thing this service says to agent-runs.

One call, one direction. There is no read path back: this service never asks what happened
to a run, because the moment it did, it would want to store the answer and the split would
be gone.
"""

from __future__ import annotations

from datetime import datetime

import httpx
import structlog
from universal_agent_contracts.errors import AgentError, ErrorCategory

from agent_schedules.domain.models import Schedule

log = structlog.get_logger(__name__)

_BAD_REQUEST = 400
_SERVER_ERROR = 500
#: Enough of the upstream body to diagnose from, bounded so a stack trace cannot become a
#: multi-megabyte JSONB column on every tick.
_ERROR_EXCERPT = 500


class RunsUnavailable(Exception):
    """agent-runs did not create the run.

    Carries a normalized :class:`AgentError` because the failure is stored, not just logged:
    "the fire failed" without "and here is what it said" is an alert nobody can action.
    """

    def __init__(self, error: AgentError) -> None:
        super().__init__(error.message or error.code)
        self.error = error


class RunsClient:
    """Creates runs in agent-runs on behalf of a schedule's owner."""

    def __init__(self, http: httpx.AsyncClient, *, api_key: str) -> None:
        self._http = http
        self._api_key = api_key

    async def create_run(
        self, schedule: Schedule, *, fire_time: datetime, idempotency_key: str
    ) -> str:
        """Create the run for one fire and return its id.

        Takes the whole :class:`Schedule` rather than loose fields on purpose: every
        privileged value in the request body — the tenant, the agent, above all
        ``on_behalf_of`` — is then read from the stored row by construction, and no caller
        can pass its own. That is the widening bug made unwritable rather than merely
        forbidden.

        Raises :class:`RunsUnavailable` for anything other than a run coming back. It never
        returns a sentinel: a fire that silently did nothing is the one outcome a scheduler
        must not have.
        """
        body = {
            "tenant_id": schedule.tenant_id,
            "agent_id": schedule.agent_id,
            "on_behalf_of": schedule.on_behalf_of,
            "input": schedule.input,
            "idempotency_key": idempotency_key,
            "metadata": {
                "schedule_id": schedule.schedule_id,
                "schedule_name": schedule.name,
                "fire_time": fire_time.isoformat(),
                "created_by": schedule.created_by,
            },
        }
        try:
            response = await self._http.post(
                "/v1/runs",
                json=body,
                headers={"X-Api-Key": self._api_key, "X-Tenant-Id": schedule.tenant_id},
            )
        except httpx.HTTPError as exc:
            raise RunsUnavailable(
                AgentError.of(exc, category=ErrorCategory.DEPENDENCY, source="agent-runs")
            ) from exc

        if response.status_code >= _BAD_REQUEST:
            raise RunsUnavailable(
                AgentError(
                    code=f"runs_http_{response.status_code}",
                    category=ErrorCategory.DEPENDENCY,
                    message=response.text[:_ERROR_EXCERPT],
                    # A 4xx is the schedule's own fault (unknown agent, revoked key) and
                    # retrying it just burns ticks; a 5xx is worth another tick.
                    retryable=response.status_code >= _SERVER_ERROR,
                    source="agent-runs",
                )
            )

        try:
            payload = response.json()
        except ValueError:
            # A captive portal or proxy answering 200 with HTML is not a created run, and
            # treating a decode error as one would lose the fire silently.
            payload = None
        run_id = payload.get("run_id") if isinstance(payload, dict) else None
        if not run_id:
            raise RunsUnavailable(
                AgentError(
                    code="runs_response_without_run_id",
                    category=ErrorCategory.DEPENDENCY,
                    message=f"agent-runs answered {response.status_code} with no run_id",
                    source="agent-runs",
                )
            )
        log.info("schedule.run_created", schedule_id=schedule.schedule_id, run_id=run_id)
        return str(run_id)
