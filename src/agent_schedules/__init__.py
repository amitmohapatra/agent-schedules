"""Schedules that fire agent runs on behalf of a user who is not present.

This is a separate service because it is the most privileged component in the stack: it
creates runs on behalf of users who are not there to approve them, so it must be able to
act as any user who owns a schedule. Isolating it bounds the blast radius of both a bug and
a compromise, and it makes the on-behalf-of grant auditable at a service boundary instead
of being an internal function call nobody reviews.

It owns schedule records and nothing else. Firing is exactly one outbound call —
``POST /v1/runs`` on agent-runs, idempotent on ``(schedule_id, fire_time)``. Run state
lives there. If this service ever reads or writes a run row, the split is in the wrong
place.
"""
