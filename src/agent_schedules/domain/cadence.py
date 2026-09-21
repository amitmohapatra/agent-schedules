"""How often a schedule fires, and when it next does.

Granularity is capped at one run per hour on purpose. ChatGPT Tasks and Claude Cowork draw
the line in the same place and for the same reason: a schedule is an unattended loop, and a
per-minute unattended loop is a slow denial-of-service against whatever it calls, paid for
by someone who is not watching.

Nothing in this module reads the clock. Every entry point takes its reference time as a
parameter, because a next-fire computation that calls ``datetime.now()`` can only be tested
by waiting, and the cases worth testing — a DST transition, a schedule created at 23:59 —
are exactly the ones you cannot wait for.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from itertools import pairwise
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

#: The coarse buckets, and the cron expression each one means. ``manual`` maps to nothing:
#: it never fires on its own, it only answers an explicit POST /fire.
#:
#: The buckets fire at local midnight (and weekly on Monday) rather than at some invented
#: "nice" hour. A schedule that needs a particular time of day says so with a cron string —
#: that is what cron support is for, and inventing 09:00 here would silently disagree with
#: whatever the caller assumed.
BUCKETS: dict[str, str | None] = {
    "hourly": "0 * * * *",
    "daily": "0 0 * * *",
    "weekly": "0 0 * * 1",
    "weekdays": "0 0 * * 1-5",
    "manual": None,
}

#: The floor. One run per hour, matched to the products this mirrors.
MIN_INTERVAL = timedelta(hours=1)

#: Validation probes real occurrences from a fixed UTC instant so that whether a cadence is
#: accepted never depends on when (or where) the check happens to run.
_PROBE_FROM = datetime(2024, 1, 1, tzinfo=UTC)
_PROBE_COUNT = 8


class InvalidCadence(ValueError):
    """A cadence this service refuses to run."""


class InvalidTimezone(ValueError):
    """A timezone name the host's tz database does not know."""


def validate_cadence(cadence: str) -> str:
    """Return the normalized cadence, or raise :class:`InvalidCadence`.

    The contract: a bucket name, or a cron expression that fires at most once an hour.

    The trap: ``croniter`` accepts ``* * * * *`` and six-field expressions carrying a
    seconds column, so "is this too frequent" cannot be answered by reading the minute
    field. It is answered by measuring the gap between actual consecutive occurrences.
    """
    normalized = cadence.strip()
    if normalized in BUCKETS:
        return normalized
    if not croniter.is_valid(normalized):
        raise InvalidCadence(
            f"cadence {cadence!r} is neither one of {sorted(BUCKETS)} nor a cron expression"
        )
    interval = _shortest_interval(normalized)
    if interval < MIN_INTERVAL:
        raise InvalidCadence(
            f"cadence {cadence!r} fires every {interval}, under the one-run-per-hour floor "
            f"({MIN_INTERVAL}): per-minute schedules are refused outright, because an "
            "unattended loop firing faster than that is a retry storm nobody is watching."
        )
    return normalized


def validate_timezone(name: str) -> str:
    """Return ``name`` if this host can resolve it, else raise :class:`InvalidTimezone`.

    Checked at write time, not at fire time: a schedule stored with a zone nobody can load
    is a schedule that throws in the ticker at 3am instead of in the caller's 4xx.
    """
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise InvalidTimezone(f"unknown timezone {name!r}") from exc
    return name


def next_fire_at(cadence: str, *, after: datetime, timezone: str) -> datetime | None:
    """The first instant ``cadence`` fires strictly after ``after``, in UTC.

    ``None`` means "never on its own" — the ``manual`` bucket.

    ``after`` must be timezone-aware; a naive one is a bug that would otherwise be read as
    UTC and quietly shift every fire by the caller's offset.

    Computed in the schedule's own zone and persisted as UTC. Both halves matter: "daily"
    for a user in Berlin means local midnight on both sides of a DST transition (so the UTC
    instant has to move), while the due query sorts and compares instants (so what is
    stored has to be absolute).
    """
    if after.tzinfo is None:
        raise ValueError("next_fire_at needs a timezone-aware reference time")
    expression = BUCKETS.get(cadence, cadence)
    if expression is None:
        return None
    local = after.astimezone(ZoneInfo(timezone))
    return croniter(expression, local).get_next(datetime).astimezone(UTC)


def _shortest_interval(expression: str) -> timedelta:
    cursor = croniter(expression, _PROBE_FROM)
    times = [cursor.get_next(datetime) for _ in range(_PROBE_COUNT)]
    return min(later - earlier for earlier, later in pairwise(times))


__all__ = [
    "BUCKETS",
    "MIN_INTERVAL",
    "InvalidCadence",
    "InvalidTimezone",
    "next_fire_at",
    "validate_cadence",
    "validate_timezone",
]
