"""Cadence arithmetic, with the reference time supplied by the test.

These are the cases a running service cannot be asked about: what a daily schedule does on
the night a country changes its clocks, and what happens when someone asks for a schedule
that fires every minute. None of them are reachable by waiting.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from agent_schedules.domain.cadence import (
    InvalidCadence,
    InvalidTimezone,
    next_fire_at,
    validate_cadence,
    validate_timezone,
)
from agent_schedules.domain.models import idempotency_key

NY = "America/New_York"
_REFERENCE = datetime(2026, 9, 21, 14, 37, tzinfo=UTC)  # a Monday afternoon


def test_hourly_fires_on_the_next_top_of_the_hour() -> None:
    """The bucket is the top of the hour, not "an hour from whenever you asked" — otherwise
    every restart quietly walks the schedule forward by a few minutes."""
    assert next_fire_at("hourly", after=_REFERENCE, timezone="UTC") == datetime(
        2026, 9, 21, 15, tzinfo=UTC
    )


def test_daily_fires_at_local_midnight_not_utc_midnight() -> None:
    """The failure this prevents: a Berlin user's "daily" arriving at 02:00 local because
    the computation was done in UTC and the zone was decoration."""
    assert next_fire_at("daily", after=_REFERENCE, timezone="Europe/Berlin") == datetime(
        2026, 9, 21, 22, tzinfo=UTC
    )


def test_weekly_fires_on_the_next_monday() -> None:
    assert next_fire_at("weekly", after=_REFERENCE, timezone="UTC") == datetime(
        2026, 9, 28, tzinfo=UTC
    )


def test_weekdays_skips_the_weekend() -> None:
    """From Friday the next weekday fire is Monday. A cadence that counted days would land
    on Saturday and deliver a Monday-morning briefing to nobody."""
    friday = datetime(2026, 9, 25, 12, tzinfo=UTC)
    assert next_fire_at("weekdays", after=friday, timezone="UTC") == datetime(
        2026, 9, 28, tzinfo=UTC
    )


def test_a_manual_schedule_never_fires_on_its_own() -> None:
    """``manual`` has no next instant at all, so it can never appear in the due list."""
    assert next_fire_at("manual", after=_REFERENCE, timezone="UTC") is None


def test_a_cron_cadence_is_honoured_in_the_schedules_own_zone() -> None:
    """14:37 UTC is already 10:37 in New York, so today's 09:00 is gone and the answer is
    tomorrow. Computing in UTC would have said "today at 09:00" — four hours in the past."""
    assert next_fire_at("0 9 * * 1-5", after=_REFERENCE, timezone=NY) == datetime(
        2026, 9, 22, 13, tzinfo=UTC
    )


def test_daily_keeps_local_midnight_across_the_spring_dst_transition() -> None:
    """The night the US springs forward, consecutive daily fires are 23 hours apart in UTC.

    That gap is the whole point: the wall clock the user asked for stays 00:00, so the
    absolute instant has to move. A service that stored the interval instead of the rule
    would fire this one at 01:00 local and drift every March."""
    first = next_fire_at("daily", after=datetime(2026, 3, 7, 12, tzinfo=UTC), timezone=NY)
    assert first is not None
    second = next_fire_at("daily", after=first, timezone=NY)
    assert second is not None

    local = ZoneInfo(NY)
    assert first.astimezone(local).hour == 0
    assert second.astimezone(local).hour == 0
    assert second - first == timedelta(hours=23)


def test_daily_keeps_local_midnight_across_the_autumn_dst_transition() -> None:
    """The mirror case: falling back makes the same two fires 25 hours apart."""
    first = next_fire_at("daily", after=datetime(2026, 10, 31, 12, tzinfo=UTC), timezone=NY)
    assert first is not None
    second = next_fire_at("daily", after=first, timezone=NY)
    assert second is not None

    local = ZoneInfo(NY)
    assert (first.astimezone(local).hour, second.astimezone(local).hour) == (0, 0)
    assert second - first == timedelta(hours=25)


def test_hourly_skips_the_local_hour_that_does_not_exist() -> None:
    """02:00 never happens on the spring-forward morning. The fire after 01:00 EST is 03:00
    EDT — one real hour later — and not a crash on a wall-clock time with no instant."""
    one_am = next_fire_at("hourly", after=datetime(2026, 3, 8, 5, 30, tzinfo=UTC), timezone=NY)
    assert one_am == datetime(2026, 3, 8, 6, tzinfo=UTC)
    following = next_fire_at("hourly", after=one_am, timezone=NY)
    assert following == datetime(2026, 3, 8, 7, tzinfo=UTC)
    assert following.astimezone(ZoneInfo(NY)).hour == 3


@pytest.mark.parametrize("cadence", ["* * * * *", "*/5 * * * *", "0,30 * * * *", "0 * * * * *"])
def test_a_cadence_faster_than_once_an_hour_is_refused(cadence: str) -> None:
    """Including the six-field spellings with a seconds column, which are valid cron and
    would sail past any check that only read the minute field."""
    with pytest.raises(InvalidCadence, match="per-minute"):
        validate_cadence(cadence)


def test_an_hourly_cron_is_accepted() -> None:
    """The floor is one run per hour, not "no cron" — exactly at the floor still passes."""
    assert validate_cadence("0 * * * *") == "0 * * * *"


def test_a_cadence_that_is_neither_bucket_nor_cron_is_refused() -> None:
    with pytest.raises(InvalidCadence, match="neither"):
        validate_cadence("every so often")


def test_an_unknown_timezone_is_refused_at_write_time() -> None:
    """Refused here rather than at fire time: a zone nobody can load should break the
    caller's request, not the ticker at 3am."""
    with pytest.raises(InvalidTimezone):
        validate_timezone("Mars/Olympus_Mons")


def test_a_naive_reference_time_is_refused() -> None:
    """A naive instant would be read as UTC and shift every fire by the caller's offset."""
    with pytest.raises(ValueError, match="timezone-aware"):
        next_fire_at("daily", after=datetime(2026, 9, 21, 14, 37), timezone="UTC")


def test_one_instant_spelled_two_ways_is_one_idempotency_key() -> None:
    """A ticker in Berlin and a ticker in UTC name the same tick differently; if that
    produced two keys, agent-runs would create two runs for it."""
    utc = datetime(2026, 9, 21, 9, tzinfo=UTC)
    berlin = utc.astimezone(ZoneInfo("Europe/Berlin"))
    assert idempotency_key("sch_1", utc) == idempotency_key("sch_1", berlin)
