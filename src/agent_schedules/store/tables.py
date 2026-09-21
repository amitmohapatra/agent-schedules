"""The one table this service owns.

There is no run table here, and adding one would be the mistake this split exists to
prevent: run state lives in agent-runs, and a second copy of it would immediately disagree.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Index, Integer, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ScheduleRow(Base):
    __tablename__ = "agent_schedules"

    schedule_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False)
    agent_id: Mapped[str] = mapped_column(String(128), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    cadence: Mapped[str] = mapped_column(String(128), nullable=False)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False, default="UTC")
    input: Mapped[dict | None] = mapped_column(JSONB)
    # NOT NULL is the point. A schedule with no identity is a run nobody authorized, and the
    # column refuses it even on the day a code path forgets to.
    on_behalf_of: Mapped[str] = mapped_column(String(128), nullable=False)
    created_by: Mapped[str | None] = mapped_column(String(128))
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    next_fire_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_fired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_run_id: Mapped[str | None] = mapped_column(String(64))
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[dict | None] = mapped_column(JSONB)
    # The backoff gate after a retryable fire failure. Separate from next_fire_at because
    # the two answer different questions: next_fire_at is *which tick* is owed (moving it
    # would change the idempotency key and turn a retry into a second run), retry_after is
    # *when this service will try again*.
    retry_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    schedule_metadata: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        # The only thing standing between a retried create and two schedules that both fire
        # forever. Scoped to the tenant: two tenants naming a schedule "nightly" is normal.
        UniqueConstraint("tenant_id", "name", name="uq_schedules_tenant_name"),
        # The ticker's query, which runs every tick: enabled schedules already due.
        Index("ix_schedules_due", "tenant_id", "enabled", "next_fire_at"),
        Index("ix_schedules_tenant_agent", "tenant_id", "agent_id"),
    )
