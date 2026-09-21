"""Application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import structlog
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from agent_schedules.api.deps import Session
from agent_schedules.api.routers import schedules
from agent_schedules.clients.runs import RunsClient
from agent_schedules.config.settings import Settings, get_settings
from agent_schedules.store.tables import Base

log = structlog.get_logger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(settings.database.url, echo=settings.database.echo)
        async with engine.begin() as conn:
            # One table, owned entirely by this service: a migration tool would be ceremony.
            # That judgement changes the day a second table appears.
            await conn.run_sync(Base.metadata.create_all)
            # create_all creates a *missing* table and then leaves it alone, so a column
            # added after this service first shipped never reaches an existing deployment —
            # it comes back as "column does not exist" on the ticker's next query instead.
            # Additive and idempotent, which is the whole of what one table has ever needed.
            await conn.execute(
                text(
                    "ALTER TABLE agent_schedules "
                    "ADD COLUMN IF NOT EXISTS retry_after TIMESTAMPTZ"
                )
            )
        http = httpx.AsyncClient(base_url=settings.runs.url, timeout=settings.runs.timeout_seconds)
        app.state.engine = engine
        app.state.sessions = async_sessionmaker(engine, expire_on_commit=False)
        app.state.runs = RunsClient(http, api_key=settings.runs.api_key)
        log.info(
            "agent_schedules.started",
            environment=settings.service.environment,
            runs_url=settings.runs.url,
        )
        try:
            yield
        finally:
            await http.aclose()
            await engine.dispose()
            log.info("agent_schedules.stopped")

    app = FastAPI(title="Agent Schedules", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    app.include_router(schedules.router)

    @app.get("/health/live", tags=["ops"])
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", tags=["ops"])
    async def ready(db: Session) -> dict[str, str]:
        """Ready means the database answers.

        Deliberately *not* a reachability check on agent-runs: this service can still accept,
        list and pause schedules while agent-runs is down, and a readiness probe that fails
        in sympathy would pull a healthy service out of rotation for someone else's outage.
        """
        await db.execute(text("SELECT 1"))
        return {"status": "ok"}

    return app
