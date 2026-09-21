"""Structured logging with request-scoped context.

structlog was already a declared dependency and ObservabilitySettings.log_level and
log_json were already settings — but nothing read them, so the service emitted uvicorn's
default text logs and the configuration was decoration. This module is what makes those two
settings mean something.

Context (tenant, run, schedule, trace) is bound per request through a :class:`ContextVar`
rather than threaded through every call site, so a log line deep in the store still carries
the tenant that asked for it.
"""

from __future__ import annotations

import logging
import sys
from contextvars import ContextVar
from typing import Any

import structlog

_log_context: ContextVar[dict[str, str] | None] = ContextVar("schedules_log_context", default=None)


def bind_log_context(**fields: str) -> None:
    """Add fields to every log line emitted for the rest of this request."""
    current = dict(_log_context.get() or {})
    current.update({k: v for k, v in fields.items() if v})
    _log_context.set(current)


def clear_log_context() -> None:
    _log_context.set(None)


def _inject_context(_: Any, __: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    for key, value in (_log_context.get() or {}).items():
        event_dict.setdefault(key, value)
    return event_dict


def configure_logging(level: str = "INFO", json_output: bool = True) -> None:
    """Install the structlog pipeline and route stdlib logging through the same stream.

    Called once at startup. json_output=False gives the human-readable console renderer,
    which is what a developer wants on a laptop and what nothing should use in production.
    """
    shared: list[Any] = [
        structlog.contextvars.merge_contextvars,
        _inject_context,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]
    if json_output:
        shared.append(structlog.processors.format_exc_info)
    renderer: Any = (
        structlog.processors.JSONRenderer() if json_output else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[*shared, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
        cache_logger_on_first_use=False,
    )
    logging.basicConfig(level=level.upper(), stream=sys.stdout, format="%(message)s", force=True)
    for noisy in ("uvicorn.access", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
