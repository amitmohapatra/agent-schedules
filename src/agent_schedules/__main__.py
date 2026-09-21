"""Console entry point."""

from __future__ import annotations

import uvicorn

from agent_schedules.api.app import create_app
from agent_schedules.config.settings import get_settings


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        create_app(settings),
        host=settings.service.host,
        port=settings.service.port,
        log_config=None,  # the service configures its own structured logging
    )


if __name__ == "__main__":
    main()
