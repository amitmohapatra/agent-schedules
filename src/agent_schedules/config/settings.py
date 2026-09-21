"""Configuration. Everything has a default that works on a laptop; nothing has a default
that is wrong in production.

One prefix, ``SCHEDULES__``, with ``__`` between levels — the same shape agent-runs and the
Memory Service use, so an operator learns it once.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

#: The authentication modes this service implements, as a closed set.
#:
#: It used to be a free-form string, and that was a fail-open: the credential check was
#: written as ``if auth_mode == "trusted_dev" and key not in api_keys``, so *every other
#: value* — including the ones the startup check pushes operators toward outside dev, and
#: including a one-character typo — turned authentication off entirely and left the
#: ``X-Tenant-Id`` header as the only "credential". A closed set turns a misspelling into a
#: startup failure instead of a silently unauthenticated deployment.
AuthMode = Literal["api_key", "trusted_dev"]


class Credential(BaseModel):
    """What one API key is allowed to be.

    A key here is not a password that unlocks the service; it *is* the caller. It names the
    tenant it speaks for, the principal it acts as, and the principals it may schedule runs
    on behalf of. All three are bound to the secret on purpose: ``X-Tenant-Id`` and
    ``on_behalf_of`` arrive in attacker-controlled bytes, so neither can be authority on its
    own. A flat list of keys with the tenant taken from a header means any valid key can act
    as any tenant, which is not authentication, only a doorbell.
    """

    model_config = ConfigDict(extra="forbid")

    #: The one tenant this credential speaks for.
    tenant_id: str
    #: Who this credential is. Recorded as ``created_by`` on everything it creates, which is
    #: the half of the audit sentence a self-asserted field could never provide.
    principal: str
    #: Principals it may schedule runs *as*, beyond itself. ``"*"`` is the service grant —
    #: a ticker has to be able to fire every schedule in its tenant, whoever owns it.
    may_act_as: list[str] = Field(default_factory=list)

    def may_schedule_for(self, principal: str) -> bool:
        """Whether this credential may make runs execute as ``principal``."""
        return (
            principal == self.principal
            or "*" in self.may_act_as
            or principal in self.may_act_as
        )


def _dev_credentials() -> dict[str, Credential]:
    """The laptop default: one key, one tenant, allowed to stand in for anyone locally."""
    return {"dev-key": Credential(tenant_id="dev", principal="dev-user", may_act_as=["*"])}


class DatabaseSettings(BaseModel):
    url: str = "postgresql+psycopg://memory:memory@localhost:5432/agent_schedules"
    pool_size: int = 10
    echo: bool = False


class ObservabilitySettings(BaseModel):
    #: OTLP export is off until an endpoint is given. A service that tries to export to
    #: nowhere spends its startup retrying a connection that will never succeed.
    otel_enabled: bool = False
    otel_endpoint: str | None = None
    log_level: str = "INFO"
    log_json: bool = True


class RunsSettings(BaseModel):
    """How to reach agent-runs — the only service this one talks to."""

    url: str = "http://localhost:8090"
    api_key: str = "dev-key"
    #: Short on purpose: a fire holds a row lock while this call is outstanding, so a slow
    #: agent-runs must become a recorded failure quickly rather than a stuck ticker.
    timeout_seconds: float = 10.0


class ServiceSettings(BaseModel):
    name: str = "agent-schedules"
    environment: str = "dev"
    #: Which credential table is in use. The key is verified in *every* mode; this only says
    #: whether the bundled dev credentials are the ones being trusted, and the startup check
    #: refuses those outside dev.
    auth_mode: AuthMode = "trusted_dev"
    api_keys: dict[str, Credential] = Field(default_factory=_dev_credentials)
    #: Consecutive failed fires before a schedule pauses itself. Three is "not a blip".
    max_consecutive_failures: int = 3
    #: How long a schedule waits after a *retryable* fire failure before it is due again.
    #: Without it the retry cadence is the ticker's, not the schedule's, and "three strikes"
    #: is three ticks — three minutes on a 60s ticker, which a slow dependency clears easily.
    #: Doubles per consecutive failure.
    retry_backoff_seconds: float = 300.0


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SCHEDULES__", env_nested_delimiter="__", env_file=".env", extra="ignore"
    )

    service: ServiceSettings = ServiceSettings()
    database: DatabaseSettings = DatabaseSettings()
    observability: ObservabilitySettings = ObservabilitySettings()
    runs: RunsSettings = RunsSettings()

    def check(self) -> None:
        """Refuse configurations that only look like they work."""
        if self.service.environment != "dev" and self.service.auth_mode == "trusted_dev":
            raise ValueError(
                "service.auth_mode='trusted_dev' is only allowed when environment='dev': "
                "outside dev it means the credentials in the repo are the live ones"
            )
        if not self.service.api_keys:
            # Fail closed rather than open — but say so, because an empty table answers
            # every request with 401 and that looks like a broken deployment, not a policy.
            raise ValueError(
                "service.api_keys is empty: this service authenticates every request, so "
                "a deployment without credentials can serve nobody"
            )
        shipped = set(self.service.api_keys) & set(_dev_credentials())
        if self.service.environment != "dev" and shipped:
            raise ValueError(
                f"service.api_keys still contains the dev credential(s) {sorted(shipped)}: "
                "a key that is published in the repo authenticates the whole internet"
            )
        if self.service.environment != "dev" and self.runs.api_key == RunsSettings().api_key:
            # This service can create runs as any user who owns a schedule. Shipping it with
            # the shared dev credential hands that reach to anyone who has read the repo.
            raise ValueError(
                "runs.api_key is still the dev default outside dev: the credential that "
                "lets this service act on behalf of users must not be the one in the README"
            )
        if self.service.max_consecutive_failures < 1:
            raise ValueError("service.max_consecutive_failures must be at least 1")
        if self.service.retry_backoff_seconds <= 0:
            raise ValueError("service.retry_backoff_seconds must be positive")
        if self.observability.otel_enabled and not self.observability.otel_endpoint:
            raise ValueError("observability.otel_enabled requires observability.otel_endpoint")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.check()
    return settings


def reset_settings_cache() -> None:
    get_settings.cache_clear()
