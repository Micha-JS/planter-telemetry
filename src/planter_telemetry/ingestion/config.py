"""Ingestion configuration from INGEST_-prefixed environment variables."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestSettings(BaseSettings):
    """All knobs documented in the README's Ingestion section."""

    model_config = SettingsConfigDict(env_prefix="INGEST_")

    mqtt_host: str = "localhost"
    mqtt_port: int = Field(default=1883, ge=1, le=65535)
    # Stable client id: the broker keeps the QoS 1 session (subscriptions and
    # queued messages) across ingestion restarts. See docs/ingestion.md.
    client_id: str = "planter-ingestion"
    # 5433: the compose file publishes TimescaleDB on host port 5433 (5432 is
    # a popular port for an unrelated local Postgres to be squatting on), so
    # the default works for host-side runs against the compose stack. Inside
    # compose the DSN is set explicitly to timescaledb:5432.
    db_dsn: str = "postgresql://planter:planter@localhost:5433/planter"
    reconnect_initial_seconds: float = Field(default=1.0, gt=0)
    reconnect_max_seconds: float = Field(default=30.0, gt=0)
    stats_interval_seconds: float = Field(default=30.0, gt=0)
    # /healthz + /metrics. Loopback by default: the compose healthcheck runs
    # inside the container, so nothing needs to be published to the host.
    # Port 0 binds an ephemeral port (used by tests to avoid collisions).
    ops_host: str = "127.0.0.1"
    ops_port: int = Field(default=8080, ge=0, le=65535)
