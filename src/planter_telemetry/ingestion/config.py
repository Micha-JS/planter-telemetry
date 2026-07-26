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
    db_dsn: str = "postgresql://planter:planter@localhost:5432/planter"
    reconnect_initial_seconds: float = Field(default=1.0, gt=0)
    reconnect_max_seconds: float = Field(default=30.0, gt=0)
    stats_interval_seconds: float = Field(default=30.0, gt=0)
