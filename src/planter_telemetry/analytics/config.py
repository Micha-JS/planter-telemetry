"""Analytics settings. All knobs documented in the README's Analytics section."""

from dataclasses import replace

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from planter_telemetry.analytics.model import BATTERY, WATER, Metric


class AnalyticsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ANALYTICS_")

    db_dsn: str = "postgresql://planter:planter@localhost:5433/planter"
    interval_seconds: float = Field(default=30.0, gt=0)
    water_target_pct: float = Field(default=10.0, ge=0, le=100)
    battery_target_volts: float = Field(default=3.4, ge=2.5, le=4.4)
    water_alert_days: float = Field(default=2.0, gt=0)
    battery_alert_days: float = Field(default=3.0, gt=0)
    alert_cooldown_hours: float = Field(default=24.0, gt=0)
    # Empty string = notifications disabled (the safe default: a fresh clone
    # must never post to anyone's ntfy topic). The URL is a write capability;
    # nothing in this service logs or stores it.
    ntfy_url: str = ""
    ops_host: str = "127.0.0.1"
    ops_port: int = Field(default=8081, ge=0, le=65535)

    def metrics(self) -> tuple[Metric, ...]:
        """The pure core's per-kind constants with the configured overrides.

        dataclasses.replace is the intended extension seam: the core never
        sees settings, and the settings never grow model logic.
        """
        return (
            replace(
                WATER,
                target=self.water_target_pct,
                alert_horizon_seconds=self.water_alert_days * 86400.0,
            ),
            replace(
                BATTERY,
                target=self.battery_target_volts,
                alert_horizon_seconds=self.battery_alert_days * 86400.0,
            ),
        )
