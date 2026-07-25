"""Simulator configuration from SIM_-prefixed environment variables."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SimulatorSettings(BaseSettings):
    """All knobs documented in the README's Simulator section."""

    model_config = SettingsConfigDict(env_prefix="SIM_")

    device_count: int = Field(default=4, ge=1)
    seed: int = 42
    # Virtual seconds that elapse per wall-clock second. 180 → a virtual day
    # every 8 wall minutes, so depletion/refills are visible in a short demo.
    acceleration: float = Field(default=180.0, gt=0)
    # Deep-sleep cadence between wake-ups, in virtual seconds.
    interval_seconds: float = Field(default=300.0, gt=0)
    mqtt_host: str = "localhost"
    mqtt_port: int = Field(default=1883, ge=1, le=65535)
    duplicate_rate: float = Field(default=0.05, ge=0, le=1)
    out_of_order_rate: float = Field(default=0.03, ge=0, le=1)
    malformed_rate: float = Field(default=0.02, ge=0, le=1)
    missed_checkin_rate: float = Field(default=0.03, ge=0, le=1)
