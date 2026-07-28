"""Settings classes: defaults, env overrides, bounds."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from planter_telemetry.analytics.config import AnalyticsSettings
from planter_telemetry.ingestion.config import IngestSettings
from planter_telemetry.simulator.config import SimulatorSettings


def test_defaults() -> None:
    settings = SimulatorSettings()
    assert settings.device_count == 4
    assert settings.seed == 42
    assert settings.acceleration == 180.0
    assert settings.interval_seconds == 300.0
    assert settings.mqtt_host == "localhost"
    assert settings.mqtt_port == 1883
    assert settings.duplicate_rate == 0.05
    assert settings.out_of_order_rate == 0.03
    assert settings.malformed_rate == 0.02
    assert settings.missed_checkin_rate == 0.03
    assert settings.heartbeat_path == Path("/tmp/planter-simulator-heartbeat")


def test_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIM_DEVICE_COUNT", "7")
    monkeypatch.setenv("SIM_SEED", "123")
    monkeypatch.setenv("SIM_ACCELERATION", "1")
    monkeypatch.setenv("SIM_MQTT_HOST", "mosquitto")
    monkeypatch.setenv("SIM_DUPLICATE_RATE", "0.5")
    monkeypatch.setenv("SIM_HEARTBEAT_PATH", "/run/beat")
    settings = SimulatorSettings()
    assert settings.device_count == 7
    assert settings.seed == 123
    assert settings.acceleration == 1.0
    assert settings.mqtt_host == "mosquitto"
    assert settings.duplicate_rate == 0.5
    assert settings.heartbeat_path == Path("/run/beat")


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("SIM_DEVICE_COUNT", "0"),
        ("SIM_ACCELERATION", "0"),
        ("SIM_INTERVAL_SECONDS", "-1"),
        ("SIM_DUPLICATE_RATE", "1.5"),
        ("SIM_MALFORMED_RATE", "-0.1"),
        ("SIM_MQTT_PORT", "70000"),
    ],
)
def test_out_of_range_rejected(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        SimulatorSettings()


def test_ingest_ops_defaults() -> None:
    settings = IngestSettings()
    assert settings.ops_host == "127.0.0.1"
    assert settings.ops_port == 8080


def test_ingest_ops_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_OPS_HOST", "0.0.0.0")
    monkeypatch.setenv("INGEST_OPS_PORT", "0")  # 0 = ephemeral, explicitly allowed
    settings = IngestSettings()
    assert settings.ops_host == "0.0.0.0"
    assert settings.ops_port == 0


@pytest.mark.parametrize("value", ["70000", "-1"])
def test_ingest_ops_port_out_of_range_rejected(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("INGEST_OPS_PORT", value)
    with pytest.raises(ValidationError):
        IngestSettings()


def test_analytics_defaults() -> None:
    settings = AnalyticsSettings()
    assert settings.db_dsn == "postgresql://planter:planter@localhost:5433/planter"
    assert settings.interval_seconds == 30.0
    assert settings.water_target_pct == 10.0
    assert settings.battery_target_volts == 3.4
    assert settings.water_alert_days == 2.0
    assert settings.battery_alert_days == 3.0
    assert settings.alert_cooldown_hours == 24.0
    assert settings.ntfy_url == ""  # notifications OFF by default: safe clone
    assert settings.ops_host == "127.0.0.1"
    assert settings.ops_port == 8081


def test_analytics_metrics_carry_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    """settings.metrics() is the config->core seam: targets and alert
    horizons land in the Metric constants without the core seeing settings."""
    monkeypatch.setenv("ANALYTICS_WATER_TARGET_PCT", "25.0")
    monkeypatch.setenv("ANALYTICS_BATTERY_ALERT_DAYS", "5.0")
    water, battery = AnalyticsSettings().metrics()
    assert water.kind == "water"
    assert water.target == 25.0
    assert water.alert_horizon_seconds == 2.0 * 86400.0
    assert battery.kind == "battery"
    assert battery.target == 3.4
    assert battery.alert_horizon_seconds == 5.0 * 86400.0


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("ANALYTICS_INTERVAL_SECONDS", "0"),
        ("ANALYTICS_WATER_TARGET_PCT", "101"),
        ("ANALYTICS_BATTERY_TARGET_VOLTS", "5.0"),
        ("ANALYTICS_WATER_ALERT_DAYS", "0"),
        ("ANALYTICS_OPS_PORT", "70000"),
    ],
)
def test_analytics_out_of_range_rejected(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        AnalyticsSettings()
