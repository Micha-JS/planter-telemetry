"""Settings classes: defaults, env overrides, bounds."""

from pathlib import Path

import pytest
from pydantic import ValidationError

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
