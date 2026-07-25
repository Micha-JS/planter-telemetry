"""SimulatorSettings: defaults, env overrides, bounds."""

import pytest
from pydantic import ValidationError

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


def test_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIM_DEVICE_COUNT", "7")
    monkeypatch.setenv("SIM_SEED", "123")
    monkeypatch.setenv("SIM_ACCELERATION", "1")
    monkeypatch.setenv("SIM_MQTT_HOST", "mosquitto")
    monkeypatch.setenv("SIM_DUPLICATE_RATE", "0.5")
    settings = SimulatorSettings()
    assert settings.device_count == 7
    assert settings.seed == 123
    assert settings.acceleration == 1.0
    assert settings.mqtt_host == "mosquitto"
    assert settings.duplicate_rate == 0.5


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
