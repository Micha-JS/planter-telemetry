"""Wire encoding and corruption modes."""

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from planter_telemetry.contract import TelemetryV1
from planter_telemetry.simulator.wire import CorruptionMode, corrupt, encode

READING = TelemetryV1(
    device_id="planter-00",
    measured_at=datetime(2026, 7, 25, 12, 0, 0, tzinfo=UTC),
    water_level=55.5,
    battery_voltage=3.8,
)


def test_encode_round_trips() -> None:
    assert TelemetryV1.model_validate_json(encode(READING)) == READING


def test_truncated_is_unparseable() -> None:
    damaged = corrupt(encode(READING), CorruptionMode.TRUNCATED)
    with pytest.raises(json.JSONDecodeError):
        json.loads(damaged)


def test_wrong_type_parses_but_fails_validation() -> None:
    damaged = corrupt(encode(READING), CorruptionMode.WRONG_TYPE)
    assert json.loads(damaged)["water_level"] == "high"
    with pytest.raises(ValidationError) as excinfo:
        TelemetryV1.model_validate_json(damaged)
    assert excinfo.value.errors()[0]["loc"] == ("water_level",)


def test_missing_field_parses_but_fails_validation() -> None:
    damaged = corrupt(encode(READING), CorruptionMode.MISSING_FIELD)
    assert "device_id" not in json.loads(damaged)
    with pytest.raises(ValidationError) as excinfo:
        TelemetryV1.model_validate_json(damaged)
    assert excinfo.value.errors()[0]["loc"] == ("device_id",)


def test_corruption_is_deterministic() -> None:
    payload = encode(READING)
    for mode in CorruptionMode:
        assert corrupt(payload, mode) == corrupt(payload, mode)
