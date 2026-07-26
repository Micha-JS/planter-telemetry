"""Contract tests: JSON round-trip and validation rejection."""

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from planter_telemetry.contract import (
    TELEMETRY_TOPIC_FILTER,
    TelemetryV1,
    device_id_from_topic,
    telemetry_topic,
)


def _reading(**overrides: object) -> TelemetryV1:
    fields: dict[str, object] = {
        "device_id": "planter-00",
        "measured_at": datetime(2026, 7, 25, 12, 34, 56, 789012, tzinfo=UTC),
        "water_level": 73.4,
        "battery_voltage": 3.91,
    }
    fields.update(overrides)
    return TelemetryV1.model_validate(fields)


def test_json_round_trip_is_exact() -> None:
    original = _reading()
    restored = TelemetryV1.model_validate_json(original.model_dump_json())
    assert restored == original
    assert restored.measured_at.microsecond == 789012


def test_naive_datetime_rejected() -> None:
    with pytest.raises(ValidationError, match="timezone"):
        _reading(measured_at=datetime(2026, 7, 25, 12, 0, 0))


def test_naive_datetime_rejected_in_json() -> None:
    payload = _reading().model_dump_json().replace("+00:00", "").replace("Z", "")
    with pytest.raises(ValidationError, match="timezone"):
        TelemetryV1.model_validate_json(payload)


def test_offset_datetime_normalized_to_utc() -> None:
    local = datetime(2026, 7, 25, 14, 0, 0, tzinfo=timezone(timedelta(hours=2)))
    reading = _reading(measured_at=local)
    assert reading.measured_at.tzinfo == UTC
    assert reading.measured_at == local  # same instant


@pytest.mark.parametrize("water_level", [-0.1, 100.1])
def test_water_level_out_of_range_rejected(water_level: float) -> None:
    with pytest.raises(ValidationError):
        _reading(water_level=water_level)


@pytest.mark.parametrize("battery_voltage", [1.0, 2.49, 4.41, 5.0])
def test_battery_voltage_out_of_range_rejected(battery_voltage: float) -> None:
    with pytest.raises(ValidationError):
        _reading(battery_voltage=battery_voltage)


@pytest.mark.parametrize(
    "device_id",
    ["a/b", "a+b", "a#b", "$sys", "Planter-00", "-leading", "", "x" * 33],
)
def test_invalid_device_id_rejected(device_id: str) -> None:
    with pytest.raises(ValidationError):
        _reading(device_id=device_id)


def test_wrong_schema_version_rejected() -> None:
    with pytest.raises(ValidationError):
        _reading(schema_version=2)


def test_extra_fields_ignored() -> None:
    reading = _reading(soil_temperature=21.5)
    assert not hasattr(reading, "soil_temperature")


def test_wrong_type_rejected() -> None:
    with pytest.raises(ValidationError):
        _reading(water_level="high")


def test_missing_field_rejected() -> None:
    with pytest.raises(ValidationError):
        TelemetryV1.model_validate({"device_id": "planter-00"})


def test_topic_matches_filter_shape() -> None:
    topic = telemetry_topic("planter-00")
    assert topic == "planter/v1/planter-00/telemetry"
    prefix, _, suffix = TELEMETRY_TOPIC_FILTER.partition("+")
    assert topic.startswith(prefix)
    assert topic.endswith(suffix)


@pytest.mark.parametrize("device_id", ["planter-00", "a", "x" * 32])
def test_device_id_from_topic_round_trips(device_id: str) -> None:
    assert device_id_from_topic(telemetry_topic(device_id)) == device_id


@pytest.mark.parametrize(
    "topic",
    [
        "planter/v2/planter-00/telemetry",  # wrong version
        "garden/v1/planter-00/telemetry",  # wrong root
        "planter/v1/planter-00/status",  # wrong leaf
        "planter/v1/planter-00",  # too few segments
        "planter/v1/planter-00/telemetry/extra",  # too many segments
        "planter/v1//telemetry",  # empty device segment
        "",
    ],
)
def test_device_id_from_topic_rejects_non_v1_telemetry(topic: str) -> None:
    assert device_id_from_topic(topic) is None


def test_device_id_from_topic_passes_garbage_segment_through() -> None:
    # Not re-validated against DeviceId: the consumer's mismatch check owns that.
    assert device_id_from_topic("planter/v1/UPPER!/telemetry") == "UPPER!"
