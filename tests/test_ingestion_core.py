"""Pure classification: every adversarial input becomes a meaningful dead letter."""

import json
from datetime import UTC, datetime

import pytest

from planter_telemetry.contract import TelemetryV1, telemetry_topic
from planter_telemetry.ingestion.core import DeadLetter, ValidReading, classify
from planter_telemetry.simulator.wire import CorruptionMode, corrupt, encode

READING = TelemetryV1(
    device_id="planter-00",
    measured_at=datetime(2026, 7, 25, 12, 0, 0, tzinfo=UTC),
    water_level=55.5,
    battery_voltage=3.8,
)
TOPIC = telemetry_topic(READING.device_id)


def _dead_letter(topic: str, payload: bytes) -> DeadLetter:
    outcome = classify(topic, payload)
    assert isinstance(outcome, DeadLetter)
    assert outcome.topic == topic
    assert outcome.payload == payload  # raw bytes preserved for debugging
    return outcome


def test_valid_message_classified_as_reading() -> None:
    outcome = classify(TOPIC, encode(READING))
    assert isinstance(outcome, ValidReading)
    assert outcome.reading == READING
    assert outcome.reading.measured_at.tzinfo == UTC


def test_naive_datetime_dead_letters_with_timezone_reason() -> None:
    payload = encode(READING).decode().replace("+00:00", "").replace("Z", "").encode()
    outcome = _dead_letter(TOPIC, payload)
    assert "measured_at" in outcome.reason
    assert "timezone" in outcome.reason


@pytest.mark.parametrize(
    ("field", "value"),
    [("water_level", 150.0), ("water_level", -3.0), ("battery_voltage", 1.0)],
)
def test_out_of_range_value_dead_letters_naming_the_field(field: str, value: float) -> None:
    fields = json.loads(encode(READING))
    fields[field] = value
    outcome = _dead_letter(TOPIC, json.dumps(fields).encode())
    assert field in outcome.reason


def test_device_mismatch_dead_letters_naming_both_ids() -> None:
    outcome = _dead_letter(telemetry_topic("planter-01"), encode(READING))
    assert "device_id mismatch" in outcome.reason
    assert "planter-01" in outcome.reason
    assert "planter-00" in outcome.reason


def test_truncated_json_dead_letters_as_invalid_json() -> None:
    payload = corrupt(encode(READING), CorruptionMode.TRUNCATED)
    outcome = _dead_letter(TOPIC, payload)
    assert "invalid json" in outcome.reason.lower()


def test_wrong_type_dead_letters_naming_the_field() -> None:
    payload = corrupt(encode(READING), CorruptionMode.WRONG_TYPE)
    outcome = _dead_letter(TOPIC, payload)
    assert "water_level" in outcome.reason


def test_missing_field_dead_letters_naming_the_field() -> None:
    payload = corrupt(encode(READING), CorruptionMode.MISSING_FIELD)
    outcome = _dead_letter(TOPIC, payload)
    assert "device_id" in outcome.reason


def test_non_telemetry_topic_dead_letters() -> None:
    outcome = _dead_letter("planter/v1/planter-00/status", encode(READING))
    assert "topic" in outcome.reason


def test_reason_is_bounded_for_hostile_payloads() -> None:
    # Every field wrong at once: the reason must stay short and deterministic.
    payload = json.dumps(
        {"device_id": "NOPE!", "measured_at": "yesterday", "water_level": "high"}
    ).encode()
    first = _dead_letter(TOPIC, payload)
    second = _dead_letter(TOPIC, payload)
    assert first.reason == second.reason
    assert len(first.reason) <= 520  # capped body plus the "invalid payload: " prefix


def test_empty_payload_dead_letters() -> None:
    outcome = _dead_letter(TOPIC, b"")
    assert "invalid payload" in outcome.reason
