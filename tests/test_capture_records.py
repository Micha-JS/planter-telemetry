"""JSONL codec: byte-lossless round-trips and strict decoding."""

import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

from planter_telemetry.cli.records import CapturedMessage, decode_record, encode_record

CAPTURED_AT = datetime(2026, 1, 1, 12, 30, tzinfo=UTC)


def test_utf8_payload_round_trips_as_readable_text() -> None:
    payload = b'{"device_id":"planter-00","water_level":51.5}'
    message = CapturedMessage("planter/v1/planter-00/telemetry", payload, CAPTURED_AT)
    line = encode_record(message)
    parsed = json.loads(line)
    assert parsed["payload"] == payload.decode()  # human-readable in diffs
    assert "payload_b64" not in parsed
    assert "\n" not in line
    assert decode_record(line) == message


@pytest.mark.parametrize(
    "payload",
    [
        b"\xff\xfe not utf-8",
        # A TRUNCATED corruption can split a multi-byte sequence:
        "battery — low".encode()[:-5],
        b"",
    ],
)
def test_non_utf8_payload_round_trips_via_base64(payload: bytes) -> None:
    message = CapturedMessage("planter/v1/planter-00/telemetry", payload, CAPTURED_AT)
    line = encode_record(message)
    parsed = json.loads(line)
    if payload and "payload_b64" in parsed:
        assert "payload" not in parsed
    assert decode_record(line) == message  # byte identity either way


def test_captured_at_preserves_timezone() -> None:
    offset_time = datetime(2026, 1, 1, 14, 30, tzinfo=timezone(timedelta(hours=2)))
    message = CapturedMessage("planter/v1/planter-01/telemetry", b"{}", offset_time)
    decoded = decode_record(encode_record(message))
    assert decoded.captured_at == offset_time
    assert decoded.captured_at.utcoffset() == timedelta(hours=2)


def test_encode_rejects_naive_captured_at() -> None:
    naive = CapturedMessage("planter/v1/planter-00/telemetry", b"{}", datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="timezone-aware"):
        encode_record(naive)


@pytest.mark.parametrize(
    ("line", "reason"),
    [
        ("not json at all", "invalid json"),
        ('["a","list"]', "not an object"),
        ('{"captured_at":"2026-01-01T00:00:00+00:00","payload":"x"}', "topic"),
        ('{"topic":"","captured_at":"2026-01-01T00:00:00+00:00","payload":"x"}', "topic"),
        ('{"topic":"t","payload":"x"}', "captured_at"),
        ('{"topic":"t","captured_at":"yesterday","payload":"x"}', "captured_at"),
        ('{"topic":"t","captured_at":"2026-01-01T00:00:00","payload":"x"}', "timezone-aware"),
        ('{"topic":"t","captured_at":"2026-01-01T00:00:00+00:00"}', "exactly one"),
        (
            '{"topic":"t","captured_at":"2026-01-01T00:00:00+00:00",'
            '"payload":"x","payload_b64":"eA=="}',
            "exactly one",
        ),
        ('{"topic":"t","captured_at":"2026-01-01T00:00:00+00:00","payload":7}', "string"),
        (
            '{"topic":"t","captured_at":"2026-01-01T00:00:00+00:00","payload_b64":"???"}',
            "payload_b64",
        ),
    ],
)
def test_malformed_lines_raise_value_error(line: str, reason: str) -> None:
    with pytest.raises(ValueError, match=reason):
        decode_record(line)


def test_serialization_is_deterministic() -> None:
    message = CapturedMessage("planter/v1/planter-00/telemetry", b'{"a":1}', CAPTURED_AT)
    assert encode_record(message) == encode_record(message)
    # Compact separators and fixed key order — the sample-file determinism
    # guard compares bytes, so this shape is load-bearing.
    assert encode_record(message) == (
        '{"topic":"planter/v1/planter-00/telemetry",'
        '"captured_at":"2026-01-01T12:30:00+00:00",'
        '"payload":"{\\"a\\":1}"}'
    )
