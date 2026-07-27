"""Log-shape smoke tests: every formatted record must be one valid JSON line."""

import json
import logging
from datetime import UTC, datetime
from typing import Any

from planter_telemetry.jsonlog import JsonFormatter

REQUIRED_FIELDS = {"ts", "level", "service", "logger", "event"}


def _format(record: logging.LogRecord) -> dict[str, Any]:
    line = JsonFormatter(service="test").format(record)
    assert "\n" not in line
    parsed = json.loads(line)
    assert isinstance(parsed, dict)
    return parsed


def _record(
    msg: str,
    *,
    extra: dict[str, Any] | None = None,
    exc_info: Any = None,
    level: int = logging.INFO,
) -> logging.LogRecord:
    return logging.getLogger("planter_telemetry.ingestion").makeRecord(
        "planter_telemetry.ingestion", level, "file.py", 1, msg, (), exc_info, extra=extra
    )


def test_required_fields_present() -> None:
    parsed = _format(_record("stats"))
    assert parsed.keys() >= REQUIRED_FIELDS
    assert parsed["event"] == "stats"
    assert parsed["level"] == "INFO"
    assert parsed["service"] == "test"
    assert parsed["logger"] == "planter_telemetry.ingestion"
    # ts is ISO-8601 UTC and parseable.
    ts = datetime.fromisoformat(parsed["ts"])
    assert ts.tzinfo is not None
    assert ts.utcoffset() == UTC.utcoffset(None)


def test_extra_fields_are_merged() -> None:
    parsed = _format(_record("stats", extra={"ingested": 42, "device_id": "planter-01"}))
    assert parsed["ingested"] == 42
    assert parsed["device_id"] == "planter-01"


def test_datetime_in_extra_does_not_crash() -> None:
    when = datetime(2026, 1, 1, tzinfo=UTC)
    parsed = _format(_record("deduplicated", extra={"measured_at": when}))
    assert parsed["measured_at"] == str(when)


def test_exception_is_serialized() -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        parsed = _format(_record("reconnect", level=logging.WARNING, exc_info=sys.exc_info()))
    assert "boom" in parsed["exc"]
    assert parsed["level"] == "WARNING"


def test_stdlib_record_attributes_are_not_leaked() -> None:
    parsed = _format(_record("stats"))
    # A sample of LogRecord internals that must stay out of the payload.
    assert {"args", "msg", "levelno", "pathname", "taskName"}.isdisjoint(parsed.keys())
