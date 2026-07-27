"""JSONL codec for captured MQTT messages. Pure: bytes and strings only.

One record per line: {"topic": ..., "captured_at": <ISO-8601, aware>, and
exactly one of "payload" (UTF-8 text) or "payload_b64" (base64)}.

Payloads are stored as readable text whenever they decode as UTF-8 —
telemetry is JSON, and the committed sample file should be reviewable in a
diff. UTF-8 decode→encode is bijective on valid sequences, so the text
branch is byte-lossless; anything else (e.g. a corruption that truncates a
multi-byte sequence) falls back to base64. Serialization is deterministic
(fixed key order, compact separators) so regenerating the sample file is a
byte-for-byte comparison.
"""

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class CapturedMessage:
    topic: str
    payload: bytes
    captured_at: datetime  # timezone-aware


def encode_record(message: CapturedMessage) -> str:
    """One JSONL line, no trailing newline."""
    if message.captured_at.tzinfo is None:
        raise ValueError("captured_at must be timezone-aware")
    record: dict[str, str] = {
        "topic": message.topic,
        "captured_at": message.captured_at.isoformat(),
    }
    try:
        record["payload"] = message.payload.decode("utf-8")
    except UnicodeDecodeError:
        record["payload_b64"] = base64.b64encode(message.payload).decode("ascii")
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"))


def decode_record(line: str) -> CapturedMessage:
    """Inverse of encode_record; raises ValueError on any malformed line."""
    try:
        record = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid json: {exc}") from exc
    if not isinstance(record, dict):
        raise ValueError("record is not an object")

    topic = record.get("topic")
    if not isinstance(topic, str) or not topic:
        raise ValueError("missing or invalid topic")

    raw_captured_at = record.get("captured_at")
    if not isinstance(raw_captured_at, str):
        raise ValueError("missing or invalid captured_at")
    try:
        captured_at = datetime.fromisoformat(raw_captured_at)
    except ValueError as exc:
        raise ValueError(f"invalid captured_at: {exc}") from exc
    if captured_at.tzinfo is None:
        raise ValueError("captured_at must be timezone-aware")

    text = record.get("payload")
    encoded = record.get("payload_b64")
    if (text is None) == (encoded is None):
        raise ValueError("exactly one of payload/payload_b64 required")
    if text is not None:
        if not isinstance(text, str):
            raise ValueError("payload must be a string")
        payload = text.encode("utf-8")
    else:
        if not isinstance(encoded, str):
            raise ValueError("payload_b64 must be a string")
        try:
            payload = base64.b64decode(encoded, validate=True)
        except binascii.Error as exc:
            raise ValueError(f"invalid payload_b64: {exc}") from exc

    return CapturedMessage(topic=topic, payload=payload, captured_at=captured_at)
