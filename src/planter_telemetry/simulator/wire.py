"""Wire encoding and payload corruption. Pure bytes in, bytes out.

Corruption happens here, at the encoding edge, so the typed reading series
never contains garbage. The three modes cover the failure classes the M2
ingestion service must dead-letter: unparseable JSON, type-invalid, and
schema-incomplete payloads.
"""

import enum
import json

from planter_telemetry.contract import TelemetryV1


class CorruptionMode(enum.Enum):
    TRUNCATED = "truncated"
    WRONG_TYPE = "wrong_type"
    MISSING_FIELD = "missing_field"


def encode(reading: TelemetryV1) -> bytes:
    """Canonical wire form: the model's JSON, UTF-8 encoded."""
    return reading.model_dump_json().encode()


def corrupt(payload: bytes, mode: CorruptionMode) -> bytes:
    """Deterministically damage an already-encoded payload."""
    if mode is CorruptionMode.TRUNCATED:
        return payload[: len(payload) // 2]
    obj: dict[str, object] = json.loads(payload)
    if mode is CorruptionMode.WRONG_TYPE:
        obj["water_level"] = "high"
    else:
        del obj["device_id"]
    return json.dumps(obj).encode()
