"""Pure message classification: (topic, payload) in, outcome out. No I/O."""

from dataclasses import dataclass

from pydantic import ValidationError

from planter_telemetry.contract import TelemetryV1, device_id_from_topic

# A hostile payload can produce arbitrarily many validation errors; the reason
# column is for humans and grep, not a full error dump.
_MAX_REASON_ERRORS = 3
_MAX_REASON_CHARS = 500


@dataclass(frozen=True)
class ValidReading:
    reading: TelemetryV1


@dataclass(frozen=True)
class DeadLetter:
    topic: str
    payload: bytes
    reason: str


Outcome = ValidReading | DeadLetter


def _reason(exc: ValidationError) -> str:
    """Condense a ValidationError into deterministic, greppable text."""
    errors = exc.errors()
    parts = []
    for error in errors[:_MAX_REASON_ERRORS]:
        loc = ".".join(str(piece) for piece in error["loc"]) or "payload"
        parts.append(f"{loc}: {error['msg']}")
    if len(errors) > _MAX_REASON_ERRORS:
        parts.append(f"(+{len(errors) - _MAX_REASON_ERRORS} more)")
    return "; ".join(parts)[:_MAX_REASON_CHARS]


def classify(topic: str, payload: bytes) -> Outcome:
    """Decide whether one MQTT message is a valid reading or a dead letter.

    Order matters: a payload that fails schema validation dead-letters on the
    validation reason even if its topic is also odd, and the topic/payload
    device_id cross-check only runs on otherwise-valid payloads.
    """
    topic_device_id = device_id_from_topic(topic)
    if topic_device_id is None:
        # The subscribe filter should make this unreachable; classify stays
        # total anyway so the service never has to trust the broker.
        return DeadLetter(topic, payload, "topic does not match planter/v1/{device_id}/telemetry")
    try:
        reading = TelemetryV1.model_validate_json(payload)
    except ValidationError as exc:
        return DeadLetter(topic, payload, f"invalid payload: {_reason(exc)}")
    if reading.device_id != topic_device_id:
        return DeadLetter(
            topic,
            payload,
            f"device_id mismatch: topic={topic_device_id!r} payload={reading.device_id!r}",
        )
    return ValidReading(reading)
