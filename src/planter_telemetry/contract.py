"""Versioned telemetry message contract.

This module is the single import surface for the wire format: the ingestion
service (M2) validates incoming payloads against the same model the simulator
publishes. The full contract — topic tree, versioning policy, idempotency
key — is documented in docs/message-contract.md.
"""

from datetime import UTC, datetime
from typing import Annotated, Final, Literal

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
)

SCHEMA_VERSION: Final = 1

# M2's ingestion service subscribes to this filter.
TELEMETRY_TOPIC_FILTER: Final = "planter/v1/+/telemetry"


def telemetry_topic(device_id: str) -> str:
    """Return the publish topic for a device's telemetry."""
    return f"planter/v1/{device_id}/telemetry"


def device_id_from_topic(topic: str) -> str | None:
    """Inverse of telemetry_topic(); None if the topic is not a v1 telemetry topic.

    The segment is not re-validated against DeviceId here: the consumer compares
    it against the payload's validated device_id, so a garbage segment surfaces
    as a mismatch rather than being silently rejected earlier.
    """
    match topic.split("/"):
        case ["planter", "v1", device_id, "telemetry"] if device_id:
            return device_id
        case _:
            return None


# Lowercase alphanumerics plus - and _, max 32 chars: excludes MQTT topic
# metacharacters (/ + #) and a leading $, so interpolating a validated id into
# telemetry_topic() is always topic-safe.
DeviceId = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")]


def _to_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


# Aware (naive datetimes are rejected) and canonicalized to UTC, so the
# idempotency key (device_id, measured_at) is stable regardless of the UTC
# offset a sender used.
UtcDatetime = Annotated[AwareDatetime, AfterValidator(_to_utc)]


class TelemetryV1(BaseModel):
    """One telemetry reading as it travels over MQTT, JSON-encoded.

    extra="ignore" lets v1 grow additive optional fields without breaking
    deployed consumers; anything more than additive is a v2 (new model, new
    topic segment). frozen=True because a reading is a value.
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    schema_version: Literal[1] = 1
    device_id: DeviceId
    measured_at: UtcDatetime
    water_level: float = Field(ge=0, le=100, description="Percent of tank capacity")
    battery_voltage: float = Field(ge=2.5, le=4.4, description="Volts, single-cell LiPo")
