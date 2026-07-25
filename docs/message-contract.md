# Message contract

The wire contract between planter devices (or the simulator) and the ingestion
pipeline. The canonical implementation is
[`planter_telemetry.contract`](../src/planter_telemetry/contract.py); every
producer and consumer imports that module rather than restating the schema.

## Topic tree

```
planter/
└── v1/
    └── {device_id}/
        └── telemetry        ← one JSON telemetry reading per message
```

- Publish topic: `planter/v1/{device_id}/telemetry`
- Subscribe filter (ingestion): `planter/v1/+/telemetry`
- QoS 1, no retained messages. The broker is a transient transport; durable
  state lives in the database (M2+).

The `v1` segment is the contract version. The `device_id` in the topic must
match the `device_id` in the payload; consumers treat a mismatch as malformed.

## Schema (v1)

JSON object, UTF-8. Field order is not significant; unknown extra fields are
ignored (see versioning policy).

| Field | JSON type | Constraints | Unit / meaning |
|---|---|---|---|
| `schema_version` | number | must be `1` | payload schema version (cross-check; the topic segment is the router) |
| `device_id` | string | `^[a-z0-9][a-z0-9_-]{0,31}$` | stable device identifier; pattern excludes MQTT topic metacharacters so ids are always topic-safe |
| `measured_at` | string | RFC 3339 timestamp **with UTC offset**; naive timestamps are rejected | when the reading was taken; canonicalized to UTC on validation |
| `water_level` | number | `0 ≤ x ≤ 100` | percent of tank capacity. Percent (not raw sensor counts) because calibration is per-device firmware business; percent is comparable across the fleet and feeds the depletion forecast directly |
| `battery_voltage` | number | `2.5 ≤ x ≤ 4.4` | volts, single-cell LiPo (4.2 V full, ~3.0 V working cutoff, 2.5 V protection cutoff; 4.4 allows LiHV / ADC headroom). Values outside the range are sensor garbage, not battery state |

Example payload:

```json
{
  "schema_version": 1,
  "device_id": "planter-00",
  "measured_at": "2026-07-25T12:34:56.789012Z",
  "water_level": 73.4,
  "battery_voltage": 3.91
}
```

## Idempotency key

`(device_id, measured_at)` uniquely identifies a reading. A device takes at
most one measurement per wake-up, so re-delivery (MQTT QoS 1 duplicates,
replay, simulator-injected duplicates) reuses the same key. Ingestion (M2)
relies on this: `INSERT ... ON CONFLICT (device_id, measured_at) DO NOTHING`.

`measured_at` is canonicalized to UTC during validation, so the key is stable
no matter which UTC offset a sender used on the wire.

## Versioning policy

- **Still v1 (no version bump):** adding new *optional* fields. Consumers
  ignore unknown keys, so devices may start sending a new field before every
  consumer understands it.
- **Requires v2:** renaming, removing, or retyping a field; changing a field's
  unit or semantics; making an optional field required. A v2 means a new
  `TelemetryV2` model **and** a new `planter/v2/{device_id}/telemetry` topic
  segment. v1 and v2 run side by side during migration; consumers subscribe to
  the versions they understand.
- `schema_version` in the payload is a cross-check against misrouted or
  mislabeled messages — v1 consumers reject payloads declaring any other
  version. The topic segment, not the payload field, is what routes messages.
