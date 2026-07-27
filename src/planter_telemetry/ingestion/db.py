"""The database edge: idempotent telemetry inserts and the dead-letter sink."""

import psycopg
from psycopg.rows import TupleRow

from planter_telemetry.contract import TelemetryV1
from planter_telemetry.ingestion.core import DeadLetter

_INSERT_READING = """\
INSERT INTO telemetry (device_id, measured_at, water_level, battery_voltage, schema_version)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (device_id, measured_at) DO NOTHING
"""

_INSERT_DEAD_LETTER = """\
INSERT INTO dead_letter (topic, payload, reason)
VALUES (%s, %s, %s)
"""

# ON CONFLICT DO NOTHING absorbs duplicates silently, which is right for
# pipeline state but invisible to the dashboard — the event log is where a
# dedupe leaves its queryable trace (occurred_at defaults to now(), the
# wall-clock arrival time, like dead_letter.received_at).
_INSERT_INGEST_EVENT = """\
INSERT INTO ingest_events (event, device_id, measured_at)
VALUES (%s, %s, %s)
"""

# LEAST/GREATEST make the registry correct under out-of-order arrival and
# redelivery without a read-modify-write: whichever order readings land in,
# first_seen/last_seen converge on the min/max measured_at.
_UPSERT_DEVICE = """\
INSERT INTO devices (device_id, first_seen, last_seen)
VALUES (%s, %s, %s)
ON CONFLICT (device_id) DO UPDATE SET
    first_seen = LEAST(devices.first_seen, EXCLUDED.first_seen),
    last_seen  = GREATEST(devices.last_seen, EXCLUDED.last_seen)
"""


class Writer:
    """Row-at-a-time writer over a single autocommit connection.

    Row-at-a-time is deliberate at this scale (~2-4 msg/s from the default
    simulator): it keeps dedupe counting and shutdown draining trivial.
    Micro-batching is the documented upgrade path for M5's accelerated
    replay (docs/ingestion.md).
    """

    def __init__(self, conn: psycopg.AsyncConnection[TupleRow]) -> None:
        self._conn = conn

    @classmethod
    async def connect(cls, dsn: str) -> "Writer":
        # Autocommit: every write is a single self-contained statement, so
        # there is no multi-statement transaction to manage.
        return cls(await psycopg.AsyncConnection.connect(dsn, autocommit=True))

    async def insert_reading(self, reading: TelemetryV1) -> bool:
        """Insert one reading; False means the idempotency key already existed."""
        cursor = await self._conn.execute(
            _INSERT_READING,
            (
                reading.device_id,
                reading.measured_at,
                reading.water_level,
                reading.battery_voltage,
                reading.schema_version,
            ),
        )
        return cursor.rowcount == 1

    async def upsert_device(self, reading: TelemetryV1) -> None:
        """Track the reading's device in the registry (first_seen/last_seen)."""
        await self._conn.execute(
            _UPSERT_DEVICE,
            (reading.device_id, reading.measured_at, reading.measured_at),
        )

    async def record_dedupe(self, reading: TelemetryV1) -> None:
        """Log a deduplicated redelivery to the ingest event table."""
        await self._conn.execute(
            _INSERT_INGEST_EVENT,
            ("deduplicated", reading.device_id, reading.measured_at),
        )

    async def insert_dead_letter(self, dead_letter: DeadLetter) -> None:
        await self._conn.execute(
            _INSERT_DEAD_LETTER,
            (dead_letter.topic, dead_letter.payload, dead_letter.reason),
        )

    async def close(self) -> None:
        await self._conn.close()
