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

    async def insert_dead_letter(self, dead_letter: DeadLetter) -> None:
        await self._conn.execute(
            _INSERT_DEAD_LETTER,
            (dead_letter.topic, dead_letter.payload, dead_letter.reason),
        )

    async def close(self) -> None:
        await self._conn.close()
