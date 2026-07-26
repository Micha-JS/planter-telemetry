"""Device registry: one row per device ever seen by ingestion.

Ingestion upserts on every valid reading (including deduplicated
redeliveries): LEAST/GREATEST in the upsert make first_seen/last_seen correct
under out-of-order arrival and replay by construction, with no read-modify-
write. M4 reads last_seen for gap detection ("device missed its expected
check-in"), which is why freshness beats batching at this fleet's message
rate (~2-4 msg/s total).

Timestamps are device time (measured_at), not arrival time: the demo clock
runs accelerated relative to the wall clock, and gap detection must compare
like with like on the device timeline.

Deliberately no foreign key from telemetry: a hypertable-to-table FK would
couple insert ordering and complicate the 0003 conversion for zero payoff —
the registry row is written before the reading in the same handler.

Revision ID: 0002
Revises: 0001
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE devices (
            device_id  text        PRIMARY KEY,
            first_seen timestamptz NOT NULL,
            last_seen  timestamptz NOT NULL,
            -- Room for M4+ (display name, location, plant species) without a
            -- migration per attribute; nothing writes it yet.
            metadata   jsonb       NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE devices")
