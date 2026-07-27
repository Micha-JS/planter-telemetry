"""Convert telemetry into a hypertable (1-day chunks, migrating any rows).

Chunk interval: at the demo data shape (4 devices x 288 readings/virtual day)
a 1-day chunk holds ~1,150 rows — performance is irrelevant at this volume,
so the interval is chosen for demoability: with the simulator's 180x clock a
new chunk appears every ~8 wall-minutes, making chunk machinery (and later
the columnstore) visible within a single compose session, and chunks align
with the daily continuous aggregate. Trade-off: a stack left running for
wall-weeks accumulates thousands of tiny chunks; acceptable for a demo repo.

migrate_data => true carries existing rows into chunks, which is what makes
this apply cleanly to a stamped M2 database that already holds telemetry.
The UNIQUE (device_id, measured_at) constraint already contains the
partition column, so ingestion's ON CONFLICT idempotency survives unchanged.

Runs inside the normal migration transaction: unlike continuous-aggregate
DDL, create_hypertable (including migrate_data) is transaction-safe.

Downgrade is deliberately not implemented: hypertable conversion is one-way
short of a full table rebuild, so the schema is forward-only from here.

Revision ID: 0003
Revises: 0002
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        SELECT create_hypertable(
            'telemetry',
            by_range('measured_at', INTERVAL '1 day'),
            migrate_data => true
        )
        """
    )


def downgrade() -> None:
    raise NotImplementedError("hypertable conversion is one-way; restore from backup instead")
