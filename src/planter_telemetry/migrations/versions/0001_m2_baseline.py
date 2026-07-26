"""M2 baseline: telemetry + dead_letter, exactly as the M2 init script made them.

This migration reproduces db/init/schema.sql (M2, now retired to
tests/fixtures/m2_schema.sql) byte-for-byte in effect: a database migrated
from scratch is indistinguishable from one created by the init script. The
stamp guard in planter_telemetry.migrate relies on that invariant — a
pre-Alembic M2 database is stamped at this revision without executing it.

Revision ID: 0001
Revises:
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE telemetry (
            device_id       text             NOT NULL,
            measured_at     timestamptz      NOT NULL,
            water_level     double precision NOT NULL,
            battery_voltage double precision NOT NULL,
            schema_version  smallint         NOT NULL,
            received_at     timestamptz      NOT NULL DEFAULT now(),
            -- Idempotency key: ingestion writes with ON CONFLICT DO NOTHING
            -- against this constraint. Hypertable-compatible (a hypertable's
            -- unique indexes must include the partition column, measured_at).
            UNIQUE (device_id, measured_at)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE dead_letter (
            id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            topic       text        NOT NULL,
            -- bytea, not text: truncation corruption can split a UTF-8
            -- sequence, and the raw wire bytes are exactly what we want to
            -- preserve for debugging.
            payload     bytea       NOT NULL,
            reason      text        NOT NULL,
            received_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE dead_letter")
    op.execute("DROP TABLE telemetry")
