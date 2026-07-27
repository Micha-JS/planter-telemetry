"""Ingest event log: pipeline observations that leave no other trace.

The idempotent insert absorbs duplicates silently (ON CONFLICT DO NOTHING),
which is correct for pipeline state but invisible to M4's meta-observability
panels — the in-memory counters only reach the logs. This table gives those
observations a queryable home: one row per event, `occurred_at` stamped with
wall-clock arrival time like dead_letter.received_at, and device_id /
measured_at identifying the affected reading on the device timeline. `event`
is an open set; ingestion writes only 'deduplicated' today.

Like dead_letter, this is observability of *arrivals*, not pipeline state:
replaying a stream twice leaves telemetry/devices/rollups unchanged (the
idempotency invariant) but appends dedupe events — each replayed message
genuinely was deduplicated, and recording that is the point.

Out-of-order arrivals deliberately have no event here: they are derivable
in SQL from telemetry.received_at (arrival order) vs measured_at (device
order), so persisting them would be redundant. See docs/dashboard-queries.md.

Downgrade is deliberately not implemented: the chain is forward-only past
0003 (see 0003's docstring).

Revision ID: 0006
Revises: 0005
"""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE ingest_events (
            id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            event       text        NOT NULL,
            device_id   text,
            measured_at timestamptz,
            occurred_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    # The dashboard's rate panels filter on wall-clock windows.
    op.execute("CREATE INDEX ingest_events_occurred_at_idx ON ingest_events (occurred_at)")


def downgrade() -> None:
    raise NotImplementedError("the schema is forward-only past 0003; restore from backup instead")
