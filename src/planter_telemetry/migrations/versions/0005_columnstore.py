"""Columnstore (compression) policy on raw telemetry; deliberately no retention.

Uses the columnstore API that superseded the classic compression API in
TimescaleDB 2.18 (ALTER TABLE ... timescaledb.enable_columnstore +
add_columnstore_policy); the old spelling still works but this repo pins an
image where the new one is canonical. segmentby = device_id because every
analytical query filters or groups by device; orderby measured_at DESC
matches the dominant "recent first" access pattern.

The 7-day `after` is generous on purpose and — like all policy scheduling —
relative to the wall clock. The simulator stamps readings ahead of the wall
clock (180x virtual time), so demo chunks only become eligible once real
time catches up to them: compression can never eat into a live demo
session. To see the columnstore act immediately, compress a chunk by hand:
    CALL convert_to_columnstore(chunk) on a row of show_chunks('telemetry').

No retention policy, deliberately: this is a demo repo whose history *is*
the demo (replay, dashboards, forecasting all feed on it), the dataset is
tiny, and a wall-clock retention policy against future-stamped data would
be a silent data-loss trap. Compression is the aging story; a production
deployment would add e.g.
    SELECT add_retention_policy('telemetry', drop_after => INTERVAL '1 year');

add_columnstore_policy is a procedure (CALL): autocommit block, which is
the only thing in this migration. Statements in that block commit
individually while alembic_version still says 0004, so a killed migrate
container re-runs this revision on the next `up` — the ALTER is naturally
idempotent and the policy takes if_not_exists, making the re-run safe.

Downgrade is deliberately not implemented: the chain is forward-only past
0003 (see 0003's docstring) — and once a chunk actually sits in the
columnstore, `enable_columnstore = false` refuses to run anyway.

Revision ID: 0005
Revises: 0004
"""

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            """
            ALTER TABLE telemetry SET (
                timescaledb.enable_columnstore,
                timescaledb.segmentby = 'device_id',
                timescaledb.orderby   = 'measured_at DESC'
            )
            """
        )
        op.execute(
            "CALL add_columnstore_policy('telemetry',"
            " after => INTERVAL '7 days', if_not_exists => true)"
        )


def downgrade() -> None:
    raise NotImplementedError("the schema is forward-only past 0003; restore from backup instead")
