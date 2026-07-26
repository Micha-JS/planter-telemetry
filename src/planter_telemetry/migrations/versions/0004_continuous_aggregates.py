"""Hourly and daily per-device rollups as continuous aggregates.

Both views are real-time aggregates (timescaledb.materialized_only = false,
non-default since TimescaleDB 2.13), and that choice is load-bearing for the
demo: the simulator's virtual clock runs 180x ahead of the wall clock, so
its measured_at values are in the future — and refresh policies compute
their windows relative to wall-clock *now*, meaning the background jobs will
materialize little or nothing during a compose session. Real-time
aggregation unions the materialized region with fresh raw rows at query
time, so the rollups are correct and queryable the moment data arrives. The
policies below are the production posture (and materialize history on
long-running stacks); they are not what makes the demo work.

The daily view aggregates raw telemetry directly rather than stacking on
the hourly view: an average of hourly averages is only correct when
weighted by per-hour sample counts, and at this data volume the simpler
exact form costs nothing.

Both CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous) and the
policy CALLs refuse to run inside a transaction, hence the autocommit
block; it is the only thing in this migration so a mid-migration failure
cannot strand unrelated committed DDL.

Revision ID: 0004
Revises: 0003
"""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None

_ROLLUP = """
CREATE MATERIALIZED VIEW {name}
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    device_id,
    time_bucket(INTERVAL '{bucket}', measured_at) AS bucket,
    avg(water_level)     AS water_avg,
    min(water_level)     AS water_min,
    max(water_level)     AS water_max,
    avg(battery_voltage) AS battery_avg,
    min(battery_voltage) AS battery_min,
    count(*)             AS sample_count
FROM telemetry
GROUP BY device_id, bucket
WITH DATA
"""


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(_ROLLUP.format(name="telemetry_hourly", bucket="1 hour"))
        op.execute(_ROLLUP.format(name="telemetry_daily", bucket="1 day"))
        # Windows span >= 2 buckets (a policy hard requirement) and stay a
        # bucket behind now() so only settled buckets are materialized.
        op.execute(
            """
            SELECT add_continuous_aggregate_policy('telemetry_hourly',
                start_offset      => INTERVAL '6 hours',
                end_offset        => INTERVAL '1 hour',
                schedule_interval => INTERVAL '15 minutes')
            """
        )
        op.execute(
            """
            SELECT add_continuous_aggregate_policy('telemetry_daily',
                start_offset      => INTERVAL '3 days',
                end_offset        => INTERVAL '1 day',
                schedule_interval => INTERVAL '1 hour')
            """
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP MATERIALIZED VIEW telemetry_daily")
        op.execute("DROP MATERIALIZED VIEW telemetry_hourly")
