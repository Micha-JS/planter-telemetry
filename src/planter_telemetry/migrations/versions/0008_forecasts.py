"""Per-device forecasts: one row per (device, kind, device-time watermark).

The primary key IS the idempotency key, mirroring telemetry's
UNIQUE (device_id, measured_at): `as_of` is the newest measured_at that fed
the fit, so re-running an analytics pass over unchanged data inserts nothing
— M2's replay invariant holds through the analytics layer. Keying on the
per-device watermark (never the fleet-wide max, which advances almost every
pass under the accelerated demo clock) is what makes the no-op real: on
real hardware, with a 5-minute cadence and a much shorter pass interval,
most passes write nothing at all. The flip side is accepted and documented:
a late out-of-order reading older than `as_of` does not move the watermark,
so the marginally improved fit it would enable is dropped.

History is kept — no upsert-in-place — because forecast evolution over time
is itself the dashboard's forecast-vs-actual story. Three clocks, three
columns, never conflated: `as_of` and `crosses_at` are device time (the
accelerated simulator stamps them ahead of the wall clock); `computed_at` is
wall-clock time, for ops only, like telemetry.received_at and
ingest_events.occurred_at.

Deliberately not a hypertable: a handful of rows per pass, read via
DISTINCT ON (device_id, kind) ... ORDER BY as_of DESC — a backwards scan of
the primary key, not a time-range scan — so chunk machinery would cost
overhead and buy nothing. No extra index for the same reason. There is
deliberately no horizon column: it is exactly crosses_at - as_of, and the
forecasts_latest view is where that convenience lives (clamped at zero so
AT_OR_BELOW_TARGET reads as "0 days left", not negative).

No retention, matching 0005's posture: the history is the demo. A production
deployment would trim on computed_at — the wall clock is the right clock for
retention, unlike for the forecast arithmetic.

Two claims above were later corrected, in 0010: the view's horizon clamp
(`greatest` ignores NULLs, so no-crossing statuses read as a confident zero)
and its DISTINCT ON scan (which walks the whole history, not one entry per
group). The writer also upserts on a status change rather than ignoring the
conflict — see docs/analytics.md — because a device that goes dark keeps its
watermark.

Downgrade is deliberately not implemented: the chain is forward-only past
0003 (see 0003's docstring).

Revision ID: 0008
Revises: 0007
"""

from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE forecasts (
            device_id           text             NOT NULL,
            kind                text             NOT NULL,
            -- Device-time watermark: the newest measured_at that fed the fit.
            as_of               timestamptz      NOT NULL,
            status              text             NOT NULL,
            target_value        double precision NOT NULL,
            latest_value        double precision,
            crosses_at          timestamptz,
            crosses_at_earliest timestamptz,
            crosses_at_latest   timestamptz,
            slope_per_hour      double precision,
            residual_mad        double precision,
            fit_points          integer,
            segment_points      integer,
            fit_span_seconds    double precision,
            truncated           boolean          NOT NULL DEFAULT false,
            -- Wall clock, for ops only; never used for forecast arithmetic.
            computed_at         timestamptz      NOT NULL DEFAULT now(),
            PRIMARY KEY (device_id, kind, as_of)
        )
        """
    )
    op.execute(
        """
        CREATE VIEW forecasts_latest AS
        SELECT DISTINCT ON (device_id, kind)
               device_id, kind, as_of, status, target_value, latest_value,
               crosses_at, crosses_at_earliest, crosses_at_latest,
               slope_per_hour, residual_mad, fit_points, segment_points,
               fit_span_seconds, truncated, computed_at,
               greatest(EXTRACT(EPOCH FROM (crosses_at - as_of)), 0)::double precision
                   AS horizon_seconds
        FROM forecasts
        ORDER BY device_id, kind, as_of DESC
        """
    )
    # 0007's ALTER DEFAULT PRIVILEGES already covers objects created here; the
    # explicit grant is an idempotent belt, and the integration suite proves
    # readability by connecting as grafana_reader and running the panel SQL.
    op.execute("GRANT SELECT ON forecasts, forecasts_latest TO grafana_reader")


def downgrade() -> None:
    raise NotImplementedError("the schema is forward-only past 0003; restore from backup instead")
