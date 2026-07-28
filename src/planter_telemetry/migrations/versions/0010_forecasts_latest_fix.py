"""forecasts_latest, corrected: NULL horizons stay NULL, and the view stops
scanning the whole forecast history.

Two defects in 0008's view, neither of which a query that merely *runs* can
catch — which is why the panel-SQL integration test saw nothing:

  * `greatest(EXTRACT(EPOCH FROM (crosses_at - as_of)), 0)` returned 0, not
    NULL, for every no-crossing status: Postgres GREATEST ignores NULL
    arguments (the result is NULL only when all of them are). A just-refilled
    planter — status `insufficient_points`, `crosses_at` NULL — therefore
    rendered as "0.00 days left" in the forecast detail panel, which is what
    a bone-dry tank reads as. That is precisely the confusion the honest
    statuses exist to prevent, and it also made the panel's
    `ORDER BY horizon_seconds NULLS FIRST` dead code. The clamp at zero is
    kept for AT_OR_BELOW_TARGET, whose crossing is its own watermark.

  * `DISTINCT ON (device_id, kind) ... ORDER BY as_of DESC` reads every row.
    Postgres has no btree skip scan before 18, so 0008's "backwards scan of
    the primary key" walks the entire index rather than stopping at one entry
    per group — over an append-only table with no retention, whose watermark
    advances on nearly every pass under the accelerated demo clock. The
    LATERAL form below descends the primary key once per (device, kind), so
    dashboard refresh cost stops tracking the length of the history.

The kind list is spelled out here because that list is exactly what makes the
index descent possible. It is the same trade the dashboard already makes in
its per-kind panels: a third metric means a migration, in both places. The
device roster comes from `devices`, which ingestion upserts with every
reading and which the attention panel already treats as the fleet — so
forecasts for a device that is not in the roster no longer appear in the
view (they remain in `forecasts`, which is where the history lives).

CREATE OR REPLACE keeps the view's identity, column list and grants; the
explicit GRANT is the same idempotent belt 0008 wears.

Downgrade is deliberately not implemented: the chain is forward-only past
0003 (see 0003's docstring).

Revision ID: 0010
Revises: 0009
"""

from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE VIEW forecasts_latest AS
        SELECT f.device_id, f.kind, f.as_of, f.status, f.target_value, f.latest_value,
               f.crosses_at, f.crosses_at_earliest, f.crosses_at_latest,
               f.slope_per_hour, f.residual_mad, f.fit_points, f.segment_points,
               f.fit_span_seconds, f.truncated, f.computed_at,
               (CASE
                    WHEN f.crosses_at IS NULL THEN NULL
                    ELSE greatest(EXTRACT(EPOCH FROM (f.crosses_at - f.as_of)), 0)
                END)::double precision AS horizon_seconds
        FROM devices d
        CROSS JOIN (VALUES ('water'::text), ('battery'::text)) AS k(kind)
        CROSS JOIN LATERAL (
            SELECT *
            FROM forecasts
            WHERE device_id = d.device_id AND kind = k.kind
            ORDER BY as_of DESC
            LIMIT 1
        ) f
        """
    )
    op.execute("GRANT SELECT ON forecasts_latest TO grafana_reader")


def downgrade() -> None:
    raise NotImplementedError("the schema is forward-only past 0003; restore from backup instead")
