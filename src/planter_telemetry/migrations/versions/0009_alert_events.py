"""Alert decisions, recorded whether or not a notification was delivered.

M7's done-criterion is "an alert fires" — a testable claim, which makes the
*decision* the durable artifact and delivery an opt-in side effect
(ANALYTICS_NTFY_URL, empty by default so a stranger's clone never posts to
anyone's topic). The service writes the decision row before attempting the
POST and fills in notified/notify_error afterwards: a hung notifier can lose
a push, never a decision.

This table is load-bearing state, not a write-only audit log: the alert
rule fires on the *transition* into the alerting condition, and the previous
state is derived from the latest row per (device_id, kind) here — never from
process memory — so a service restart cannot re-fire a standing alert. (At
the demo's accelerated clock, a condition-based rule would repeat the same
notification every few wall minutes for as long as a planter stayed dry;
hysteresis and the data-time cooldown live in the pure model.)

`as_of` is device time — the watermark of the deciding forecast — and the
UNIQUE mirrors the forecasts primary key: re-running a pass over unchanged
data re-derives the same decision and the insert is a no-op. `occurred_at`
is wall-clock time, for ops, like ingest_events.occurred_at.

The ntfy URL is deliberately not stored (and never logged): an ntfy topic
URL is a write capability, and a table Grafana can read is no place for one.

Downgrade is deliberately not implemented: the chain is forward-only past
0003 (see 0003's docstring).

Revision ID: 0009
Revises: 0008
"""

from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE alert_events (
            id                bigint           GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            device_id         text             NOT NULL,
            kind              text             NOT NULL,
            state             text             NOT NULL,
            -- Device time: the watermark of the forecast that decided this.
            as_of             timestamptz      NOT NULL,
            crosses_at        timestamptz,
            horizon_seconds   double precision,
            threshold_seconds double precision NOT NULL,
            notified          boolean          NOT NULL DEFAULT false,
            notify_error      text,
            occurred_at       timestamptz      NOT NULL DEFAULT now(),
            UNIQUE (device_id, kind, as_of, state)
        )
        """
    )
    # The hot read is "latest decision per (device, kind)" on the device
    # timeline — the DISTINCT ON both the transition rule and the dashboard
    # use.
    op.execute("CREATE INDEX alert_events_state_idx ON alert_events (device_id, kind, as_of DESC)")
    op.execute("GRANT SELECT ON alert_events TO grafana_reader")


def downgrade() -> None:
    raise NotImplementedError("the schema is forward-only past 0003; restore from backup instead")
