"""The database edge: the telemetry window read, idempotent forecast and
alert-event writes. One autocommit connection per pass — analytics is
poll-driven, so there is no persistent connection to babysit and no
reconnect loop to reason about; a failed pass is retried whole on the next
interval."""

from datetime import datetime

import psycopg
from psycopg.rows import TupleRow

from planter_telemetry.analytics.model import AlertState, Forecast, Sample

_SELECT_DATA_NOW = "SELECT max(measured_at) FROM telemetry"

# One query per pass, grouped in Python — not one per device (N+1). The fit
# reads raw telemetry, not the M3 rollups, on purpose: hourly averaging
# smears a refill step across its bucket, destroying exactly the signal
# segment detection depends on (docs/analytics.md).
_SELECT_WINDOW = """\
SELECT device_id, measured_at, water_level, battery_voltage
FROM telemetry
WHERE measured_at > %s
ORDER BY device_id, measured_at
"""

# Which metric kind each value column carries. The pairing lives here, next
# to the SELECT whose column order it describes, so that no caller can pair a
# Metric with a column by position — reordering (or extending) the service's
# metric tuple must never fit water's samples against battery's constants.
WINDOW_KINDS: tuple[str, ...] = ("water", "battery")

# The primary key is the idempotency key: a pass over unchanged data
# re-derives the same status at the same watermark and writes nothing.
#
# The conflict clause updates rather than ignores, gated on the status having
# actually changed, because a device that goes dark keeps its watermark: its
# STALE forecast carries the same (device_id, kind, as_of) as the last
# healthy one, so DO NOTHING left forecasts_latest showing a frozen "ok" with
# a never-decreasing horizon for a planter that stopped reporting days ago —
# the dashboard's single worst lie. Gating on status keeps every other
# guarantee intact: unchanged data changes no row (the replay invariant), and
# a late out-of-order reading still does not rewrite a forecast, because it
# moves neither the watermark nor the status.
_INSERT_FORECAST = """\
INSERT INTO forecasts (
    device_id, kind, as_of, status, target_value, latest_value,
    crosses_at, crosses_at_earliest, crosses_at_latest,
    slope_per_hour, residual_mad, fit_points, segment_points,
    fit_span_seconds, truncated
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (device_id, kind, as_of) DO UPDATE SET
    status              = EXCLUDED.status,
    target_value        = EXCLUDED.target_value,
    latest_value        = EXCLUDED.latest_value,
    crosses_at          = EXCLUDED.crosses_at,
    crosses_at_earliest = EXCLUDED.crosses_at_earliest,
    crosses_at_latest   = EXCLUDED.crosses_at_latest,
    slope_per_hour      = EXCLUDED.slope_per_hour,
    residual_mad        = EXCLUDED.residual_mad,
    fit_points          = EXCLUDED.fit_points,
    segment_points      = EXCLUDED.segment_points,
    fit_span_seconds    = EXCLUDED.fit_span_seconds,
    truncated           = EXCLUDED.truncated,
    computed_at         = now()
WHERE forecasts.status IS DISTINCT FROM EXCLUDED.status
"""

# The transition rule's durable state: latest decision per (device, kind),
# straight off alert_events_state_idx. Reading it from the table (never
# process memory) is what makes a restart unable to re-fire a standing alert.
_SELECT_ALERT_STATES = """\
SELECT DISTINCT ON (device_id, kind) device_id, kind, state, as_of
FROM alert_events
ORDER BY device_id, kind, as_of DESC
"""

# RETURNING id tells the caller whether this decision is new: a conflict
# (same decision re-derived from unchanged data) returns no row, and the
# caller must then not notify again.
_INSERT_ALERT_EVENT = """\
INSERT INTO alert_events (
    device_id, kind, state, as_of, crosses_at, horizon_seconds, threshold_seconds
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (device_id, kind, as_of, state) DO NOTHING
RETURNING id
"""

_MARK_NOTIFIED = """\
UPDATE alert_events SET notified = %s, notify_error = %s WHERE id = %s
"""


class Store:
    """Row-at-a-time reader/writer over a single autocommit connection.

    Same posture as ingestion's Writer: every statement is self-contained,
    so there is no transaction state to manage, and the idempotent inserts
    make a crashed pass harmless — the next one re-derives and conflicts.
    """

    def __init__(self, conn: psycopg.AsyncConnection[TupleRow]) -> None:
        self._conn = conn

    @classmethod
    async def connect(cls, dsn: str) -> "Store":
        return cls(await psycopg.AsyncConnection.connect(dsn, autocommit=True))

    async def data_now(self) -> datetime | None:
        """The fleet-wide newest measured_at — the pass's data-time 'now'.

        None on an empty database, which is a *successful* pass with zero
        forecasts, not an error: a fresh volume races the simulator on the
        first up.
        """
        cursor = await self._conn.execute(_SELECT_DATA_NOW)
        row = await cursor.fetchone()
        return row[0] if row is not None else None

    async def load_window(self, cutoff: datetime) -> dict[str, dict[str, list[Sample]]]:
        """Samples per device per metric kind, in device-time order, for
        everything newer than the cutoff.

        Keyed by kind (see WINDOW_KINDS) rather than returned as raw rows:
        the caller asks for the kind its Metric names and cannot accidentally
        read a column by position.
        """
        cursor = await self._conn.execute(_SELECT_WINDOW, (cutoff,))
        series: dict[str, dict[str, list[Sample]]] = {}
        for device_id, measured_at, water, battery in await cursor.fetchall():
            by_kind = series.get(device_id)
            if by_kind is None:
                by_kind = series[device_id] = {kind: [] for kind in WINDOW_KINDS}
            by_kind["water"].append(Sample(measured_at, water))
            by_kind["battery"].append(Sample(measured_at, battery))
        return series

    async def latest_alert_states(self) -> dict[tuple[str, str], AlertState]:
        cursor = await self._conn.execute(_SELECT_ALERT_STATES)
        return {
            (device_id, kind): AlertState(state=state, as_of=as_of)
            for device_id, kind, state, as_of in await cursor.fetchall()
        }

    async def insert_forecast(self, device_id: str, fc: Forecast) -> bool:
        """Record one forecast; False means this watermark already carried
        this status (the idempotent no-op)."""
        if fc.as_of is None:  # NO_DATA: nothing to key a row on
            return False
        fit = fc.fit
        cursor = await self._conn.execute(
            _INSERT_FORECAST,
            (
                device_id,
                fc.kind,
                fc.as_of,
                fc.status.value,
                fc.target,
                fc.latest_value,
                fc.crosses_at,
                fc.crosses_at_earliest,
                fc.crosses_at_latest,
                fit.slope_per_hour if fit else None,
                fit.residual_mad if fit else None,
                fit.points if fit else None,
                fit.segment_points if fit else None,
                fit.span_seconds if fit else None,
                fit.truncated if fit else False,
            ),
        )
        return cursor.rowcount == 1

    async def insert_alert_event(
        self,
        device_id: str,
        kind: str,
        state: str,
        as_of: datetime,
        crosses_at: datetime | None,
        horizon_seconds: float | None,
        threshold_seconds: float,
    ) -> int | None:
        """Record one alert decision; None means it was already recorded
        (re-derived from unchanged data) and must not notify again."""
        cursor = await self._conn.execute(
            _INSERT_ALERT_EVENT,
            (device_id, kind, state, as_of, crosses_at, horizon_seconds, threshold_seconds),
        )
        row = await cursor.fetchone()
        return row[0] if row is not None else None

    async def mark_notified(self, event_id: int, error: str | None) -> None:
        """Record the delivery outcome for an alert decision, after the fact:
        the decision row always exists before delivery is attempted."""
        await self._conn.execute(_MARK_NOTIFIED, (error is None, error, event_id))

    async def close(self) -> None:
        await self._conn.close()
