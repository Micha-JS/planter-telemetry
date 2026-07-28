# Dashboard queries

Every SQL query behind the [Planter Fleet dashboard](../grafana/dashboards/planter-fleet.json),
reviewable outside Grafana JSON. A unit test (`tests/test_dashboard.py`)
asserts each panel's `rawSql` appears here verbatim (whitespace-normalized),
so this file cannot silently drift from the dashboard.

Two rationales recur across the panels; read these first.

**Device timeline vs wall clock.** The simulator's virtual clock runs 180×
ahead of the wall clock starting from wall-now, so `measured_at` (device
time) is future-stamped, while `received_at` on `telemetry`/`dead_letter`
and `occurred_at` on `ingest_events` are wall-clock arrival times. Two
consequences:

- The dashboard's time axis is device time — the default range is
  `now-1h → now+7d` (into the future, deliberately), and staleness is
  measured against the fleet-wide `max(measured_at)` ("virtual now"), never
  against wall `now()`. The generous right edge is load-bearing: `now+X`
  advances at wall speed while the data advances 180× faster, so the live
  edge leaves the window after X/179 of wall time — 7 days keeps it
  in-window for ~56 wall-minutes, a full demo session.
- The meta-observability stat panels do the opposite: they ignore the
  dashboard time range entirely, because filtering wall-clock arrival
  columns by a virtual-time range would silently mix the two clocks. Totals
  are all-time; the rate panel uses an explicit wall-clock window.

**Raw vs continuous aggregate.** The headline water/battery panels read raw
`telemetry`: at demo volume (4 devices × one reading per 5 virtual minutes)
raw is effortless, and only raw preserves what this repo exists to show —
individual check-ins as points, visible gaps where check-ins were missed,
out-of-order structure. A second panel reads `telemetry_hourly` explicitly,
proving the M3 continuous aggregate live (real-time aggregation makes it
correct from the first reading) and staying cheap at any range on a
long-running stack. A single `$__interval`-bucketed panel would do neither
job well: it would never touch the aggregate and would erase the artifacts.

`$__timeFilter(col)` is Grafana's macro for "col between the dashboard's
from/to".

## Fleet row

### Water level per planter (raw)

```sql
SELECT measured_at AS "time", device_id, water_level
FROM telemetry
WHERE $__timeFilter(measured_at)
ORDER BY measured_at
```

Rendered with points always on and a connect-nulls threshold (`spanNulls`)
of 450 000 ms — 1.5× the 5-virtual-minute cadence. The threshold does two
jobs at once: Grafana's long-to-wide conversion interleaves each device's
points with nulls (the fleet is phase-staggered, so each timestamp carries
one device's value), which the threshold bridges — while any genuine gap
longer than 1.5 check-in intervals stays a visible break in the line, which
is exactly how the simulator's injected missed check-ins show up.

### Time since last check-in (device timeline)

```sql
SELECT d.device_id,
       EXTRACT(EPOCH FROM ((SELECT max(measured_at) FROM telemetry) - d.last_seen))::bigint
           AS behind_seconds
FROM devices d
ORDER BY d.device_id
```

`devices.last_seen` is maintained by ingestion in device time
(`GREATEST`-upsert, see [storage.md](storage.md)); subtracting it from the
fleet's newest reading compares like with like. Thresholds are calibrated to
the deep-sleep cadence (300 virtual seconds): green up to one missed wake-up
of jitter, amber from 600 s (two missed wake-ups), red from 1800 s (six).
The panel hard-codes that default cadence — retune the thresholds if
`SIM_INTERVAL_SECONDS` changes.

### Battery voltage per planter (raw)

```sql
SELECT measured_at AS "time", device_id, battery_voltage
FROM telemetry
WHERE $__timeFilter(measured_at)
ORDER BY measured_at
```

### Water level — hourly rollup (continuous aggregate)

```sql
SELECT bucket AS "time", device_id, water_avg
FROM telemetry_hourly
WHERE $__timeFilter(bucket)
ORDER BY bucket
```

Average only, to keep one series per device. For an envelope, add the
min/max columns the aggregate already carries:

```sql
SELECT bucket AS "time", device_id, water_avg, water_min, water_max
FROM telemetry_hourly
WHERE $__timeFilter(bucket)
ORDER BY bucket
```

## Pipeline meta-observability row

The slot beside the stat tiles holds M7's "attention needed" panel — it
lives in this row's grid but belongs to the [Forecasts row](#forecasts-row)
below, where its query is documented.

### Dead-lettered (total)

```sql
SELECT count(*) AS dead_lettered FROM dead_letter
```

### Dead-letter rate / min

```sql
SELECT round(count(*) * 60.0 / GREATEST(EXTRACT(EPOCH FROM (now() -
           GREATEST((SELECT min(received_at) FROM telemetry),
                    now() - INTERVAL '15 minutes'))), 60.0), 2) AS per_minute
FROM dead_letter
WHERE received_at > now() - INTERVAL '15 minutes'
```

Explicit wall-clock window (see above). At the simulator defaults
(~2.9 messages/s fleet-wide, 2% malformed) expect ~3.5/min. The
denominator is the window's *covered* wall time, not a flat 15 minutes:
clamping the window start to the stream's first arrival
(`min(received_at)` on `telemetry`) keeps the rate honest during the
first 15 minutes of a fresh stack — exactly the advertised demo window —
and the 60-second floor avoids divide-by-near-zero right at startup.
(`GREATEST` ignores NULLs, so an empty `telemetry` falls back to the
plain 15-minute window.)

### Deduplicated (total)

```sql
SELECT count(*) AS deduplicated FROM ingest_events WHERE event = 'deduplicated'
```

The idempotent insert absorbs duplicates silently; `ingest_events` is where
each absorbed redelivery leaves its trace (one row per dedupe, written by
ingestion — see [ingestion.md](ingestion.md)). The event log carries
`occurred_at`, so a rate-over-time panel is a `time_bucket` away if wanted.

### Out-of-order arrivals

```sql
SELECT count(*) AS out_of_order
FROM (SELECT measured_at,
             max(measured_at) OVER (PARTITION BY device_id ORDER BY received_at
                 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS newest_before
      FROM telemetry) arrivals
WHERE measured_at < newest_before
```

Derived, not tracked: a reading arrived out of order exactly when its device
timestamp is older than the newest device timestamp that had already
arrived for that device — arrival order is `received_at`, device order is
`measured_at`, and the window function compares the two. No ingestion
bookkeeping needed. The subquery scans the hypertable; trivial at demo
volume, and bounding it by a `received_at` window is the knob if a stack
runs for wall-weeks.

### Missed check-ins per hour

```sql
SELECT bucket AS "time", device_id, greatest(12 - sample_count, 0) AS missed
FROM telemetry_hourly
WHERE $__timeFilter(bucket)
  AND bucket > time_bucket(INTERVAL '1 hour', (SELECT min(measured_at) FROM telemetry))
  AND bucket < time_bucket(INTERVAL '1 hour', (SELECT max(measured_at) FROM telemetry))
ORDER BY bucket
```

The deep-sleep schedule predicts 12 readings per hour per device at the
default 5-virtual-minute cadence; the shortfall is the missed-check-in
count (the constant 12 is coupled to `SIM_INTERVAL_SECONDS`). Only full
hours count: the newest bucket is still filling and the oldest covers the
partial hour before the stream began, so both always undercount and would
render as false "missed" spikes — and both cutoffs come from the device
timeline (`min`/`max(measured_at)`), not the wall clock, for the usual
reason. Companion
to the visual gaps on the raw panels: the raw lines show *where* check-ins
went missing, this panel counts them per device per hour.

### Recent dead letters

```sql
SELECT received_at, topic, reason, left(encode(payload, 'escape'), 80) AS payload_prefix
FROM dead_letter
ORDER BY received_at DESC
LIMIT 10
```

`payload` is `bytea` — the corruption modes deliberately produce broken
UTF-8, so the raw bytes are rendered via `encode(..., 'escape')` and
truncated for display. The full payload stays in the table.

## Forecasts row

The M7 analytics service writes one forecast row per (device, kind,
device-time watermark); `forecasts_latest` is the newest per (device, kind)
with the horizon derived as `greatest(crosses_at - as_of, 0)` — clamped so
"already at target" reads as 0 days left, never negative. All three panels
anchor on device time (`as_of` IS `measured_at`-time), so `$__timeFilter`
applies where a time axis exists; the model and its honesty rules are in
[analytics.md](analytics.md).

### Attention needed (days left)

```sql
SELECT d.device_id,
       min(f.horizon_seconds) / 86400.0 AS days_left
FROM devices d
LEFT JOIN forecasts_latest f
       ON f.device_id = d.device_id
      AND f.status IN ('ok', 'at_or_below_target')
GROUP BY d.device_id
ORDER BY d.device_id
```

One tile per device: the soonest horizon across water and battery, in days
of device time. `LEFT JOIN` from `devices` so a device never vanishes from
an attention panel just because it has no fit yet — no qualifying forecast
renders as the panel's `noValue` text, "warming up", which is a status
(fresh device, or a just-refilled tank rebuilding history), not a gap; the
detail table shows the exact reason. Thresholds red < 1 day / amber < 3 /
green are calibrated to the analytics alert horizon
(`ANALYTICS_WATER_ALERT_DAYS`, default 2) — retune them together, the way
the check-in panel couples to `SIM_INTERVAL_SECONDS`.

### Forecast vs actual — $planter

The dashboard's first template variable selects the device:

```sql
SELECT device_id FROM devices ORDER BY device_id
```

Target A, the forecast horizon over its history (right axis, days):

```sql
SELECT as_of AS "time",
       'days until empty' AS metric,
       greatest(EXTRACT(EPOCH FROM (crosses_at - as_of)), 0)::double precision / 86400.0 AS value
FROM forecasts
WHERE device_id = ${planter:sqlstring}
  AND kind = 'water'
  AND status IN ('ok', 'at_or_below_target')
  AND $__timeFilter(as_of)
ORDER BY 1
```

Target B, the actual level (left axis, percent):

```sql
SELECT measured_at AS "time",
       'water level' AS metric,
       water_level AS value
FROM telemetry
WHERE device_id = ${planter:sqlstring}
  AND $__timeFilter(measured_at)
ORDER BY 1
```

This is the honesty panel: within one depletion segment the horizon falls at
exactly one day per day of device time (an integration test asserts it), and
it resets upward at every refill. Because simulated owners refill at 5–25 %
while the forecast target is 10 %, most planters get watered before the
horizon reaches zero — the forecast beaten by the owner is the correct
outcome, not a bug. `${planter:sqlstring}` (not `'$planter'`): Grafana's
`sqlstring` formatter quotes and escapes the value; the variable is sourced
from a database query and `grafana_reader` is read-only, but the formatter
is the actual control. `$__timeFilter(as_of)` is valid here because `as_of`
is device time — the same clock as the dashboard's axis.

### Forecast detail (latest per device and kind)

```sql
SELECT f.device_id, f.kind, f.status,
       round(f.latest_value::numeric, 2) AS latest,
       round(f.target_value::numeric, 2) AS target,
       round((f.slope_per_hour * 24.0)::numeric, 3) AS change_per_day,
       round((f.horizon_seconds / 86400.0)::numeric, 2) AS days_left,
       f.crosses_at, f.crosses_at_earliest, f.crosses_at_latest,
       f.fit_points,
       round((f.fit_span_seconds / 3600.0)::numeric, 1) AS fit_hours,
       f.as_of
FROM forecasts_latest f
ORDER BY f.horizon_seconds NULLS FIRST, f.device_id, f.kind
```

Statuses are displayed, so `insufficient_points` after a refill reads as an
answer rather than a hole. `crosses_at_earliest`/`latest` are the
distribution-free confidence band on the fitted slope (optimistically narrow
for water — see [analytics.md](analytics.md)). `target` is a column, not a
panel constant: a historical row stays interpretable if
`ANALYTICS_WATER_TARGET_PCT` is retuned. `NULLS FIRST` floats the devices
with no crossing to the top — they are the ones needing a look.
