# Storage layer

The M3 centerpiece: TimescaleDB as a showcase rather than a sink. Alembic
migrations own the schema, telemetry is a hypertable with hourly/daily
continuous aggregates on top, a columnstore policy handles aging, and a
device registry tracks the fleet — all reproducible from an empty database
in CI.

## Schema

```
                          ┌────────────────────────────────┐
 MQTT ─► ingestion ──────►│ telemetry  (hypertable)        │
             │            │  1-day chunks on measured_at   │
             │            │  UNIQUE (device_id,measured_at)│
             │            │  columnstore after 7 days      │
             │            └───────────────┬────────────────┘
             │                            │ continuous aggregates
             │                  ┌─────────┴─────────┐
             │                  ▼                   ▼
             │          telemetry_hourly     telemetry_daily
             │          (real-time, per-device avg/min/max
             │           water, avg/min battery, count)
             │
             ├──────────► devices       (device_id PK, first_seen,
             │                           last_seen, metadata jsonb)
             ├──────────► dead_letter   (raw payload + reason)
             └──────────► ingest_events (dedupe trace, M4)
```

`telemetry` and `dead_letter` are unchanged from M2; the registry and the
Timescale machinery are M3. There is deliberately no foreign key from
`telemetry` to `devices`: it would couple insert ordering to the registry for
zero payoff — ingestion writes the registry row first in the same handler.

## Migrations: Alembic, raw SQL, forward-only

Migrations live in
[`planter_telemetry.migrations`](../src/planter_telemetry/migrations/) (inside
the package, so they ship in the image) and are plain SQL inside
`op.execute()` — Alembic provides the ordering, version table, and stamping
machinery; SQL stays reviewable SQL. Conventions, enforced by unit tests
where possible:

- **Sequential ids** (`0001`, `0002`, …), exactly one head. New revisions:
  `uv run alembic revision --rev-id NNNN -m "slug"`.
- **Never edited after merge.** A wrong migration is fixed by the next one.
- **Forward-only past 0003.** Hypertable conversion is one-way short of a
  table rebuild (and disabling the columnstore refuses to run once chunks
  are compressed); downgrade raises rather than pretending.
- **Non-transactional revisions are re-runnable.** Continuous-aggregate and
  policy DDL must run in an autocommit block, where each statement commits
  while `alembic_version` still points at the previous revision — so a
  migrate container killed partway must be able to re-run the revision on
  the next `up`. Every such statement is guarded (`IF NOT EXISTS` /
  `if_not_exists => true`).

The compose stack runs a one-shot `migrate` service before ingestion starts;
CI's testcontainers fixture runs the identical code path
([`planter_telemetry.migrate`](../src/planter_telemetry/migrate.py)), so
every integration test doubles as a migrate-from-empty test.

**The stamp guard:** migration `0001` reproduces the M2 init script exactly,
and a database that has `telemetry` but no `alembic_version` is stamped at
`0001` before upgrading. A pre-M3 database therefore migrates cleanly with
its rows carried into chunks (`migrate_data => true`), and a from-scratch
database is indistinguishable from an upgraded one — both facts are
CI-asserted, not aspirational.

Since M3 the database keeps its data in a named volume across restarts;
`make down-clean` drops it for the pristine-demo behavior.

## Hypertable: 1-day chunks, sized for the demo, not for throughput

At the demo shape (4 devices × one reading per 5 virtual minutes ≈ 1,150
rows per virtual day) any chunk interval performs fine — so the interval is
chosen for legibility: with the simulator's 180× clock a new 1-day chunk
appears every ~8 wall-minutes, chunk machinery is visible within a single
compose session, and chunks align 1:1 with the daily aggregate's buckets.
The trade-off (thousands of tiny chunks if a stack runs for wall-weeks) is
the right one for a demo repo and would be the first knob revisited for a
real fleet.

The M2 idempotency contract survives conversion untouched:
`UNIQUE (device_id, measured_at)` includes the partition column, which is
exactly what a hypertable requires, and `ON CONFLICT ... DO NOTHING`
behaves identically against chunks.

## Continuous aggregates: real-time on purpose

`telemetry_hourly` and `telemetry_daily` roll up per-device water
(avg/min/max), battery (avg/min), and reading counts. Two decisions matter
more than the SQL:

- **`materialized_only = false` is load-bearing, not a nicety.** The
  simulator's virtual clock runs 180× ahead of the wall clock, so
  `measured_at` values are in the *future* — and refresh policies compute
  their windows relative to wall-clock `now()`, so the background jobs
  materialize little or nothing during a demo session. Real-time
  aggregation unions the materialized region with fresh raw rows at query
  time, which is what makes the rollups correct and queryable the moment
  data arrives. (Since TimescaleDB 2.13 the default is materialized-only;
  the migration sets this explicitly.) The refresh policies still exist as
  the production posture — on a long-running stack they steadily
  materialize history.
- **Daily aggregates raw telemetry, not the hourly view.** An average of
  hourly averages is only correct when weighted by per-hour counts; at this
  volume the simple exact form costs nothing.

Tests refresh with `CALL refresh_continuous_aggregate(view, NULL, NULL)`
(a procedure — autocommit connection required) and assert both paths, before
and after refresh, against Python-computed expectations.

## Columnstore yes, retention no

Raw telemetry converts to columnstore 7 days after its timestamps age past
the wall clock, segmented by `device_id` (every analytical query filters or
groups by device) and ordered by `measured_at DESC`. Because demo data is
future-stamped, compression can never eat into a live session — to watch it
act immediately, convert the chunks by hand (`CALL` takes no subquery
arguments, hence the loop):

```sql
DO $$
DECLARE chunk regclass;
BEGIN
    FOR chunk IN SELECT show_chunks('telemetry') LOOP
        CALL convert_to_columnstore(chunk);
    END LOOP;
END $$;
```

There is **no retention policy**, deliberately. The history *is* the demo —
replay, dashboards, and the M7 forecasts all feed on it — and a wall-clock
retention policy against future-stamped data would be a silent data-loss
trap. Compression is the aging story here; a production deployment would add
one line:

```sql
SELECT add_retention_policy('telemetry', drop_after => INTERVAL '1 year');
```

## Device registry

`devices` is maintained by ingestion with a single upsert per valid reading
(unconditionally — it is idempotent, it guarantees the device row exists
before its reading, and a redelivery's identical `measured_at` makes it a
no-op on the registry):

```sql
INSERT INTO devices (device_id, first_seen, last_seen)
VALUES (%s, %s, %s)
ON CONFLICT (device_id) DO UPDATE SET
    first_seen = LEAST(devices.first_seen, EXCLUDED.first_seen),
    last_seen  = GREATEST(devices.last_seen, EXCLUDED.last_seen)
```

`LEAST`/`GREATEST` make out-of-order arrival correct by construction, with
no read-modify-write. Timestamps are device time (`measured_at`), not
arrival time, so M4's gap detection compares like with like on the device
timeline. Per-message (rather than throttled) updates are the simplest
correct thing at ~2–4 msg/s fleet-wide; M5's replay-at-speed turned out to
be absorbed comfortably by the same row-at-a-time path (CI's replay-smoke
pushes the whole sample window through in firehose mode), so micro-batching
remains the documented upgrade path if scale ever demands it, not something
the current load justifies. `metadata jsonb` is room for M4+
(display names, location) without a migration per attribute.

## Example queries

Paste into `psql postgresql://planter:planter@localhost:5433/planter` while
the stack runs.

Fleet snapshot — latest state per device via Timescale's `last()`:

```sql
SELECT d.device_id,
       round(last(t.water_level, t.measured_at)::numeric, 1)     AS water_pct,
       round(last(t.battery_voltage, t.measured_at)::numeric, 2) AS battery_v,
       max(t.measured_at)                                        AS last_reading,
       d.first_seen
FROM devices d
JOIN telemetry t USING (device_id)
GROUP BY d.device_id, d.first_seen
ORDER BY d.device_id;
```

Daily water consumption per device, refill-aware — summing only downward
steps between consecutive readings, so refill jumps don't cancel consumption
(the naive `first() - last()` delta goes wrong the moment a tank is topped
up mid-window):

```sql
SELECT device_id,
       time_bucket('1 day', measured_at)::date        AS day,
       round(sum(greatest(prev_level - water_level, 0))::numeric, 1) AS pct_consumed
FROM (
    SELECT device_id, measured_at, water_level,
           lag(water_level) OVER (PARTITION BY device_id ORDER BY measured_at) AS prev_level
    FROM telemetry
) steps
GROUP BY device_id, day
ORDER BY day, device_id;
```

Check-in health from the hourly rollup — hours where a device delivered
fewer readings than its deep-sleep cadence predicts (12/hour at the
5-virtual-minute default); the seed of the dashboard's missed-check-in
panel (the productionized variant, along with every other panel query,
lives in [docs/dashboard-queries.md](dashboard-queries.md)). The
newest bucket is excluded: it is still filling, so it always undercounts —
and the cutoff comes from the device timeline (`max(measured_at)`), not the
wall clock, because demo data is future-stamped:

```sql
SELECT device_id, bucket, sample_count
FROM telemetry_hourly
WHERE sample_count < 12
  AND bucket < time_bucket(INTERVAL '1 hour', (SELECT max(measured_at) FROM telemetry))
ORDER BY bucket DESC, device_id
LIMIT 20;
```

Storage introspection — the chunks backing the hypertable and their
columnstore state:

```sql
SELECT chunk_name, range_start, range_end, is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'telemetry'
ORDER BY range_start;
```

## Proof

CI-verified against a real TimescaleDB (testcontainers, `make integration`):

- an empty database migrates to head and lands in the asserted end state
  (hypertable with 1-day chunks, both real-time aggregates with refresh
  policies, columnstore enabled, no retention job, the M2 unique
  constraint intact);
- migrating twice is a no-op;
- a migrate run killed partway through the non-transactional 0004 (view
  already created, version row still at 0003) recovers: the next run
  reaches head with the full end state;
- a database at `0001` is column-for-column, constraint-for-constraint
  identical to one built by the M2 init script;
- a pre-Alembic M2 database with existing rows is stamped and upgraded, its
  rows surviving into chunks;
- hourly and daily rollups match Python-computed expectations on the seeded
  stream — both through the real-time path (no refresh) and after an
  explicit refresh — and a hand-computed hourly window comes out exact;
- replaying a stream changes neither telemetry nor the rollups;
- the registry converges on min/max `measured_at` under out-of-order
  arrival and replay, one row per device, and malformed messages never
  touch it;
- every M2 delivery-semantics test runs unchanged against the migrated
  schema.
