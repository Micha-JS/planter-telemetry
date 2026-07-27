# planter-telemetry

An end-to-end IoT telemetry pipeline for an ESP32 planter sensor pod. Off-the-shelf
platforms like ThingsBoard, Home Assistant, and Datacake already solve "chart my
sensor" — this repo is not a product, it opens the box those platforms hide: a typed,
tested, replayable ingestion pipeline (MQTT → validated idempotent writes →
TimescaleDB → provisioned Grafana) with visible data-quality handling. On top of the
pipeline: **refill forecasting** (predicts *when* each planter runs dry, not just that
a threshold was crossed), **pipeline meta-observability** (dashboards about the data
itself — dedupes, dead-letters, out-of-order arrivals, missed check-ins), and
**replay/time-travel** (re-run any historical window at speed, safe because ingestion
is idempotent).

**Status: M4 — dashboard.** See
[planter-telemetry-plan.md](planter-telemetry-plan.md) for the full milestone plan.

## Architecture

```
ESP32 sensor pod (optional real hardware)  ─┐
                                            ├─► MQTT broker ─► Ingestion service ─► TimescaleDB ─► Grafana
Device simulator (default demo mode)       ─┘   (Mosquitto)    (typed Python)       (Postgres)     (provisioned)
```

Everything runs via `docker compose up`, with a device simulator as the default
data source — real hardware is an optional backend, never a requirement.

## Quickstart

Start the broker, the device simulator, the ingestion service, TimescaleDB,
and Grafana:

```bash
docker compose up -d --build
```

Watch the telemetry stream with the clients bundled in the Mosquitto image (no host
installs needed):

```bash
docker compose exec mosquitto mosquitto_sub -t 'planter/v1/+/telemetry' -v
```

Within a minute you'll see four planters publishing JSON readings with visibly
falling water levels — plus, by design, occasional duplicates, stale out-of-order
readings, and corrupted payloads (the failure modes the ingestion pipeline visibly
survives):

```
planter/v1/planter-02/telemetry {"schema_version":1,"device_id":"planter-02","measured_at":"2026-07-25T11:01:18.750666Z","water_level":89.4,"battery_voltage":3.899}
planter/v1/planter-00/telemetry {"schema_version":1,"device_id":"planter-00","measured_at":"2026-07-25T11:08:28.538817Z","water_level":60.6,"battery_voltage":4.099}
planter/v1/planter-00/telemetry {"schema_version":1,"device_id":"planter-00","measured_at":"2026-07-25T11:03:29.111993Z","water_level":60.8,"battery_voltage":4.093}
planter/v1/planter-00/telemetry {"schema_version":1,"device_id":"planter-0
```

Watch validated rows land in the database (duplicates are absorbed by the
idempotent insert; corrupted payloads divert to `dead_letter` instead):

```bash
docker compose exec timescaledb psql -U planter -d planter \
  -c "SELECT count(*) FROM telemetry" \
  -c "SELECT topic, reason, received_at FROM dead_letter ORDER BY id DESC LIMIT 3"
```

Query the per-device rollups (continuous aggregates, live from the first
reading):

```bash
docker compose exec timescaledb psql -U planter -d planter \
  -c "SELECT device_id, bucket, round(water_avg::numeric,1) AS water_avg, sample_count
      FROM telemetry_hourly ORDER BY bucket DESC, device_id LIMIT 8"
```

Then open the dashboard — no login, no setup, no clicking "add datasource":

**<http://localhost:3000>**

![Planter Fleet dashboard](docs/img/dashboard.png)

Within ~2 minutes of `up` the charts are visibly moving: the simulator's
clock runs 180× ahead of the wall clock, so every wall-minute adds three
virtual hours of per-planter water/battery history — plus the
meta-observability row, where the pipeline shows off what it survived
(dead-lettered garbage, absorbed duplicates, out-of-order arrivals, missed
check-ins). The default time range reaches into the *future* (`now → now+7d`)
for the same reason; details in
[docs/dashboard-queries.md](docs/dashboard-queries.md).

If you have mosquitto clients installed on your host,
`mosquitto_sub -h localhost -t 'planter/v1/+/telemetry' -v` works the same way
against port 1883, and `psql` reaches the database on `localhost:5433`
(user/password/db all `planter`). Tear down with `docker compose down` — data
persists in a named volume across restarts; `make down-clean` also removes it,
so the next `up` starts from an empty, freshly migrated database.

The wire format, topic tree, versioning policy, and idempotency key are documented
in [docs/message-contract.md](docs/message-contract.md).

## Simulator

The simulator publishes N fake planters with realistic per-device dynamics: water
levels deplete slowly and monotonically between occasional refill jumps, batteries
decay over virtual days, and devices wake on a deep-sleep cadence. It is
deliberately adversarial — duplicates, out-of-order deliveries, malformed payloads,
and missed check-ins are injected at configurable rates so the downstream pipeline
has something to prove.

Generation is pure and seeded: the same `SIM_SEED` produces the identical message
stream, which is what makes the ingestion idempotency tests (M2) and forecast
tests (M7) reproducible. Time runs accelerated so a demo shows movement within
minutes; simulated timestamps run ahead of the wall clock as a result.

Configuration via environment variables (set them on the `simulator` service in
`docker-compose.yml`):

| Variable | Default | Meaning |
|---|---|---|
| `SIM_DEVICE_COUNT` | `4` | number of fake planters (`planter-00`, `planter-01`, …) |
| `SIM_SEED` | `42` | master seed; same seed → identical stream |
| `SIM_ACCELERATION` | `180` | virtual seconds per wall-clock second (180 → one virtual day per 8 wall minutes) |
| `SIM_INTERVAL_SECONDS` | `300` | deep-sleep cadence between wake-ups, in virtual seconds |
| `SIM_MQTT_HOST` | `localhost` | broker host (`mosquitto` inside compose) |
| `SIM_MQTT_PORT` | `1883` | broker port |
| `SIM_DUPLICATE_RATE` | `0.05` | chance per wake-up of re-publishing the previous payload |
| `SIM_OUT_OF_ORDER_RATE` | `0.03` | chance per wake-up of a reading being delivered 1–3 wake-ups late |
| `SIM_MALFORMED_RATE` | `0.02` | chance per wake-up of a corrupted payload (rotating: truncated JSON, wrong types, missing fields) |
| `SIM_MISSED_CHECKIN_RATE` | `0.03` | chance per wake-up of the device skipping a check-in entirely |

## Ingestion

The ingestion service subscribes to the telemetry topic tree (QoS 1, persistent
session), validates every message against the shared Pydantic contract, and writes
idempotently to TimescaleDB: `INSERT … ON CONFLICT (device_id, measured_at) DO
NOTHING`, so replaying any stream twice changes nothing. Messages that fail
validation — truncated JSON, out-of-range values, naive timestamps, a topic whose
device id contradicts the payload — land in a `dead_letter` table with the raw
payload and a reason, never crash the service, and never disappear silently.
Design and delivery-semantics notes: [docs/ingestion.md](docs/ingestion.md).

Configuration via environment variables (set them on the `ingestion` service in
`docker-compose.yml`):

| Variable | Default | Meaning |
|---|---|---|
| `INGEST_MQTT_HOST` | `localhost` | broker host (`mosquitto` inside compose) |
| `INGEST_MQTT_PORT` | `1883` | broker port |
| `INGEST_CLIENT_ID` | `planter-ingestion` | stable MQTT client id; the broker keeps the QoS 1 session across restarts |
| `INGEST_DB_DSN` | `postgresql://planter:planter@localhost:5433/planter` | TimescaleDB connection string (default matches the compose stack's host port) |
| `INGEST_RECONNECT_INITIAL_SECONDS` | `1` | first reconnect delay after broker/DB loss |
| `INGEST_RECONNECT_MAX_SECONDS` | `30` | reconnect backoff cap |
| `INGEST_STATS_INTERVAL_SECONDS` | `30` | cadence of the `stats` counter log event |
| `INGEST_OPS_HOST` | `127.0.0.1` | bind address for `/healthz` + `/metrics` (loopback: the compose healthcheck runs in-container) |
| `INGEST_OPS_PORT` | `8080` | port for `/healthz` + `/metrics`; `0` binds an ephemeral port (used by tests) |

## Storage

The database is a TimescaleDB hypertable with hourly/daily per-device rollups
(continuous aggregates), a columnstore policy for aging data, and a device
registry maintained by ingestion. Schema changes are Alembic migrations —
raw SQL, sequentially numbered, run automatically by a one-shot `migrate`
service before ingestion starts, and proven in CI from an empty database.
Design, rationale, and pasteable analytical queries:
[docs/storage.md](docs/storage.md).

## Dashboard

Everything Grafana is provisioned as code — a reviewer never clicks "add
datasource" or "import dashboard", and Grafana's internal state is
deliberately ephemeral (no volume): `down`/`up` reproduces the identical
instance from these files alone.

| File | What it provisions |
|---|---|
| [`grafana/provisioning/datasources/timescaledb.yml`](grafana/provisioning/datasources/timescaledb.yml) | the TimescaleDB datasource (uid `planter-timescaledb`), connecting as the read-only `grafana_reader` role that migration `0007` creates |
| [`grafana/provisioning/dashboards/planter.yml`](grafana/provisioning/dashboards/planter.yml) | the file provider that loads every dashboard JSON from `grafana/dashboards/` (UI edits and deletion disabled — the repo is the source of truth) |
| [`grafana/dashboards/planter-fleet.json`](grafana/dashboards/planter-fleet.json) | the "Planter Fleet" dashboard: per-planter water/battery (raw + M3 hourly rollup), last-check-in staleness with green/amber/red thresholds, and the pipeline meta-observability row |

Anonymous viewer access is enabled (it's a local demo bound to loopback);
editing requires the `admin` user. Every panel query is documented and
test-enforced in [docs/dashboard-queries.md](docs/dashboard-queries.md), and
`tests/test_dashboard.py` lints the JSON structurally (datasource references
by provisioned uid, no `${DS_*}` import variables, demo-tuned time range).
The compose healthcheck marks Grafana healthy only once it can run the
datasource health check — an actual `SELECT 1` against TimescaleDB as
`grafana_reader` — and CI's `make grafana-smoke` asserts the provisioned
datasource and dashboard exist and that a query through Grafana sees a
nonzero telemetry count.

Configuration via environment variables (read by `docker-compose.yml`; the
`GRAFANA_DB_*` pair is interpolated by Grafana into the datasource yaml):

| Variable | Default | Meaning |
|---|---|---|
| `GRAFANA_ADMIN_PASSWORD` | `planter` | password of the `admin` user (host env var, dev-only default) |
| `GRAFANA_DB_USER` | `grafana_reader` | database role the datasource connects as |
| `GRAFANA_DB_PASSWORD` | `grafana_reader` | its password (dev-only, created by migration `0007`) |

## Development

Dependencies are managed with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
```

Then the usual targets:

```bash
make lint         # ruff format --check + ruff check
make typecheck    # mypy --strict
make test         # pytest (unit tests)
make integration  # pytest -m integration: real broker + TimescaleDB via testcontainers
```

## License

[MIT](LICENSE)
