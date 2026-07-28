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

![The committed sample window replayed through the broker while the dashboard fills](docs/img/demo.gif)

*Top: the committed sample window replayed at `--speed 100`, with
`mosquitto_sub` showing it cross the broker. Bottom: the provisioned dashboard
filling in — a timelapse of screenshots taken during that same replay.*

**Status: M7 — analytics layer (final planned milestone).** See
[planter-telemetry-plan.md](planter-telemetry-plan.md) for the full milestone plan.

## Architecture

```
ESP32 sensor pod (optional real hardware)  ─┐
                                            ├─► MQTT broker ─► Ingestion service ─► TimescaleDB ─► Grafana
Device simulator (default demo mode)       ─┘   (Mosquitto)    (typed Python)       (Postgres)  ▲  (provisioned)
                                                                                               │
                                                                          Analytics service ───┘
                                                                          (refill forecasts, alerts)
```

Everything runs via `docker compose up`, with a device simulator as the default
data source — real hardware is an optional backend, never a requirement. The
tour of what each component is for, and why it was chosen, is in
[docs/architecture.md](docs/architecture.md).

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

Finally, replay the committed sample window through the broker — the same
path live traffic takes, as fast as the broker will accept it:

```bash
docker compose exec -T ingestion uv run --no-sync planter-telemetry replay \
  --host mosquitto --no-delay - < samples/telemetry-window.jsonl
```

That finishes in about a second and adds 185 readings dated January 2026
(view them at `from=2026-01-01&to=2026-01-02`). Swap `--no-delay` for
`--speed 100` to watch it arrive paced instead, as in the GIF above — that
takes about three minutes of wall clock for the window's 5.3 virtual hours.

Run it a second time and nothing changes: readings are keyed by
`(device_id, measured_at)` and written with `ON CONFLICT DO NOTHING`, so
replay is idempotent by construction — and CI asserts exactly that
(`make replay-smoke`).

If you have mosquitto clients installed on your host,
`mosquitto_sub -h localhost -t 'planter/v1/+/telemetry' -v` works the same way
against port 1883, and `psql` reaches the database on `localhost:5433`
(user/password/db all `planter`). Tear down with `docker compose down` — data
persists in a named volume across restarts; `make down-clean` also removes it,
so the next `up` starts from an empty, freshly migrated database.

**Timings**, measured on an Apple M1 with a fast connection, from a genuinely
cold state — fresh clone, no images, no build cache:

| | Cold | Warm |
|---|---|---|
| `docker compose up -d --build` returns | 72 s | 9 s |
| all five services healthy | 80 s | — |
| dashboard showing live readings | **89 s** | **20 s** |

Cold means the ~2.8 GB of images (TimescaleDB, Grafana, Mosquitto, plus the
Python base) were pulled during that 72 s, so a slower connection moves this
number and little else. To reproduce the cold state:

```bash
docker compose down -v --rmi local        # or: make down-clean, to keep the images
docker image rm eclipse-mosquitto:2 timescale/timescaledb:2.28.3-pg17 \
  grafana/grafana:12.3.0 python:3.12-slim ghcr.io/astral-sh/uv:0.11.14
docker builder prune -f
```

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
| `SIM_HEARTBEAT_PATH` | `/tmp/planter-simulator-heartbeat` | file touched after each accepted publish; the compose healthcheck asserts it is fresh |

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

## Analytics

The headline feature: the dashboard tells you *when* to water, not that you
should have. A pure forecasting core splits each planter's history into
depletion segments at refill events, fits a robust line (Theil–Sen, stdlib
only — no numpy, deliberately) to the most recent segment, and extrapolates
to a configurable "effectively empty" level; the same core, with different
constants, forecasts battery days-until-charge from voltage decay. Forecasts
land in a `forecasts` table (append-only history, idempotent on the
per-device data-time watermark) and surface as the dashboard's
green/amber/red **attention panel**, a forecast-vs-actual panel that shows
the horizon ticking down in real time, and a detail table.

![Attention needed panel: per-planter days-until-empty with thresholds](docs/img/attention-panel.png)

Honesty is the design center: a fresh device, a short segment, or a
just-refilled tank yields an explicit no-forecast reason ("warming up" on
the panel), never an extrapolation from a handful of points — and CI
verifies predicted empty times against the simulator's *configured*
depletion rates within derived tolerances. All forecast arithmetic runs in
data time (`measured_at`), so "days until empty" means days of device time
under the accelerated demo clock and wall time on real hardware. Model,
tolerances, and caveats: [docs/analytics.md](docs/analytics.md).

Alerts fire on the **forecast horizon** ("empty in under 2 days"), not on
raw thresholds — and they fire as recorded decisions in an `alert_events`
table, with an opt-in [ntfy](https://ntfy.sh) push. Transitions only, with
hysteresis and a data-time cooldown, so a standing condition never spams.

Configuration (all `ANALYTICS_*` knobs, read by the analytics container):

| Variable | Default | Meaning |
|---|---|---|
| `ANALYTICS_DB_DSN` | `postgresql://planter:planter@localhost:5433/planter` | TimescaleDB connection (compose overrides host/port) |
| `ANALYTICS_INTERVAL_SECONDS` | `30` | wall-clock pause between analytics passes |
| `ANALYTICS_WATER_TARGET_PCT` | `10` | "effectively empty" level — wick delivery fails before 0 % |
| `ANALYTICS_BATTERY_TARGET_VOLTS` | `3.4` | charge-needed level, above the ~3.0 V working cutoff |
| `ANALYTICS_WATER_ALERT_DAYS` | `2` | fire when days-until-empty drops to this horizon |
| `ANALYTICS_BATTERY_ALERT_DAYS` | `3` | fire when days-until-charge drops to this horizon |
| `ANALYTICS_ALERT_COOLDOWN_HOURS` | `24` | data-time floor between repeat notifications for a standing condition |
| `ANALYTICS_NTFY_URL` | *(empty — disabled)* | ntfy topic URL to POST alerts to; decisions are recorded either way |
| `ANALYTICS_OPS_HOST` | `127.0.0.1` | bind address for `/healthz` + `/metrics` |
| `ANALYTICS_OPS_PORT` | `8081` | port for `/healthz` + `/metrics`; `0` binds an ephemeral port (used by tests) |

## Dashboard

Everything Grafana is provisioned as code — a reviewer never clicks "add
datasource" or "import dashboard", and Grafana's internal state is
deliberately ephemeral (no volume): `down`/`up` reproduces the identical
instance from these files alone.

| File | What it provisions |
|---|---|
| [`grafana/provisioning/datasources/timescaledb.yml`](grafana/provisioning/datasources/timescaledb.yml) | the TimescaleDB datasource (uid `planter-timescaledb`), connecting as the read-only `grafana_reader` role that migration `0007` creates |
| [`grafana/provisioning/dashboards/planter.yml`](grafana/provisioning/dashboards/planter.yml) | the file provider that loads every dashboard JSON from `grafana/dashboards/` (UI edits and deletion disabled — the repo is the source of truth) |
| [`grafana/dashboards/planter-fleet.json`](grafana/dashboards/planter-fleet.json) | the "Planter Fleet" dashboard: per-planter water/battery (raw + M3 hourly rollup), last-check-in staleness with green/amber/red thresholds, the pipeline meta-observability row, and the M7 forecast row (attention needed, forecast-vs-actual, forecast detail) |

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

## Ops

### Healthchecks

`docker compose ps` shows every long-running service healthy — and each check
proves the service's *dependency*, not merely a live process:

| Service | Healthy means |
|---|---|
| `mosquitto` | a real `mosquitto_pub` round-trip succeeds |
| `timescaledb` | `pg_isready` over TCP (initdb's socket-only phase can't pass early) |
| `ingestion` | its own `/healthz` returns 200: subscribed to the broker **and** writing to the database |
| `simulator` | the heartbeat file (`SIM_HEARTBEAT_PATH`) is fresher than 30 s — touched only after a publish accepted while connected |
| `analytics` | its own `/healthz` returns 200: the last analytics pass succeeded and completed recently |
| `grafana` | the provisioned datasource health check runs `SELECT 1` against TimescaleDB as `grafana_reader` |

`migrate` is one-shot and shows `Exited (0)` instead; ingestion and Grafana
gate on it via `service_completed_successfully`.

### Health and metrics endpoints

The ingestion and analytics services each serve two endpoints on a side port
(`INGEST_OPS_HOST:INGEST_OPS_PORT` and `ANALYTICS_OPS_HOST:ANALYTICS_OPS_PORT`,
container-internal by default), through one shared ops server
(`planter_telemetry/ops.py`) — each service supplies only what health
*means* for it:

- `GET /healthz` — `200 {"status": "healthy", ...}` when subscribed to the
  broker and writing to the database; `503 unhealthy` otherwise, with both
  connection flags in the body for diagnosis. There is deliberately no
  "degraded" state: the service has one reconnect path that tears down and
  rebuilds both connections together, so 503 simply covers "starting" and
  "reconnecting".
- `GET /metrics` — Prometheus text format: the ingestion counters
  (`planter_ingestion_{ingested,deduplicated,dead_lettered,out_of_order}_total`)
  plus process/platform basics. The metrics are a live view over the same
  in-memory counters the `stats` log event reports — not a second
  bookkeeping system; the SQL derivations behind the dashboard remain the
  source of truth. Wiring an actual Prometheus server is future work — the
  endpoint is scrape-ready.

For analytics, `/healthz` means "the last pass succeeded and isn't overdue"
(a service whose passes all fail while its loop spins is unhealthy), and
`/metrics` serves `planter_analytics_*` counters (passes, forecasts written,
alerts fired/cleared, notify failures) plus two honest gauges
(`last_pass_duration_seconds`, `devices_forecast`).

### Structured logs

Every service logs one JSON object per line on stderr. The message is a
constant event name; everything variable travels in fields:

```json
{"ts": "2026-07-27T12:00:00.123456+00:00", "level": "INFO", "service": "ingestion",
 "logger": "planter_telemetry.ingestion", "event": "stats",
 "ingested": 1412, "deduplicated": 63, "dead_lettered": 28, "out_of_order": 41}
```

`docker compose logs ingestion | grep '"event": "dead_letter"'` is a query,
not an archaeology session.

### Capture & replay

The `planter-telemetry` CLI (also `python -m planter_telemetry.cli`) captures
a live message window to newline-delimited JSON and replays it later —
through MQTT, never by writing to the database, so replayed traffic exercises
exactly the ingestion path live traffic takes:

```bash
# capture 100 messages (or stop after 60 s, whichever comes first)
uv run planter-telemetry capture --count 100 --duration 60 --out window.jsonl

# replay at 100x (default): captured gaps compressed, payloads byte-identical
uv run planter-telemetry replay window.jsonl

# firehose mode, reading from stdin
uv run planter-telemetry replay --no-delay - < window.jsonl
```

Payloads replay byte-identical — `measured_at` is *not* rewritten, because
rewriting would forge new readings and defeat the idempotency that makes
replay safe. A committed, deterministically generated sample window ships in
[`samples/telemetry-window.jsonl`](samples/telemetry-window.jsonl)
(3 devices, ~5.5 virtual hours, duplicates/garbage included), so the demo
works on a fresh clone with zero prior capture:

```bash
uv run planter-telemetry replay --no-delay - < samples/telemetry-window.jsonl
```

Run it twice: the second pass changes nothing — that is the idempotency
story, and CI asserts it (`make replay-smoke`: all services healthy → replay
→ row count and checksum → replay again → checksum unchanged → the window is
visible through Grafana).

## Real hardware (opt-in)

Demo mode keeps the anonymous, loopback-bound broker untouched; hardware mode
is a compose override that adds a second, password- and ACL-protected listener
on LAN port 1884 to the same broker — so real pods and the simulator feed one
identical pipeline with zero application-code changes.

```bash
make hardware-passwd DEVICE=planter-a4cf12   # create a device credential
make hardware-up                             # demo stack + LAN listener on 1884
```

The default quickstart above never changes, whether or not you use this.
[docs/hardware-bridge.md](docs/hardware-bridge.md) is the contract the firmware
must meet: topics, payload schema, credentials, deep-sleep guidance, and a
"first successful reading" checklist.

## Documentation

- [architecture.md](docs/architecture.md) — diagram, component rationale, the idempotency story (5-minute read)
- [message-contract.md](docs/message-contract.md) — wire format, topic tree, versioning policy
- [ingestion.md](docs/ingestion.md) — consumer layering, delivery semantics, failure behavior
- [storage.md](docs/storage.md) — hypertable, continuous aggregates, migrations
- [analytics.md](docs/analytics.md) — the forecast model, its derived tolerances, and its honesty rules
- [dashboard-queries.md](docs/dashboard-queries.md) — every panel's SQL, with rationale
- [hardware-bridge.md](docs/hardware-bridge.md) — pointing a real ESP32 pod at the stack

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
make sample       # regenerate samples/telemetry-window.jsonl (guarded by tests/test_sample.py)
make replay-smoke # compose up → all healthy → replay the sample twice → idempotency proven
make analytics-smoke # clean volume → forecasts from live data → alert fires → re-pass is a no-op
make link-check   # docs: no dead relative links or anchors (offline)
make hardware-config-check  # hardware-mode compose merge + broker config boot test
```

## Future work

- **Anomaly detection.** Leak, wick-failure, and sensor-fault baselines on
  top of the same per-device history — new consumers of the segments the
  M7 forecaster already computes.
- **A better depletion model.** The linear-per-segment fit is deliberate
  (see [docs/analytics.md](docs/analytics.md)); a LiPo voltage-to-charge
  curve or temperature-aware uptake model slots into one function without
  touching segmentation, gating, or the tests' structure.
- **Closed-loop auto-watering** — the "write path" companion to this repo's
  read path: acting on a forecast instead of reporting it. Deliberately out
  of scope here; it belongs in its own project with its own safety story.

## License

[MIT](LICENSE)
