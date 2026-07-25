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

**Status: M1 — message contract + simulator.** See
[planter-telemetry-plan.md](planter-telemetry-plan.md) for the full milestone plan.

## Planned architecture

```
ESP32 sensor pod (optional real hardware)  ─┐
                                            ├─► MQTT broker ─► Ingestion service ─► TimescaleDB ─► Grafana
Device simulator (default demo mode)       ─┘   (Mosquitto)    (typed Python)       (Postgres)     (provisioned)
```

Everything will run via `docker compose up`, with a device simulator as the default
data source — real hardware is an optional backend, never a requirement.

## Quickstart (what works today)

Start the MQTT broker and the device simulator:

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
readings, and corrupted payloads (the failure modes the ingestion pipeline must
survive, arriving in M2):

```
planter/v1/planter-02/telemetry {"schema_version":1,"device_id":"planter-02","measured_at":"2026-07-25T11:01:18.750666Z","water_level":89.4,"battery_voltage":3.899}
planter/v1/planter-00/telemetry {"schema_version":1,"device_id":"planter-00","measured_at":"2026-07-25T11:08:28.538817Z","water_level":60.6,"battery_voltage":4.099}
planter/v1/planter-00/telemetry {"schema_version":1,"device_id":"planter-00","measured_at":"2026-07-25T11:03:29.111993Z","water_level":60.8,"battery_voltage":4.093}
planter/v1/planter-00/telemetry {"schema_version":1,"device_id":"planter-0
```

If you have mosquitto clients installed on your host,
`mosquitto_sub -h localhost -t 'planter/v1/+/telemetry' -v` works the same way
against port 1883. Tear down with `docker compose down`.

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

## Development

Dependencies are managed with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
```

Then the usual targets:

```bash
make lint       # ruff format --check + ruff check
make typecheck  # mypy --strict
make test       # pytest
```

## License

[MIT](LICENSE)
