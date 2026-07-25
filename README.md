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

**Status: M0 — scaffold.** See [planter-telemetry-plan.md](planter-telemetry-plan.md)
for the full milestone plan.

## Planned architecture

```
ESP32 sensor pod (optional real hardware)  ─┐
                                            ├─► MQTT broker ─► Ingestion service ─► TimescaleDB ─► Grafana
Device simulator (default demo mode)       ─┘   (Mosquitto)    (typed Python)       (Postgres)     (provisioned)
```

Everything will run via `docker compose up`, with a device simulator as the default
data source — real hardware is an optional backend, never a requirement.

## Quickstart (what works today)

Start the MQTT broker:

```bash
docker compose up -d
```

Smoke-test it with the clients bundled in the Mosquitto image (no host installs
needed). In one terminal, subscribe:

```bash
docker compose exec mosquitto mosquitto_sub -t 'planter/#' -v
```

In another, publish:

```bash
docker compose exec mosquitto mosquitto_pub -t planter/smoke -m hello
```

The subscriber prints `planter/smoke hello`. If you have mosquitto clients installed
on your host, `mosquitto_sub -h localhost -t 'planter/#' -v` works the same way
against port 1883. Tear down with `docker compose down`.

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
