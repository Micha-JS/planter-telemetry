# planter-telemetry — milestone plan

Public portfolio repo #2: an end-to-end IoT telemetry pipeline for the ESP32 planter
sensor pod. Showcases streaming/IoT data engineering: MQTT ingestion, idempotent
writes, time-series storage, config-as-code dashboards, and a small analytics layer.

**Companion to** the energy-platform flagship. Same engineering standards, deliberately
different stack surface (async MQTT consumer, TimescaleDB, Grafana) so the two repos
demonstrate complementary skills.

## Positioning

Off-the-shelf platforms (ThingsBoard, Home Assistant, Datacake) already solve
"chart my sensor." This repo is not a product — it opens the box those platforms
hide: a typed, tested, replayable ingestion pipeline with visible data-quality
handling. The README says this explicitly.

Differentiators over standard apps:

- **Refill forecasting** — per-planter depletion model predicts *when* each planter
  runs dry ("empty in ~4 days"), not just that a threshold was crossed. Alerts fire
  on forecast horizon, not raw level.
- **Pipeline meta-observability** — dashboard panels about the data itself:
  deduplicated duplicates, dead-lettered messages, out-of-order arrivals, missed
  check-ins vs expected deep-sleep schedule.
- **Replay/time-travel** — re-run any historical window through the pipeline at
  accelerated speed; safe because ingestion is idempotent and deterministic.

## Architecture

```
ESP32 sensor pod (optional real hardware)  ─┐
                                            ├─► MQTT broker ─► Ingestion service ─► TimescaleDB ─► Grafana
Device simulator (default demo mode)       ─┘   (Mosquitto)    (typed Python)       (Postgres)     (provisioned)
```

Everything runs via `docker compose up`. The simulator is the default data source;
real hardware is an optional backend, never a requirement.

## Stack decisions

| Concern | Choice | Rationale |
|---|---|---|
| Broker | Eclipse Mosquitto | Industry standard, tiny, zero config to start |
| Ingestion | Python 3.12+, `aiomqtt`, Pydantic v2 | Async consumer; typed, versioned message schema |
| Storage | TimescaleDB (Postgres extension) | Showcases SQL: hypertables, continuous aggregates, `ON CONFLICT` idempotency; no new query language |
| Migrations | Alembic (or plain SQL migrations) | Runs clean from empty DB in CI |
| Dashboard | Grafana, fully provisioned as code | Industry-standard IoT observability; zero manual clicks; keeps Python surface focused on ingestion |
| Analytics | Python job/service over TimescaleDB | Rolling per-device fits; small and honest, room to grow |
| Orchestration | Docker Compose | One-command demo |

## Engineering standards (all milestones)

- Typed Python: `mypy --strict`, `ruff` lint + format.
- Tests with `pytest`; every milestone lands with tests for its scope.
- GitHub Actions CI green from M0 onward; branch-per-milestone + PR workflow.
- Idempotent ingestion: replaying any message stream twice produces identical DB state.
- Synthetic demo mode is first-class: fresh clone → `docker compose up` → working
  dashboard, no hardware, no manual setup.
- Malformed input is captured (dead-letter), never silently dropped and never a crash.

## Message contract (established in M1, referenced throughout)

- Versioned Pydantic schema, e.g. `schema_version`, `device_id`, `measured_at` (UTC),
  `water_level`, `battery_voltage`, optional extras.
- Topic convention: `planter/v1/{device_id}/telemetry` (exact shape decided in M1).
- Natural key for idempotency: `(device_id, measured_at)`.

---

## Milestones

### M0 — Scaffold

Repo skeleton, tooling, CI, compose skeleton.

Scope:
- `pyproject.toml` with ruff, mypy (strict), pytest configured.
- GitHub Actions: lint + typecheck + tests on push/PR.
- `docker-compose.yml` with Mosquitto only; broker config committed.
- README stub with positioning statement and planned architecture.

**Done when:** CI is green on main; `docker compose up` starts a broker that
`mosquitto_pub`/`mosquitto_sub` can talk to.

### M1 — Message contract + simulator

The synthetic demo backbone. The simulator is deliberately adversarial: it produces
the failure modes the pipeline must visibly survive.

Scope:
- Pydantic telemetry schema (versioned) + topic naming convention, documented.
- Simulator container: N configurable fake planters with realistic dynamics —
  slow water depletion, refill jumps, battery decay, deep-sleep publish intervals.
- Injected imperfections at configurable rates: duplicate messages, out-of-order
  delivery, malformed payloads.
- Unit tests: schema round-trip, simulator dynamics sanity (levels bounded,
  depletion monotonic between refills).

**Done when:** `docker compose up` shows a plausible multi-device message stream via
`mosquitto_sub`, including occasional duplicates/garbage; schema and simulator tests
pass in CI.

### M2 — Ingestion service

The centerpiece. Async MQTT consumer → validated, idempotent writes.

Scope:
- `aiomqtt` consumer subscribing to the telemetry topic tree.
- Validation against the Pydantic schema; valid rows written to TimescaleDB with
  `INSERT ... ON CONFLICT (device_id, measured_at) DO NOTHING`.
- Dead-letter table for malformed/unparseable messages (raw payload + reason + topic).
- Graceful startup/shutdown; reconnect on broker loss.
- Tests: idempotency (same stream ingested twice → identical row counts), malformed
  handling (garbage lands in dead-letter, service keeps running), integration test
  against real Postgres + broker in CI (compose or testcontainers).

**Done when:** rows land in the DB from the live simulator; the replay-twice test
proves zero duplicates; malformed messages appear in the dead-letter table; all of
it verified in CI.

### M3 — Storage layer

Make the database a showcase, not just a sink.

Scope:
- Hypertable for telemetry; migrations run clean from an empty database.
- Continuous aggregates: hourly (and daily) per-device rollups.
- Retention/compression policy on raw data (generous, but present and documented).
- Device registry table (id, name, first_seen, metadata) maintained by ingestion.
- Tests: migration from scratch in CI; aggregate correctness on known input.

**Done when:** hourly aggregates are queryable and correct; CI spins up an empty
Postgres, migrates, and passes.

### M4 — Dashboard

Grafana, fully provisioned as code — zero manual clicks.

Scope:
- Grafana container with provisioned datasource + dashboards (JSON/YAML in repo).
- Panels: water level per planter, battery voltage, last-seen / gap detection.
- Meta-observability panels: dedupe count, dead-letter count/rate, out-of-order
  arrivals, missed expected check-ins.
- Compose healthchecks so Grafana waits for the DB.

**Done when:** fresh clone → `docker compose up` → browser shows live dashboards fed
by synthetic data, with no manual configuration whatsoever.

### M5 — Ops polish + replay

Scope:
- Health endpoints on the ingestion service; structured (JSON) logging.
- Compose healthchecks across all services.
- CLI: capture a message window to file; replay from file through the broker at
  configurable speed (e.g. 100×).
- Optional (only if it stays small): Prometheus metrics endpoint on ingestion.

**Done when:** `docker compose ps` shows all services healthy; a captured window
replayed at speed flows end-to-end and — because of idempotency — replaying it again
changes nothing.

### M6 — Real-hardware bridge + docs

Scope:
- Doc: connecting the real ESP32 pod (broker config, topic mapping from existing
  firmware, auth notes). No firmware code in this repo.
- Architecture doc (diagram + the "why these choices" story).
- README finale: demo GIF (replay at speed is ideal footage), 5-minute quickstart,
  positioning statement, future-work section (incl. closed-loop auto-watering as the
  natural "write path" extension — explicitly out of scope here).

**Done when:** the README passes the "stranger clones it and has a live dashboard in
5 minutes" test; hardware doc is sufficient to point the real pod at the stack.

### M7 — Analytics layer

The headline feature: tells you when to water, not that you should have.

Scope:
- Per-planter depletion model (rolling fit over recent depletion segments, refill
  events excluded) → forecast refill-by date; start simple, structure for upgrades.
- Battery-life forecast from voltage decay (same mechanism).
- Forecasts written to a table; Grafana "attention needed" panel: days-until-empty
  and days-until-charge per device.
- Alerting on forecast horizon (< N days) rather than raw thresholds — via Grafana
  alerting or a small notifier (ntfy fits the existing pod setup).
- Tests: forecast correctness on synthetic streams with known depletion rates.

**Done when:** the dashboard shows a credible days-until-empty per planter that a
test verifies against the simulator's configured depletion rate; an alert fires when
the horizon crosses the threshold.

---

## Out of scope (deliberately)

- Actuation / auto-watering control loop (future project: the "write path").
- Weather or plant-species integrations, mobile apps.
- Firmware development (lives in the private planter repo).
- Anomaly detection (leak / wick-failure / sensor-fault baselines) — natural M8 or
  future-work if appetite remains after M7.

## Suggested order

M0 → M1 → M2 → M3 → M4 → M5 → M6 → M7. M0–M2 is a working invisible pipeline,
M3–M4 makes it visible, M5–M6 makes it credible, M7 makes it interesting.
