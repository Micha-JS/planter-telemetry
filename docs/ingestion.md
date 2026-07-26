# Ingestion service

The M2 centerpiece: an async MQTT consumer
([`planter_telemetry.ingestion`](../src/planter_telemetry/ingestion/)) that turns
the broker's at-least-once message stream into validated, idempotent rows in
TimescaleDB, with a dead-letter table for everything that fails validation.

## Layering

Mirrors the simulator's pure/impure split — the interesting logic is testable
without a broker or a database:

```
aiomqtt (edge) ─► core.classify (pure) ─► db.Writer (edge)
```

- `core.classify(topic, payload)` decides, with no I/O: valid reading, or dead
  letter with a human-readable reason. Order: topic shape → schema validation
  (via the shared [`TelemetryV1`](../src/planter_telemetry/contract.py) model) →
  topic/payload `device_id` cross-check.
- `db.Writer` owns the three statements (reading insert, device-registry
  upsert, dead-letter insert); `service.run()` wires the edges together and
  owns reconnection and shutdown.

## Delivery semantics: effectively-once, honestly

Three pieces combine, each doing one job:

1. **QoS 1** — the broker redelivers until the consumer acknowledges:
   at-least-once from broker to consumer, never silent loss in transit.
2. **Persistent session** (`clean_session=False` + the stable client id
   `planter-ingestion`, MQTT 3.1.1) — while the ingestion service is down or
   reconnecting, the broker queues matching QoS 1 messages for its session
   instead of dropping them.
3. **Idempotent writes** — `INSERT … ON CONFLICT (device_id, measured_at)
   DO NOTHING`. A reading's natural key makes every redelivery, replay, or
   duplicate a no-op. This is *why* at-least-once is the right delivery choice:
   the database, not the broker, is where exactly-once semantics are
   manufactured.

The result is effectively-once *into the database* for any message the service
processes. Two boundaries are deliberate and worth stating plainly:

- **The ack window.** paho (and therefore aiomqtt) sends the QoS 1 PUBACK when
  the message is received, before it is processed — there is no manual-ack API.
  A crash between ack and database commit can lose the in-flight message(s).
  Mitigations, in order of the failure they cover: on SIGTERM the service
  finishes the in-flight message *and* writes out everything already buffered
  (acked) in the client's incoming queue before exiting; on database loss that
  same buffered backlog is salvaged into memory, held across the reconnect,
  and replayed first; if stop arrives while the database is still down, a
  final bounded flush attempt runs at exit and any loss is logged loudly
  (`dropping N acked message(s)`), never silently. The residual window — a
  hard kill (SIGKILL) between ack and commit — is real but only ever costs
  messages already acked and not yet committed. We do not claim exactly-once
  end-to-end.
- **Broker restarts.** Mosquitto runs with `persistence false`, so the
  persistent session (and anything queued in it) survives ingestion restarts
  but not broker restarts. Consistent with the architecture's stance: the
  broker is a transient transport; durable state lives in the database.

## Dead letters, not crashes

Anything that fails classification — unparseable JSON, schema violations,
naive timestamps, out-of-range values, a topic/payload `device_id` mismatch —
becomes a row in `dead_letter` carrying the raw payload bytes (`bytea`:
truncation can split UTF-8), the topic, a greppable reason, and `received_at`.
The service never crashes on message content and never silently drops input.
M4 puts a panel on this table.

## Write strategy

Since M3, every valid reading also upserts the device registry
(`devices.first_seen`/`last_seen`, `LEAST`/`GREATEST` so ordering never
matters) before the reading insert — deliberately also on redeliveries, so
`last_seen` advances even when the reading itself deduplicates. Rationale in
[docs/storage.md](storage.md).

Row-at-a-time, on purpose. The default simulator produces ~2–4 messages/second
(4 devices, 300 virtual-second cadence at 180× acceleration); a single INSERT
per message is nowhere near any limit at this scale, and it keeps dedupe
counting (`rowcount` after `ON CONFLICT DO NOTHING`) and shutdown draining
trivial. When M5's replay-at-100× needs more throughput, the upgrade path is
micro-batching in `db.Writer` (flush every N messages / T ms with
`executemany`) behind the same interface — correctness is unaffected either
way because idempotency does not depend on batching.

## Failure behavior

- **Broker or DB loss:** one reconnect path — both connections are torn down
  and rebuilt with exponential backoff (1 s doubling to 30 s). No half-alive
  states; compose healthchecks only gate the first start.
- **Shutdown (SIGTERM/SIGINT):** stop consuming, finish the in-flight message,
  close both connections, log a final `ingested/deduplicated/dead_lettered`
  summary.
- **Observability:** a stats line with those three counters every 30 s
  (`INGEST_STATS_INTERVAL_SECONDS`); every dead letter logs a warning with its
  reason. JSON logging arrives with the ops polish in M5.

## Proof

The claims above are CI-verified against a real broker and a real TimescaleDB
(testcontainers, `make integration`):

- replaying an identical seeded stream twice leaves row count *and* content
  unchanged;
- injected duplicates produce exactly one row per `(device_id, measured_at)`
  and the dedupe counter matches;
- malformed messages land in `dead_letter` with meaningful reasons while valid
  messages around them ingest and the service keeps running;
- out-of-order arrivals ingest fine — the write path assumes nothing about
  ordering;
- stopping mid-burst loses nothing: a broker-side backlog delivered (and
  acked) faster than it is written still lands completely, because shutdown
  drains the client's buffered queue;
- a database outage mid-stream loses nothing: acked messages are salvaged
  into memory, replayed after the reconnect, and every unique reading ends up
  in the table.

`make ingest-smoke` additionally proves the composed artifact: simulator →
broker → ingestion → rows in the database, ingestion container still healthy.
