# Hardware bridge: connecting the real ESP32 pod

This document is the contract the firmware must meet to feed this stack. It
is written for future-me, six months from now, holding a flashed sensor pod
and a fresh clone of this repo. Firmware code lives in the separate private
planter repo and never lands here; everything only that repo can answer is
marked `TODO(firmware)` — fill those in against the real firmware, don't
guess. The [TODO ledger](#todo-ledger) at the bottom collects them all.

Nothing in this document changes the default demo path. Hardware mode is an
opt-in compose override; `docker compose up -d --build` behaves exactly as
the [README quickstart](../README.md#quickstart) describes whether or not you
ever read this page.

## Two modes, one pipeline

| | Demo mode (default) | Hardware mode (opt-in) |
|---|---|---|
| Start | `docker compose up -d --build` | `make hardware-up` |
| Broker listener | 1883, anonymous | 1883 unchanged **plus** 1884 on the LAN |
| Who connects | simulator, ingestion, CLI, healthcheck | same, plus real pods on 1884 |
| Auth | none (1883 is bound to host loopback — unreachable from the LAN) | password file + ACL on 1884 |

`make hardware-up` is shorthand for:

```bash
docker compose -f docker-compose.yml -f docker-compose.hardware.yml up -d --build
```

The override ([docker-compose.hardware.yml](../docker-compose.hardware.yml))
swaps in [mosquitto.hardware.conf](../mosquitto/mosquitto.hardware.conf),
which keeps the internal listener exactly as demo mode has it and adds an
authenticated LAN listener on 1884. Nothing downstream — ingestion, database,
dashboard — knows or cares which listener a message arrived on; a real pod
and the simulator feed one identical pipeline.

The simulator keeps running in hardware mode, on purpose: during firmware
bring-up the dashboard is visibly alive before the first real reading lands.
For a real-only stream:

```bash
docker compose -f docker-compose.yml -f docker-compose.hardware.yml stop simulator
```

## Identity and topics

- **Publish topic:** `planter/v1/{device_id}/telemetry`, QoS 1, **no retained
  messages**.
- **`device_id`:** must match `^[a-z0-9][a-z0-9_-]{0,31}$` — lowercase
  alphanumerics plus `-`/`_`, starting alphanumeric, max 32 chars. The
  pattern excludes MQTT topic metacharacters, so a valid id is always
  topic-safe, and it is also a valid Mosquitto username. A lowercased MAC
  suffix (e.g. `planter-a4cf12`) is a fine choice.
- **MQTT username == `device_id`.** The broker ACL is one line —
  `pattern write planter/v1/%u/telemetry`
  ([mosquitto/auth/acl.conf](../mosquitto/auth/acl.conf)) — so each device
  may publish exactly its own telemetry topic and nothing else, and devices
  have no read access at all.
- **ACL denials are silent.** MQTT 3.1.1 has no "publish denied" error: the
  broker accepts the packet, acknowledges it (QoS 1), and drops it. If the
  username and the topic's `device_id` segment disagree, the message
  vanishes without any error on the device. Watch the wire (below) to catch
  this.
- The `device_id` **inside the payload** must equal the topic segment.
  Ingestion cross-checks and dead-letters mismatches with reason
  `device_id mismatch: topic='…' payload='…'`.
- Firmware MQTT client id: `TODO(firmware)` (any stable, unique string is
  fine; the pipeline never sees it — only the broker does).

## Payload contract (v1)

The normative reference is [message-contract.md](message-contract.md) and its
canonical implementation
[`planter_telemetry.contract`](../src/planter_telemetry/contract.py). The
short version, restated here because this is the page open during bring-up:

One JSON object (UTF-8) per message:

```json
{
  "schema_version": 1,
  "device_id": "planter-00",
  "measured_at": "2026-07-25T12:34:56.789012Z",
  "water_level": 73.4,
  "battery_voltage": 3.91
}
```

Field mapping — the firmware-side column is deliberately unfilled; complete
it from the real firmware during review:

| Contract field | Type / constraint | Unit | Firmware source |
|---|---|---|---|
| `schema_version` | number, literally `1` | — | `TODO(firmware)` (hardcode `1`) |
| `device_id` | string, `^[a-z0-9][a-z0-9_-]{0,31}$` | — | `TODO(firmware)` (derived from MAC? compiled in?) |
| `measured_at` | string, RFC 3339 **with UTC offset**; naive timestamps are rejected | UTC on validation | `TODO(firmware)` (clock source — see below) |
| `water_level` | number, `0 ≤ x ≤ 100` | % of tank capacity | `TODO(firmware)` (which sensor/ADC, and the counts→percent calibration) |
| `battery_voltage` | number, `2.5 ≤ x ≤ 4.4` | volts, single-cell LiPo | `TODO(firmware)` (ADC pin + voltage-divider math) |

Extra keys are ignored by v1 consumers (additive versioning policy), so the
firmware may send more fields than this — never fewer, never retyped.

> **`measured_at` is half the idempotency key.** Rows are deduplicated on
> `(device_id, measured_at)`, so the pod needs real UTC time at measurement,
> not "seconds since boot". How the firmware gets it — NTP on wake, a backup
> RTC, something else — is `TODO(firmware)`. If two wakes ever report the
> same `measured_at`, the second reading is silently treated as a duplicate
> of the first; a clock that only advances between wakes is the requirement.

## Broker connection

- **Host:** the LAN address of the machine running the stack — find it with
  `ipconfig getifaddr en0` (macOS) or `ip -4 addr` (Linux). Port **1884**.
  (1883 stays loopback-bound; a pod on the LAN cannot reach it.)
- **Credentials:** username = `device_id`, password = whatever you set when
  creating the device user (next section).
- **Firmware config location for host/port/credentials:** `TODO(firmware)`.
- **Transport honesty:** 1884 is username/password over plain TCP — no TLS
  in this milestone. Credentials travel readable on the LAN, so hardware
  mode assumes a trusted home network. TLS on 1884 is the obvious future
  hardening step.

### Creating a device user

```bash
make hardware-passwd DEVICE=planter-a4cf12
```

This validates the id against the `device_id` pattern, runs
`mosquitto_passwd` inside the `eclipse-mosquitto:2` image against the
mounted `mosquitto/auth/` directory (so file ownership and permissions come
out broker-readable on every platform), prompts for a password, and — if the
stack is running — SIGHUPs the broker so the new credentials take effect
immediately. SIGHUP re-reads `password_file` and `acl_file`; it does not
disconnect already-connected clients.

`mosquitto/auth/passwd` is gitignored and must never be committed. The ACL
file next to it is committed — it contains a pattern, not secrets.

### Deep sleep and QoS

The pod's publish loop should be: wake → measure → connect (**clean
session**) → publish QoS 1 → wait for PUBACK → disconnect → sleep.

- **Use a clean session on every wake.** The pod subscribes to nothing, so a
  broker-side persistent session would hold nothing for it; don't build
  firmware assumptions on session state surviving sleep. (The *consumer*
  persistent session described in [ingestion.md](ingestion.md) belongs to
  the ingestion service and is unrelated to devices.)
- **When in doubt, resend.** If the PUBACK is lost and the firmware
  republishes the same reading on the next wake, the pipeline's idempotent
  insert absorbs it as a duplicate — resending is always safe, dropping data
  is not. Err on the side of resending.
- Keepalive, reconnect/backoff, and connect-timeout behavior of the firmware
  MQTT client: `TODO(firmware)`.
- Publish cadence (deep-sleep interval): `TODO(firmware)` — this number
  matters for the dashboard's last-seen thresholds (see checklist step 5).

## First successful reading — checklist

Run these in order; each step isolates one layer.

1. **Start hardware mode and watch the wire:**

   ```bash
   make hardware-up
   docker compose exec mosquitto mosquitto_sub -t 'planter/v1/+/telemetry' -v
   ```

   Simulator traffic scrolls by immediately (that's the pipeline proving
   itself); the pod's `device_id` appearing in that stream is the first
   success signal.

2. **Nothing appears when the pod publishes?** Two different failures look
   identical from the couch:
   - *Connection refused (`not authorised`)* — the device user doesn't exist
     in the passwd file, the password is wrong, or the broker was never
     SIGHUPed after `make hardware-passwd`. The firmware's MQTT client sees
     a CONNACK refusal: code 5 on MQTT 3.1.1, code 135 on MQTT 5.
   - *Connected but silently dropped* — the ACL. The topic being published
     doesn't match `planter/v1/<username>/telemetry` (typo'd topic, or
     username ≠ `device_id`). The broker acknowledges and discards; only the
     absence on `mosquitto_sub` tells you.

3. **On the wire but no database row?** The payload failed validation — it
   dead-lettered instead of crashing anything. Read the reason:

   ```bash
   docker compose exec timescaledb psql -U planter -d planter -c \
     "SELECT received_at, topic, reason, left(encode(payload, 'escape'), 80) AS payload_prefix
      FROM dead_letter ORDER BY received_at DESC LIMIT 10"
   ```

   The `reason` tells you which firmware bug to fix — these are the real
   strings, straight from the pipeline:

   | Reason | What the firmware did |
   |---|---|
   | `invalid payload: payload: Invalid JSON: EOF while parsing a string at line 1 column 25` | Truncated publish — buffer too small, or the payload length was miscounted |
   | `invalid payload: measured_at: Input should have timezone info` | Naive timestamp; append `Z` (or a real offset) |
   | `invalid payload: water_level: Input should be less than or equal to 100` | Sent raw sensor counts instead of percent |
   | `device_id mismatch: topic='planter-01' payload='planter-00'` | Topic segment and payload id disagree |
   | `topic does not match planter/v1/{device_id}/telemetry` | Wrong topic shape entirely |

   Up to three validation errors are listed per message, `;`-separated.

4. **Confirm the row:**

   ```bash
   docker compose exec timescaledb psql -U planter -d planter -c \
     "SELECT * FROM telemetry WHERE device_id = 'planter-a4cf12' ORDER BY measured_at DESC LIMIT 5"
   ```

5. **Dashboard:** open <http://localhost:3000> — the pod appears in the
   fleet panels once its first row lands. **Caveat before trusting the
   last-seen tile:** "Time since last check-in" measures each device against
   the fleet-wide `max(measured_at)` ("virtual now" — see
   [dashboard-queries.md](dashboard-queries.md)), with thresholds tuned to
   the simulator's 300 s cadence (green < 600 s, orange from 600 s, red from
   1800 s). While the simulator runs, its clock dominates the fleet max, and
   a real pod on a slower deep-sleep interval sits orange/red **while
   perfectly healthy**. That's expected until the thresholds are retuned to
   the real cadence (`TODO(firmware)`), or the simulator is stopped.

## The pipeline is the safety net

During firmware bring-up, this stack is the debugger, not the thing to
protect from the firmware:

- A nonconforming payload becomes a `dead_letter` row carrying the raw bytes
  and a diagnostic reason. Nothing crashes; nothing is silently dropped.
  The [meta-observability panels](dashboard-queries.md#pipeline-meta-observability-row)
  make dead-letter rate visible on the dashboard itself.
- Duplicates — QoS 1 redelivery, resend-after-lost-PUBACK, an over-eager
  retry loop — are absorbed by
  `INSERT … ON CONFLICT (device_id, measured_at) DO NOTHING` and counted,
  not duplicated.
- Out-of-order arrival is fine; rows are keyed by `measured_at`, not arrival
  time.

So: point the pod at the stack early, publish aggressively, and read the
dead-letter table as the firmware's error log.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Connection Refused: not authorised` | No such user / wrong password / broker not reloaded | `make hardware-passwd DEVICE=…`, which SIGHUPs a running broker for you |
| Publish "succeeds" but nothing on `mosquitto_sub` | ACL silent drop: topic ≠ `planter/v1/<username>/telemetry` | Make username == `device_id` == topic segment |
| On the wire, but no row **and** no dead letter | Duplicate key — a row with that `(device_id, measured_at)` already exists | Working as designed; check the firmware clock actually advances between wakes |
| Row exists but panels look empty | Dashboard time range vs `measured_at` | Widen the Grafana time range; check the pod's clock isn't in the past/future |
| `mosquitto/auth/passwd` is a *directory* | A hand-rolled single-file bind mount ran before the file existed — docker created a directory | Delete it; always create users via `make hardware-passwd` |
| Broker exits immediately at startup | Missing passwd file, or an edited config placed auth options before `per_listener_settings` | `make hardware-up` preflights the passwd file; keep `per_listener_settings true` the first directive |

## TODO ledger

Everything above that only the firmware repo can answer, in one list for the
review pass against the real firmware:

- [ ] MQTT client id scheme
- [ ] `device_id` provisioning (MAC-derived? compiled in?)
- [ ] `measured_at` clock source (NTP on wake? RTC?) and its guarantee that
      time advances between wakes
- [ ] `water_level` sensor + counts→percent calibration
- [ ] `battery_voltage` ADC pin + divider math
- [ ] Where broker host/port/credentials live in firmware config
- [ ] MQTT client keepalive / reconnect / connect-timeout behavior
- [ ] Publish cadence (deep-sleep interval) → retune the last-seen
      thresholds in the dashboard to match
