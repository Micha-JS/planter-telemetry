"""Integration: the ingestion service against a real broker and TimescaleDB.

The service runs in-process as an asyncio task so tests can inject counters,
stop it gracefully, and assert it survives garbage. Each test primes the
broker-side persistent session (service client id + clean_session=False)
before publishing, so QoS 1 messages queue on the broker even if the service
has not finished connecting — no publish-vs-subscribe race, and exactly the
delivery semantics documented in docs/ingestion.md.
"""

import asyncio
import itertools
import time
from collections.abc import AsyncIterator, Callable, Iterable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import LiteralString

import aiomqtt
import psycopg
import pytest

from planter_telemetry.contract import TELEMETRY_TOPIC_FILTER, TelemetryV1, telemetry_topic
from planter_telemetry.ingestion.config import IngestSettings
from planter_telemetry.ingestion.db import Writer
from planter_telemetry.ingestion.ops import HealthState
from planter_telemetry.ingestion.service import Counters, run
from planter_telemetry.simulator.config import SimulatorSettings
from planter_telemetry.simulator.stream import Emission, build_streams
from planter_telemetry.simulator.wire import CorruptionMode, corrupt, encode

pytestmark = pytest.mark.integration

START = datetime(2026, 1, 1, tzinfo=UTC)
WAIT_SECONDS = 60.0  # CI docker is slower than local; polls exit early anyway

Snapshot = list[tuple[str, datetime, float, float, int]]


def _sim_settings(duplicate_rate: float = 0.0) -> SimulatorSettings:
    """Seeded fleet with all imperfections off unless a test opts in."""
    return SimulatorSettings(
        device_count=3,
        seed=42,
        duplicate_rate=duplicate_rate,
        out_of_order_rate=0.0,
        malformed_rate=0.0,
        missed_checkin_rate=0.0,
    )


def _emissions(settings: SimulatorSettings, count: int) -> list[Emission]:
    return list(itertools.islice(build_streams(settings, START), count))


def _unique_keys(emissions: Iterable[Emission]) -> set[tuple[str, datetime]]:
    return {(e.reading.device_id, e.reading.measured_at) for e in emissions if e.corruption is None}


def _wire(emissions: Iterable[Emission]) -> list[tuple[str, bytes]]:
    messages = []
    for emission in emissions:
        payload = encode(emission.reading)
        if emission.corruption is not None:
            payload = corrupt(payload, emission.corruption)
        messages.append((telemetry_topic(emission.reading.device_id), payload))
    return messages


def _reading(device_id: str, minute: int) -> TelemetryV1:
    return TelemetryV1(
        device_id=device_id,
        measured_at=START + timedelta(minutes=minute),
        water_level=50.0,
        battery_voltage=3.7,
    )


async def _prime_session(host: str, port: int, client_id: str) -> None:
    """Create the broker-side persistent session before the service starts."""
    async with aiomqtt.Client(host, port, identifier=client_id, clean_session=False) as client:
        await client.subscribe(TELEMETRY_TOPIC_FILTER, qos=1)


async def _publish(host: str, port: int, messages: Iterable[tuple[str, bytes]]) -> None:
    async with aiomqtt.Client(host, port) as client:
        for topic, payload in messages:
            # qos=1 publish awaits the PUBACK, so returning means the broker
            # has everything — a solid ordering point before polling the DB.
            await client.publish(topic, payload, qos=1)


@asynccontextmanager
async def _running_service(
    host: str,
    port: int,
    dsn: str,
    client_id: str,
    reconnect_initial_seconds: float = 1.0,
    health: HealthState | None = None,
) -> AsyncIterator[tuple[Counters, asyncio.Task[None]]]:
    settings = IngestSettings(
        mqtt_host=host,
        mqtt_port=port,
        client_id=client_id,
        db_dsn=dsn,
        reconnect_initial_seconds=reconnect_initial_seconds,
        ops_port=0,  # ephemeral: parallel tests must not fight over 8080
    )
    counters = Counters()
    stop = asyncio.Event()
    task = asyncio.create_task(run(settings, counters=counters, stop=stop, health=health))
    try:
        yield counters, task
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=15)


async def _scalar(dsn: str, query: LiteralString) -> int:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cursor = await conn.execute(query)
        row = await cursor.fetchone()
        assert row is not None
        return int(row[0])


async def _wait_until(predicate: Callable[[], bool], description: str) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    while not predicate():
        if time.monotonic() > deadline:
            pytest.fail(f"timed out waiting for {description}")
        await asyncio.sleep(0.2)


async def _wait_for_scalar(dsn: str, query: LiteralString, expected: int) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    last = -1
    while time.monotonic() < deadline:
        last = await _scalar(dsn, query)
        if last == expected:
            return
        await asyncio.sleep(0.2)
    pytest.fail(f"timed out: {query} stuck at {last}, expected {expected}")


async def _snapshot(dsn: str) -> Snapshot:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cursor = await conn.execute(
            "SELECT device_id, measured_at, water_level, battery_voltage, schema_version"
            " FROM telemetry ORDER BY device_id, measured_at"
        )
        return [(r[0], r[1], r[2], r[3], r[4]) for r in await cursor.fetchall()]


async def test_replay_twice_changes_nothing(mqtt_endpoint: tuple[str, int], clean_db: str) -> None:
    host, port = mqtt_endpoint
    emissions = _emissions(_sim_settings(), 60)
    expected = len(_unique_keys(emissions))
    assert expected == 60  # clean stream: every emission is a distinct reading

    await _prime_session(host, port, "it-replay")
    async with _running_service(host, port, clean_db, "it-replay") as (counters, _task):
        await _publish(host, port, _wire(emissions))
        await _wait_for_scalar(clean_db, "SELECT count(*) FROM telemetry", expected)
        first = await _snapshot(clean_db)

        await _publish(host, port, _wire(emissions))  # the identical stream again
        await _wait_until(
            lambda: counters.deduplicated >= expected, "second pass fully deduplicated"
        )
        second = await _snapshot(clean_db)

    assert len(first) == expected
    assert second == first  # row count *and* content unchanged
    assert counters.ingested == expected
    assert counters.deduplicated == expected
    assert counters.dead_lettered == 0
    # The idempotency invariant covers pipeline state (telemetry, devices,
    # rollups) — not the arrival log. Like dead_letter, ingest_events records
    # what actually came over the wire: the replayed pass genuinely was
    # deduplicated, one event per message.
    assert (
        await _scalar(clean_db, "SELECT count(*) FROM ingest_events WHERE event = 'deduplicated'")
        == expected
    )


async def test_duplicates_within_stream_are_deduplicated(
    mqtt_endpoint: tuple[str, int], clean_db: str
) -> None:
    host, port = mqtt_endpoint
    emissions = _emissions(_sim_settings(duplicate_rate=0.3), 80)
    unique = len(_unique_keys(emissions))
    duplicates = len(emissions) - unique
    assert duplicates > 0  # the seeded stream really contains duplicates

    await _prime_session(host, port, "it-dupes")
    async with _running_service(host, port, clean_db, "it-dupes") as (counters, _task):
        await _publish(host, port, _wire(emissions))
        await _wait_for_scalar(clean_db, "SELECT count(*) FROM telemetry", unique)
        await _wait_until(
            lambda: counters.ingested + counters.deduplicated == len(emissions),
            "every emission accounted for",
        )

    assert counters.ingested == unique
    assert counters.deduplicated == duplicates
    assert counters.dead_lettered == 0

    # Every dedupe left a queryable trace identifying the duplicated reading.
    async with await psycopg.AsyncConnection.connect(clean_db) as conn:
        cursor = await conn.execute(
            "SELECT device_id, measured_at FROM ingest_events"
            " WHERE event = 'deduplicated' ORDER BY device_id, measured_at"
        )
        events = [(str(r[0]), r[1]) for r in await cursor.fetchall()]
    expected_events = sorted(
        (e.reading.device_id, e.reading.measured_at) for e in emissions if e.kind == "duplicate"
    )
    assert events == expected_events


async def test_malformed_messages_dead_letter_without_crashing(
    mqtt_endpoint: tuple[str, int], clean_db: str
) -> None:
    host, port = mqtt_endpoint
    valid_before = [
        (telemetry_topic("planter-00"), encode(_reading("planter-00", i))) for i in range(5)
    ]
    valid_after = [
        (telemetry_topic("planter-01"), encode(_reading("planter-01", i))) for i in range(5)
    ]
    sacrificial = encode(_reading("planter-00", 30))
    garbage = [
        (telemetry_topic("planter-00"), corrupt(sacrificial, CorruptionMode.TRUNCATED)),
        (telemetry_topic("planter-00"), corrupt(sacrificial, CorruptionMode.WRONG_TYPE)),
        (telemetry_topic("planter-00"), corrupt(sacrificial, CorruptionMode.MISSING_FIELD)),
        # Valid payload, wrong topic: the cross-check must catch it.
        (telemetry_topic("planter-01"), encode(_reading("planter-00", 31))),
    ]

    await _prime_session(host, port, "it-malformed")
    async with _running_service(host, port, clean_db, "it-malformed") as (counters, task):
        await _publish(host, port, [*valid_before, *garbage, *valid_after])
        await _wait_for_scalar(clean_db, "SELECT count(*) FROM dead_letter", len(garbage))
        # Valid messages on both sides of the garbage all made it.
        await _wait_for_scalar(clean_db, "SELECT count(*) FROM telemetry", 10)
        assert not task.done()  # the service survived

        async with await psycopg.AsyncConnection.connect(clean_db) as conn:
            cursor = await conn.execute("SELECT reason FROM dead_letter ORDER BY id")
            reasons = [str(row[0]) for row in await cursor.fetchall()]

    assert counters.dead_lettered == len(garbage)
    assert "invalid json" in reasons[0].lower()  # TRUNCATED
    assert "water_level" in reasons[1]  # WRONG_TYPE
    assert "device_id" in reasons[2]  # MISSING_FIELD
    assert "device_id mismatch" in reasons[3]
    assert "planter-00" in reasons[3] and "planter-01" in reasons[3]


async def test_shutdown_drains_acked_backlog(mqtt_endpoint: tuple[str, int], clean_db: str) -> None:
    """Stopping mid-burst loses nothing.

    All 60 messages are queued broker-side before the service starts, so
    connecting delivers them in one burst — paho acks the burst into
    aiomqtt's incoming queue far faster than row-at-a-time writes drain it.
    Stopping right after the first row forces the shutdown drain to write
    the acked backlog; whatever the broker had not yet delivered stays in
    the persistent session and arrives after the restart. Without the
    drain, the acked-but-unprocessed messages are gone forever and the
    final count sticks below 60.
    """
    host, port = mqtt_endpoint
    emissions = _emissions(_sim_settings(), 60)

    await _prime_session(host, port, "it-drain")
    await _publish(host, port, _wire(emissions))

    async with _running_service(host, port, clean_db, "it-drain") as (counters, _task):
        await _wait_until(lambda: counters.ingested >= 1, "first row written")
    # The context manager has set stop and awaited the task: the service is
    # fully down, and everything it acked must already be in the database.

    async with _running_service(host, port, clean_db, "it-drain") as (_counters, _task):
        await _wait_for_scalar(clean_db, "SELECT count(*) FROM telemetry", 60)

    assert await _scalar(clean_db, "SELECT count(*) FROM dead_letter") == 0


async def test_db_outage_loses_nothing(
    mqtt_endpoint: tuple[str, int], clean_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Acked messages survive a database outage via salvage + replay.

    A window of injected OperationalErrors spans several reconnect cycles;
    each failure happens with acked messages sitting in aiomqtt's queue,
    which the service must salvage into memory and replay after the
    reconnect. Every unique reading must land despite the outage.
    """
    host, port = mqtt_endpoint
    emissions = _emissions(_sim_settings(), 40)

    real_insert = Writer.insert_reading
    calls = itertools.count()
    fail_start, fail_stop = 10, 13  # three failures -> at least three reconnects

    async def flaky_insert(self: Writer, reading: TelemetryV1) -> bool:
        if fail_start <= next(calls) < fail_stop:
            raise psycopg.OperationalError("injected outage")
        return await real_insert(self, reading)

    monkeypatch.setattr(Writer, "insert_reading", flaky_insert)

    await _prime_session(host, port, "it-outage")
    async with _running_service(
        host, port, clean_db, "it-outage", reconnect_initial_seconds=0.1
    ) as (counters, task):
        await _publish(host, port, _wire(emissions))
        await _wait_for_scalar(clean_db, "SELECT count(*) FROM telemetry", 40)
        assert not task.done()  # survived the outage

    assert counters.ingested == 40
    assert counters.dead_lettered == 0


async def test_out_of_order_arrival_ingests_fine(
    mqtt_endpoint: tuple[str, int], clean_db: str
) -> None:
    host, port = mqtt_endpoint
    newer = _reading("planter-00", 10)
    older = _reading("planter-00", 0)
    messages = [
        (telemetry_topic(r.device_id), encode(r))
        for r in (newer, older)  # newest first: no ordering assumption in the write path
    ]

    await _prime_session(host, port, "it-ooo")
    async with _running_service(host, port, clean_db, "it-ooo") as (counters, _task):
        await _publish(host, port, messages)
        await _wait_for_scalar(clean_db, "SELECT count(*) FROM telemetry", 2)

    assert counters.ingested == 2
    assert counters.deduplicated == 0
    assert counters.dead_lettered == 0
    # The in-memory arrival observation behind /metrics: the older reading
    # arrived after a newer one had been seen for the same device.
    assert counters.out_of_order == 1
