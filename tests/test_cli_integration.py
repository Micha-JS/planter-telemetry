"""Integration: the capture/replay CLI against a real broker and database.

The M5 done-criterion, proven end-to-end through the CLI: a captured window
replayed through MQTT produces exactly the state a direct ingest of the
same stream produces, and replaying the same file again changes nothing.
Replay never touches the database — everything flows through the same
ingestion path as live traffic.
"""

import asyncio
import io
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path

import aiomqtt
import psycopg
import pytest

from planter_telemetry.cli.capture import capture
from planter_telemetry.cli.records import decode_record, encode_record
from planter_telemetry.cli.replay import replay
from planter_telemetry.cli.sample import (
    SAMPLE_DEAD_LETTERS,
    SAMPLE_UNIQUE_VALID_ROWS,
    sample_records,
    sample_settings,
)
from planter_telemetry.contract import TELEMETRY_TOPIC_FILTER, telemetry_topic
from planter_telemetry.ingestion.config import IngestSettings
from planter_telemetry.ingestion.service import Counters, run
from planter_telemetry.simulator.stream import build_streams
from planter_telemetry.simulator.wire import encode

pytestmark = pytest.mark.integration

START = datetime(2026, 1, 1, tzinfo=UTC)
WAIT_SECONDS = 60.0

Snapshot = list[tuple[str, datetime, float, float, int]]


async def _wait_until(predicate: Callable[[], bool], description: str) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    while not predicate():
        if time.monotonic() > deadline:
            pytest.fail(f"timed out waiting for {description}")
        await asyncio.sleep(0.2)


async def _prime_session(host: str, port: int, client_id: str) -> None:
    """Create the broker-side persistent session before the service starts."""
    async with aiomqtt.Client(host, port, identifier=client_id, clean_session=False) as client:
        await client.subscribe(TELEMETRY_TOPIC_FILTER, qos=1)


@asynccontextmanager
async def _running_service(
    host: str, port: int, dsn: str, client_id: str
) -> AsyncIterator[Counters]:
    settings = IngestSettings(
        mqtt_host=host, mqtt_port=port, client_id=client_id, db_dsn=dsn, ops_port=0
    )
    counters = Counters()
    stop = asyncio.Event()
    task = asyncio.create_task(run(settings, counters=counters, stop=stop))
    try:
        yield counters
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=15)


async def _snapshot(dsn: str) -> Snapshot:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cursor = await conn.execute(
            "SELECT device_id, measured_at, water_level, battery_voltage, schema_version"
            " FROM telemetry ORDER BY device_id, measured_at"
        )
        return [(r[0], r[1], r[2], r[3], r[4]) for r in await cursor.fetchall()]


async def _truncate(dsn: str) -> None:
    # Mirrors the clean_db fixture: full per-phase isolation within one test.
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        await conn.execute(
            "TRUNCATE telemetry, dead_letter, devices, ingest_events RESTART IDENTITY"
        )
        await conn.execute("TRUNCATE telemetry_hourly")
        await conn.execute("TRUNCATE telemetry_daily")
        await conn.commit()


async def test_capture_round_trips_wire_bytes(mqtt_endpoint: tuple[str, int]) -> None:
    """Everything the broker delivers — valid JSON, corrupted fragments,
    non-UTF-8 garbage — lands in the JSONL byte-identical and in order."""
    host, port = mqtt_endpoint
    probe_topic = telemetry_topic("probe")
    reading = next(iter(build_streams(sample_settings(), START))).reading
    batch: list[tuple[str, bytes]] = [
        (telemetry_topic(reading.device_id), encode(reading)),
        (telemetry_topic("planter-00"), encode(reading)[:20]),  # truncated JSON
        (telemetry_topic("planter-01"), b"\xff\xfe raw bytes"),  # not UTF-8
        (telemetry_topic("planter-02"), b""),
    ]

    out = io.StringIO()
    task = asyncio.create_task(
        capture(
            host=host,
            port=port,
            topic=TELEMETRY_TOPIC_FILTER,
            out=out,
            count=None,
            duration_seconds=WAIT_SECONDS,
        )
    )
    try:
        async with aiomqtt.Client(host, port) as publisher:
            # No readiness signal from a plain subscribe: probe until the
            # capture demonstrably receives, then send the real batch.
            deadline = time.monotonic() + WAIT_SECONDS
            while not out.getvalue():
                if time.monotonic() > deadline:
                    pytest.fail("capture never received the probe")
                await publisher.publish(probe_topic, b"probe", qos=1)
                await asyncio.sleep(0.2)
            for topic, payload in batch:
                await publisher.publish(topic, payload, qos=1)
        await _wait_until(
            lambda: (
                sum(1 for line in out.getvalue().splitlines() if f'"{probe_topic}"' not in line)
                >= len(batch)
            ),
            "the batch to be captured",
        )
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    records = [decode_record(line) for line in out.getvalue().splitlines()]
    delivered = [(r.topic, r.payload) for r in records if r.topic != probe_topic]
    assert delivered == batch  # byte-identical, order preserved
    assert all(r.captured_at.tzinfo is not None for r in records)


async def test_replay_matches_direct_ingest_and_is_idempotent(
    mqtt_endpoint: tuple[str, int], clean_db: str, tmp_path: Path
) -> None:
    host, port = mqtt_endpoint
    # The exact stream shipped as samples/telemetry-window.jsonl — this test
    # proves the committed demo artifact end-to-end, pinned counts included.
    records = list(sample_records())
    capture_file = tmp_path / "window.jsonl"
    capture_file.write_text("".join(encode_record(r) + "\n" for r in records), encoding="utf-8")
    total = len(records)

    await _prime_session(host, port, "it-cli-replay")
    async with _running_service(host, port, clean_db, "it-cli-replay") as counters:
        # First replay into an empty database.
        with capture_file.open(encoding="utf-8") as source:
            published = await replay(host=host, port=port, source=source, speed=None)
        assert published == total
        await _wait_until(
            lambda: counters.ingested + counters.deduplicated + counters.dead_lettered >= total,
            "first replay fully processed",
        )
        replayed = await _snapshot(clean_db)
        assert len(replayed) == SAMPLE_UNIQUE_VALID_ROWS
        assert counters.ingested == SAMPLE_UNIQUE_VALID_ROWS
        assert counters.dead_lettered == SAMPLE_DEAD_LETTERS  # dead-letter path exercised

        # Second replay of the SAME file: telemetry must not change at all.
        # (ingest_events is an arrival log and grows by design — the
        # idempotency claim is about pipeline state, asserted on telemetry.)
        with capture_file.open(encoding="utf-8") as source:
            await replay(host=host, port=port, source=source, speed=None)
        await _wait_until(
            lambda: counters.ingested + counters.deduplicated + counters.dead_lettered >= 2 * total,
            "second replay fully processed",
        )
        assert await _snapshot(clean_db) == replayed

    # Direct ingest of the same wire stream into a clean database produces
    # the identical state — replay through the CLI is the same path.
    await _truncate(clean_db)
    await _prime_session(host, port, "it-cli-direct")
    async with _running_service(host, port, clean_db, "it-cli-direct"):
        async with aiomqtt.Client(host, port) as publisher:
            for record in records:
                await publisher.publish(record.topic, record.payload, qos=1)
        deadline = time.monotonic() + WAIT_SECONDS
        while await _snapshot(clean_db) != replayed:
            if time.monotonic() > deadline:
                direct = await _snapshot(clean_db)
                assert direct == replayed  # fails with the diff
            await asyncio.sleep(0.2)
