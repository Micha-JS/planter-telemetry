"""Integration: the M3 storage layer against a real TimescaleDB.

Drives the service's own dispatch (_handle_one) with a real Writer — the
full MQTT loop is covered by test_ingestion_integration; the subject here is
what lands in the tables: device-registry semantics and (from 0004 on)
continuous-aggregate correctness.
"""

import itertools
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from statistics import mean
from typing import LiteralString

import psycopg
import pytest

from planter_telemetry.contract import TelemetryV1, telemetry_topic
from planter_telemetry.ingestion.db import Writer
from planter_telemetry.ingestion.service import Counters, _handle_one
from planter_telemetry.simulator.config import SimulatorSettings
from planter_telemetry.simulator.stream import Emission, build_streams
from planter_telemetry.simulator.wire import encode

pytestmark = pytest.mark.integration

START = datetime(2026, 1, 1, tzinfo=UTC)

# device_id -> (first_seen, last_seen)
Registry = dict[str, tuple[datetime, datetime]]


def _emissions(count: int) -> list[Emission]:
    """Clean seeded fleet stream (same shape the M2 integration tests use)."""
    settings = SimulatorSettings(
        device_count=3,
        seed=42,
        duplicate_rate=0.0,
        out_of_order_rate=0.0,
        malformed_rate=0.0,
        missed_checkin_rate=0.0,
    )
    return list(itertools.islice(build_streams(settings, START), count))


async def _ingest(dsn: str, emissions: list[Emission]) -> Counters:
    writer = await Writer.connect(dsn)
    counters = Counters()
    try:
        for emission in emissions:
            item = (telemetry_topic(emission.reading.device_id), encode(emission.reading))
            await _handle_one(item, writer, counters)
    finally:
        await writer.close()
    return counters


async def _registry(dsn: str) -> Registry:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cursor = await conn.execute("SELECT device_id, first_seen, last_seen FROM devices")
        return {row[0]: (row[1], row[2]) for row in await cursor.fetchall()}


def _expected_registry(emissions: list[Emission]) -> Registry:
    expected: Registry = {}
    for emission in emissions:
        at = emission.reading.measured_at
        lo, hi = expected.get(emission.reading.device_id, (at, at))
        expected[emission.reading.device_id] = (min(lo, at), max(hi, at))
    return expected


async def test_ingesting_a_stream_populates_the_registry(clean_db: str) -> None:
    emissions = _emissions(60)
    counters = await _ingest(clean_db, emissions)
    assert counters.ingested == 60

    registry = await _registry(clean_db)
    assert len(registry) == 3
    # first_seen/last_seen are exactly the min/max measured_at per device.
    assert registry == _expected_registry(emissions)


async def test_reingest_advances_last_seen_without_duplicate_rows(clean_db: str) -> None:
    emissions = _emissions(60)
    first_half, second_half = emissions[:30], emissions[30:]

    await _ingest(clean_db, first_half)
    before = await _registry(clean_db)

    # Pure redelivery: readings deduplicate, the registry does not move.
    replay = await _ingest(clean_db, first_half)
    assert (replay.ingested, replay.deduplicated) == (0, 30)
    assert await _registry(clean_db) == before

    # New readings advance last_seen; still one row per device.
    await _ingest(clean_db, second_half)
    after = await _registry(clean_db)
    assert set(after) == set(before)
    assert after == _expected_registry(emissions)


async def test_out_of_order_arrival_converges_on_min_max(clean_db: str) -> None:
    def reading(minute: int) -> TelemetryV1:
        return TelemetryV1(
            device_id="planter-oo",
            measured_at=START + timedelta(minutes=minute),
            water_level=50.0,
            battery_voltage=3.7,
        )

    later, earlier = reading(10), reading(0)

    writer = await Writer.connect(clean_db)
    try:
        for item in (later, earlier):
            await writer.upsert_device(item)
            await writer.insert_reading(item)
    finally:
        await writer.close()

    assert await _registry(clean_db) == {"planter-oo": (earlier.measured_at, later.measured_at)}


async def test_malformed_messages_never_touch_the_registry(clean_db: str) -> None:
    writer = await Writer.connect(clean_db)
    counters = Counters()
    try:
        await _handle_one((telemetry_topic("planter-bad"), b"not json"), writer, counters)
    finally:
        await writer.close()

    assert counters.dead_lettered == 1
    assert await _registry(clean_db) == {}


# (device_id, bucket) -> (water avg/min/max, battery avg/min, sample count)
Rollup = dict[tuple[str, datetime], tuple[float, float, float, float, float, int]]


def _floor_hour(at: datetime) -> datetime:
    return at.replace(minute=0, second=0, microsecond=0)


def _floor_day(at: datetime) -> datetime:
    return at.replace(hour=0, minute=0, second=0, microsecond=0)


def _expected_rollup(emissions: list[Emission], floor: Callable[[datetime], datetime]) -> Rollup:
    groups: dict[tuple[str, datetime], list[TelemetryV1]] = {}
    for emission in emissions:
        reading = emission.reading
        groups.setdefault((reading.device_id, floor(reading.measured_at)), []).append(reading)
    return {
        key: (
            mean(r.water_level for r in readings),
            min(r.water_level for r in readings),
            max(r.water_level for r in readings),
            mean(r.battery_voltage for r in readings),
            min(r.battery_voltage for r in readings),
            len(readings),
        )
        for key, readings in groups.items()
    }


def _rollup(dsn: str, view: LiteralString) -> Rollup:
    query: LiteralString = (
        "SELECT device_id, bucket, water_avg, water_min, water_max,"
        " battery_avg, battery_min, sample_count FROM " + view
    )
    with psycopg.connect(dsn) as conn:
        return {
            (row[0], row[1]): (row[2], row[3], row[4], row[5], row[6], row[7])
            for row in conn.execute(query)
        }


def _refresh(dsn: str, view: LiteralString) -> None:
    # A procedure that refuses to run inside a transaction: autocommit.
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("CALL refresh_continuous_aggregate('" + view + "', NULL, NULL)")


def _assert_rollup_equal(actual: Rollup, expected: Rollup) -> None:
    assert actual.keys() == expected.keys()
    for key, values in expected.items():
        assert actual[key] == pytest.approx(values), key


async def test_hourly_rollup_matches_a_hand_computed_window(clean_db: str) -> None:
    def reading(minute: int, water: float, battery: float) -> TelemetryV1:
        return TelemetryV1(
            device_id="planter-agg",
            measured_at=START + timedelta(hours=10, minutes=minute),
            water_level=water,
            battery_voltage=battery,
        )

    in_window = [reading(5, 60.0, 3.9), reading(25, 50.0, 3.7), reading(45, 40.0, 3.8)]
    next_hour = reading(65, 30.0, 3.6)  # must land in the 11:00 bucket, not ours

    writer = await Writer.connect(clean_db)
    try:
        for item in [*in_window, next_hour]:
            await writer.upsert_device(item)
            await writer.insert_reading(item)
    finally:
        await writer.close()

    # No refresh on purpose: the real-time path must already serve this.
    rollup = _rollup(clean_db, "telemetry_hourly")
    assert rollup[("planter-agg", START + timedelta(hours=10))] == pytest.approx(
        (50.0, 40.0, 60.0, mean([3.9, 3.7, 3.8]), 3.7, 3)
    )
    assert rollup[("planter-agg", START + timedelta(hours=11))] == pytest.approx(
        (30.0, 30.0, 30.0, 3.6, 3.6, 1)
    )


async def test_rollups_match_python_computed_expectation(clean_db: str) -> None:
    emissions = _emissions(200)
    counters = await _ingest(clean_db, emissions)
    assert counters.ingested == 200

    hourly_expected = _expected_rollup(emissions, _floor_hour)
    daily_expected = _expected_rollup(emissions, _floor_day)
    assert len(hourly_expected) >= 10  # several buckets per device, not a trivial pass

    # Real-time aggregation: correct answers before any refresh has run —
    # this is what makes rollups visible within a demo session despite the
    # simulator's future-stamped clock.
    _assert_rollup_equal(_rollup(clean_db, "telemetry_hourly"), hourly_expected)
    _assert_rollup_equal(_rollup(clean_db, "telemetry_daily"), daily_expected)

    # Materialized path: identical numbers after an explicit refresh.
    _refresh(clean_db, "telemetry_hourly")
    _refresh(clean_db, "telemetry_daily")
    _assert_rollup_equal(_rollup(clean_db, "telemetry_hourly"), hourly_expected)
    _assert_rollup_equal(_rollup(clean_db, "telemetry_daily"), daily_expected)


async def test_replayed_stream_leaves_rollups_unchanged(clean_db: str) -> None:
    emissions = _emissions(60)
    await _ingest(clean_db, emissions)
    _refresh(clean_db, "telemetry_hourly")
    before = _rollup(clean_db, "telemetry_hourly")

    replay = await _ingest(clean_db, emissions)
    assert replay.deduplicated == 60
    _refresh(clean_db, "telemetry_hourly")
    assert _rollup(clean_db, "telemetry_hourly") == before
