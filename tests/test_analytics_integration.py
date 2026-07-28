"""Integration: the analytics pass against a real TimescaleDB.

The subjects are the done-criteria: forecasts verified against the
simulator's configured depletion rates, re-running a pass over unchanged
data changing nothing, honest no-forecast statuses, the alert firing (and
clearing) as decisions in alert_events, and the dashboard's SQL actually
executable by grafana_reader.

Telemetry is seeded by inserting simulator-generated readings directly —
the MQTT path is M2's subject, not this file's. Ground truth comes from
DeviceParams and the replayed un-rounded initial state, as in
test_analytics_model.
"""

import asyncio
import itertools
import json
import random
import re
import socket
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import aiohttp
import psycopg
import pytest

from planter_telemetry.analytics.config import AnalyticsSettings
from planter_telemetry.analytics.ops import HealthState
from planter_telemetry.analytics.service import Counters, Gauges, run, run_pass
from planter_telemetry.contract import TelemetryV1
from planter_telemetry.simulator.dynamics import DeviceParams, device_readings

pytestmark = pytest.mark.integration

START = datetime(2026, 1, 1, tzinfo=UTC)
STEP = timedelta(minutes=5)
WAIT_SECONDS = 60.0

Snapshot = list[tuple[Any, ...]]


def _settings(dsn: str) -> AnalyticsSettings:
    return AnalyticsSettings(db_dsn=dsn, ops_port=0)


def _steady(depletion_pct_per_hour: float = 2.0, battery_decay: float = 0.0025) -> DeviceParams:
    """Refills disabled, no jitter: an exactly linear device."""
    return DeviceParams(
        depletion_pct_per_hour=depletion_pct_per_hour,
        refill_threshold_pct=0.0,
        refill_prob_per_wake=0.0,
        battery_decay_v_per_hour=battery_decay,
        jitter_seconds=0.0,
    )


def _readings(
    params: DeviceParams, seed: str, n: int, device_id: str = "planter-00"
) -> list[TelemetryV1]:
    return list(
        itertools.islice(device_readings(device_id, params, random.Random(seed), START, STEP), n)
    )


def _initial_state(seed: str) -> tuple[float, float]:
    """The un-rounded initial (level, battery) device_readings will draw."""
    rng = random.Random(seed)
    return rng.uniform(40.0, 100.0), rng.uniform(3.7, 4.2)


async def _insert(dsn: str, readings: list[TelemetryV1]) -> None:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        async with conn.cursor() as cursor:
            await cursor.executemany(
                "INSERT INTO telemetry"
                " (device_id, measured_at, water_level, battery_voltage, schema_version)"
                " VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                [
                    (r.device_id, r.measured_at, r.water_level, r.battery_voltage, 1)
                    for r in readings
                ],
            )


async def _pass(dsn: str) -> Counters:
    counters = Counters()
    await run_pass(_settings(dsn), counters, Gauges())
    return counters


async def _rows(dsn: str, query: str, params: tuple[Any, ...] = ()) -> Snapshot:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cursor = await conn.execute(query.encode(), params)
        return [tuple(row) for row in await cursor.fetchall()]


async def _forecast_row(dsn: str, kind: str) -> tuple[Any, ...]:
    rows = await _rows(
        dsn,
        "SELECT status, crosses_at, slope_per_hour, as_of, latest_value"
        " FROM forecasts_latest WHERE device_id = 'planter-00' AND kind = %s",
        (kind,),
    )
    assert len(rows) == 1
    return rows[0]


async def test_pass_forecasts_match_configured_rates(clean_db: str) -> None:
    """The milestone's core claim: predicted_empty_at lands within a stated
    tolerance of the analytically-true crossing computed from the device's
    configured rates (tolerance derivations in test_analytics_model)."""
    seed = "accuracy"
    level0, battery0 = _initial_state(seed)
    await _insert(clean_db, _readings(_steady(), seed, 288))
    counters = await _pass(clean_db)
    assert counters.forecasts_written == 2  # water + battery, one device

    status, crosses_at, slope, _, _ = await _forecast_row(clean_db, "water")
    assert status == "ok"
    true_crossing = START + timedelta(hours=(level0 - 10.0) / 2.0)
    horizon = (true_crossing - START).total_seconds()
    assert abs((crosses_at - true_crossing).total_seconds()) <= 0.01 * horizon
    assert slope == pytest.approx(-2.0, abs=0.1 / 23.9)  # 0.1/span_hours bound

    status, crosses_at, slope, _, _ = await _forecast_row(clean_db, "battery")
    assert status in ("ok", "beyond_horizon")
    true_crossing = START + timedelta(hours=(battery0 - 3.4) / 0.0025)
    horizon = (true_crossing - START).total_seconds()
    assert abs((crosses_at - true_crossing).total_seconds()) <= 0.08 * horizon


async def test_rerunning_a_pass_changes_nothing(clean_db: str) -> None:
    """The done-criterion inherited from M2: same data window, second pass,
    identical tables — count AND content, not just count."""
    await _insert(clean_db, _readings(_steady(depletion_pct_per_hour=5.0), "idem", 288))
    first_counters = await _pass(clean_db)
    assert first_counters.forecasts_written == 2
    assert first_counters.alerts_fired == 1  # 5 pct/h: horizon well inside 2 d

    select_forecasts = (
        "SELECT device_id, kind, as_of, status, target_value, latest_value, crosses_at,"
        " crosses_at_earliest, crosses_at_latest, slope_per_hour, residual_mad, fit_points,"
        " segment_points, fit_span_seconds, truncated"
        " FROM forecasts ORDER BY device_id, kind, as_of"
    )
    select_alerts = (
        "SELECT device_id, kind, state, as_of, crosses_at, horizon_seconds, threshold_seconds,"
        " notified, notify_error FROM alert_events ORDER BY device_id, kind, as_of, state"
    )
    forecasts_before = await _rows(clean_db, select_forecasts)
    alerts_before = await _rows(clean_db, select_alerts)

    second_counters = await _pass(clean_db)
    assert second_counters.forecasts_written == 0
    assert second_counters.alerts_fired == 0
    assert await _rows(clean_db, select_forecasts) == forecasts_before
    assert await _rows(clean_db, select_alerts) == alerts_before


async def test_refill_resets_the_forecast_honestly(clean_db: str) -> None:
    """A refill mid-window first yields INSUFFICIENT_POINTS (never the old
    tank's rate), then a fresh forecast at the new segment's rate once it
    has history. The rate must never go absurd across the jump."""
    params = _steady(depletion_pct_per_hour=2.0)
    await _insert(clean_db, _readings(params, "refill", 288))
    await _pass(clean_db)
    status, _, slope_before, _, _ = await _forecast_row(clean_db, "water")
    assert status == "ok"

    # The owner refills to 95 and the plant drinks slower (1.0 pct/h).
    last_at = START + 287 * STEP
    hours = STEP.total_seconds() / 3600.0
    refilled = [
        TelemetryV1(
            device_id="planter-00",
            measured_at=last_at + (i + 1) * STEP,
            water_level=round(95.0 - 1.0 * i * hours, 1),
            battery_voltage=3.9,
        )
        for i in range(20)
    ]
    await _insert(clean_db, refilled)
    await _pass(clean_db)
    status, crosses_at, slope, _, _ = await _forecast_row(clean_db, "water")
    assert status == "insufficient_points"  # 20 points < 36: honest silence
    assert crosses_at is None
    assert slope is None

    more = [
        TelemetryV1(
            device_id="planter-00",
            measured_at=last_at + (i + 1) * STEP,
            water_level=round(95.0 - 1.0 * i * hours, 1),
            battery_voltage=3.9,
        )
        for i in range(20, 60)
    ]
    await _insert(clean_db, more)
    await _pass(clean_db)
    status, crosses_at, slope, as_of, _ = await _forecast_row(clean_db, "water")
    assert status == "ok"
    assert slope == pytest.approx(-1.0, abs=0.05)  # the NEW rate, not -2.0
    assert slope != pytest.approx(slope_before, abs=0.5)
    # Sane crossing: (95 - 10) / 1.0 = 85 h from the refill, forward in time.
    assert crosses_at is not None
    assert crosses_at > as_of


async def test_fresh_device_gets_no_forecast(clean_db: str) -> None:
    await _insert(clean_db, _readings(_steady(), "fresh", 10))
    counters = await _pass(clean_db)
    assert counters.forecasts_written == 2  # the honest status IS recorded
    for kind in ("water", "battery"):
        status, crosses_at, slope, _, _ = await _forecast_row(clean_db, kind)
        assert status == "insufficient_points"
        assert crosses_at is None
        assert slope is None


async def test_horizon_ticks_down_with_data_time(clean_db: str) -> None:
    """The clock check: within one depletion segment the horizon shrinks at
    exactly one day per day of device time — consecutive forecasts must
    satisfy horizon_i - horizon_j ~= as_of_j - as_of_i."""
    params = _steady(depletion_pct_per_hour=1.5)
    readings = _readings(params, "clock", 336)
    await _insert(clean_db, readings[:288])
    await _pass(clean_db)
    await _insert(clean_db, readings[288:312])  # +2 h of device time
    await _pass(clean_db)
    await _insert(clean_db, readings[312:336])  # +2 h more
    await _pass(clean_db)

    rows = await _rows(
        clean_db,
        "SELECT as_of, EXTRACT(EPOCH FROM (crosses_at - as_of))::double precision"
        " FROM forecasts WHERE kind = 'water' AND status = 'ok' ORDER BY as_of",
    )
    assert len(rows) == 3
    for (earlier_as_of, earlier_horizon), (later_as_of, later_horizon) in itertools.pairwise(rows):
        elapsed = (later_as_of - earlier_as_of).total_seconds()
        shrank = earlier_horizon - later_horizon
        assert abs(shrank - elapsed) <= 0.02 * elapsed + 900.0


async def test_alert_fires_clears_and_survives_restart(clean_db: str) -> None:
    """The alert done-criterion, as decisions in alert_events: a fast
    depleting device fires; re-deriving from unchanged data (which is also
    what a service restart does — state lives in the table, not the process)
    does not re-fire; a refill's long-horizon forecast clears."""
    fast = _steady(depletion_pct_per_hour=5.0)  # horizon ~8-18 h << 2 d
    await _insert(clean_db, _readings(fast, "alert", 288))
    counters = await _pass(clean_db)
    assert counters.alerts_fired == 1
    firing = await _rows(
        clean_db,
        "SELECT state, notified, notify_error, horizon_seconds FROM alert_events"
        " WHERE device_id = 'planter-00' AND kind = 'water'",
    )
    assert len(firing) == 1
    state, notified, notify_error, horizon_seconds = firing[0]
    assert state == "firing"
    assert notified is False and notify_error is None  # ntfy off by default
    assert horizon_seconds <= 2 * 86400.0

    # Restart-equivalent: a fresh pass over the same data. No re-fire.
    counters = await _pass(clean_db)
    assert counters.alerts_fired == 0

    # Refill to a slow drip: horizon (95-10)/0.5 = 170 h > 1.5 x 48 h.
    last_at = START + 287 * STEP
    hours = STEP.total_seconds() / 3600.0
    recovered = [
        TelemetryV1(
            device_id="planter-00",
            measured_at=last_at + (i + 1) * STEP,
            water_level=round(95.0 - 0.5 * i * hours, 1),
            battery_voltage=3.9,
        )
        for i in range(40)
    ]
    await _insert(clean_db, recovered)
    counters = await _pass(clean_db)
    assert counters.alerts_cleared == 1
    states = await _rows(
        clean_db,
        "SELECT state FROM alert_events WHERE device_id = 'planter-00' AND kind = 'water'"
        " ORDER BY as_of",
    )
    assert [row[0] for row in states] == ["firing", "cleared"]


async def test_empty_database_is_a_successful_pass(clean_db: str) -> None:
    """A fresh volume races the simulator on first up: max(measured_at) is
    NULL, and the pass must succeed with zero forecasts rather than mark the
    service unhealthy."""
    counters = await _pass(clean_db)
    assert counters.forecasts_written == 0
    assert await _rows(clean_db, "SELECT count(*) FROM forecasts") == [(0,)]


# --- the service loop and its health surface ---


async def _wait_until(predicate: Callable[[], bool], description: str) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    while not predicate():
        if time.monotonic() > deadline:
            pytest.fail(f"timed out waiting for {description}")
        await asyncio.sleep(0.2)


async def _get_json(port: int, path: str) -> tuple[int, dict[str, object]]:
    async with (
        aiohttp.ClientSession() as session,
        session.get(f"http://127.0.0.1:{port}{path}") as response,
    ):
        body: dict[str, object] = await response.json()
        return response.status, body


def _dead_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def test_service_healthz_healthy_then_shutdown(clean_db: str) -> None:
    """The loop end-to-end on an empty database: first pass runs immediately,
    /healthz reports 200 (empty = success), shutdown is graceful."""
    settings = AnalyticsSettings(db_dsn=clean_db, ops_port=0, interval_seconds=3600.0)
    health = HealthState()
    counters = Counters()
    stop = asyncio.Event()
    task = asyncio.create_task(run(settings, counters=counters, stop=stop, health=health))
    try:
        await _wait_until(lambda: health.last_pass_monotonic is not None, "first pass")
        assert health.ops_port is not None
        status, body = await _get_json(health.ops_port, "/healthz")
        assert status == 200
        assert body["last_pass_ok"] is True
        assert counters.passes == 1

        metrics_status, _ = await _get_json(health.ops_port, "/healthz")
        assert metrics_status == 200
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=15)


async def test_service_healthz_unhealthy_on_unreachable_database() -> None:
    """Passes fail against a dead port; /healthz must say so (503), and the
    failure is counted, not crashed on."""
    settings = AnalyticsSettings(
        db_dsn=f"postgresql://planter:planter@127.0.0.1:{_dead_port()}/planter",
        ops_port=0,
        interval_seconds=3600.0,
    )
    health = HealthState()
    counters = Counters()
    stop = asyncio.Event()
    task = asyncio.create_task(run(settings, counters=counters, stop=stop, health=health))
    try:
        await _wait_until(lambda: health.last_pass_monotonic is not None, "first pass attempt")
        assert health.ops_port is not None
        status, body = await _get_json(health.ops_port, "/healthz")
        assert status == 503
        assert body["last_pass_ok"] is False
        assert counters.pass_failures >= 1
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=15)


# --- the dashboard's SQL, executed as the dashboard's role ---


def _panel_queries() -> list[tuple[str, str]]:
    dashboard = json.loads(
        (Path(__file__).parent.parent / "grafana" / "dashboards" / "planter-fleet.json").read_text()
    )
    queries: list[tuple[str, str]] = []
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            queries.append((panel["title"], target["rawSql"]))
        for nested in panel.get("panels", []):
            for target in nested.get("targets", []):
                queries.append((nested["title"], target["rawSql"]))
    return queries


def _executable(sql: str) -> str:
    """Grafana macros/variables -> plain SQL, the same way Grafana would."""
    sql = re.sub(r"\$__timeFilter\(([^)]+)\)", r"\1 IS NOT NULL", sql)
    sql = sql.replace("${planter:sqlstring}", "'planter-00'")
    return sql


async def test_grafana_reader_can_execute_every_panel_query(clean_db: str) -> None:
    """Nothing else proves 0007's grants actually cover what the dashboard
    reads: connect as grafana_reader and run every panel's rawSql. Catches a
    missing grant, a dropped column, and a view typo — each of which is 'the
    dashboard is blank at demo time'."""
    await _insert(clean_db, _readings(_steady(), "grafana", 60))
    await _pass(clean_db)

    parts = urlsplit(clean_db.replace("postgresql://", "http://"))
    reader_dsn = f"postgresql://grafana_reader:grafana_reader@{parts.hostname}:{parts.port}/planter"
    queries = _panel_queries()
    assert len(queries) >= 10  # the dashboard genuinely was parsed
    async with await psycopg.AsyncConnection.connect(reader_dsn) as conn:
        for title, raw_sql in queries:
            cursor = await conn.execute(_executable(raw_sql).encode())
            await cursor.fetchall()  # any missing grant/column raises here
