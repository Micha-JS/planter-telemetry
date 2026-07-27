"""Integration: /healthz and /metrics against real (and dead) dependencies.

Healthy end-to-end is tested with the session containers; the unhealthy
states point the service at freshly-released ports instead of stopping the
shared containers (non-destructive, and start-up-unhealthy exercises the
same 503 path). The healthy→unhealthy→healthy transition is deliberately
untested — it would require restarting session-scoped containers; the flag
resets share the reconnect path proven in test_ingestion_integration.
"""

import asyncio
import logging
import socket
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import aiohttp
import pytest

from planter_telemetry.ingestion.config import IngestSettings
from planter_telemetry.ingestion.ops import HealthState
from planter_telemetry.ingestion.service import Counters, run

pytestmark = pytest.mark.integration

WAIT_SECONDS = 60.0


def _dead_port() -> int:
    """A port that was just free — nothing is listening on it."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _wait_until(predicate: Callable[[], bool], description: str) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    while not predicate():
        if time.monotonic() > deadline:
            pytest.fail(f"timed out waiting for {description}")
        await asyncio.sleep(0.2)


@asynccontextmanager
async def _service_with_ops(
    mqtt_host: str,
    mqtt_port: int,
    dsn: str,
    client_id: str,
    reconnect_initial_seconds: float = 0.1,
) -> AsyncIterator[tuple[HealthState, Counters, asyncio.Task[None]]]:
    settings = IngestSettings(
        mqtt_host=mqtt_host,
        mqtt_port=mqtt_port,
        client_id=client_id,
        db_dsn=dsn,
        reconnect_initial_seconds=reconnect_initial_seconds,
        ops_port=0,  # ephemeral; the bound port comes back via HealthState
    )
    health = HealthState()
    counters = Counters()
    stop = asyncio.Event()
    task = asyncio.create_task(run(settings, counters=counters, stop=stop, health=health))
    try:
        await _wait_until(lambda: health.ops_port is not None, "ops server to bind")
        yield health, counters, task
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=15)


async def _get_json(port: int, path: str) -> tuple[int, dict[str, object]]:
    async with (
        aiohttp.ClientSession() as session,
        session.get(f"http://127.0.0.1:{port}{path}") as response,
    ):
        body: dict[str, object] = await response.json()
        return response.status, body


async def test_healthy_reports_200_and_metrics_serve(
    mqtt_endpoint: tuple[str, int], db_dsn: str, caplog: pytest.LogCaptureFixture
) -> None:
    host, port = mqtt_endpoint
    # The ops server must not access-log: a compose healthcheck probes every
    # 5 s, and each line would carry a formatted request string as its
    # "event" — flooding stderr and breaking the constant-event convention.
    caplog.set_level(logging.INFO, logger="aiohttp.access")
    async with _service_with_ops(host, port, db_dsn, "it-health-ok") as (health, _counters, task):
        assert health.ops_port is not None

        async def _healthy() -> bool:
            status, _ = await _get_json(health.ops_port or 0, "/healthz")
            return status == 200

        deadline = time.monotonic() + WAIT_SECONDS
        while not await _healthy():
            if time.monotonic() > deadline:
                pytest.fail("service never became healthy")
            await asyncio.sleep(0.2)

        status, body = await _get_json(health.ops_port, "/healthz")
        assert status == 200
        assert body["status"] == "healthy"
        assert body["broker_connected"] is True
        assert body["db_connected"] is True
        assert isinstance(body["counters"], dict)

        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{health.ops_port}/metrics") as response,
        ):
            assert response.status == 200
            text = await response.text()
        for name in ("ingested", "deduplicated", "dead_lettered", "out_of_order"):
            assert f"planter_ingestion_{name}_total" in text
        # Platform basics; process_* metrics additionally appear on Linux
        # (the ProcessCollector reads /proc), i.e. inside the container.
        assert "python_info" in text
        assert not [r for r in caplog.records if r.name == "aiohttp.access"]
        assert not task.done()


async def test_broker_down_is_503(db_dsn: str) -> None:
    # reconnect_initial_seconds=30: after the first failed attempt the
    # service sits in its backoff wait, so the assertions below sample the
    # backoff window deterministically.
    async with _service_with_ops(
        "127.0.0.1", _dead_port(), db_dsn, "it-health-nobroker", reconnect_initial_seconds=30
    ) as (
        health,
        _counters,
        task,
    ):
        assert health.ops_port is not None
        status, body = await _get_json(health.ops_port, "/healthz")
        assert status == 503
        assert body["status"] == "unhealthy"
        assert body["broker_connected"] is False
        # The first attempt did connect to the database before the broker
        # connect failed. One second in, the service is inside the 30 s
        # backoff — BOTH flags must already be down: /healthz reporting a
        # live DB connection (or worse, 200) during the backoff would let
        # the compose healthcheck ride out an outage as "healthy".
        await asyncio.sleep(1)
        status, body = await _get_json(health.ops_port, "/healthz")
        assert status == 503
        assert body["broker_connected"] is False
        assert body["db_connected"] is False
        assert not task.done()  # still retrying, not dead


async def test_db_down_is_503(mqtt_endpoint: tuple[str, int]) -> None:
    host, port = mqtt_endpoint
    dead_dsn = f"postgresql://planter:planter@127.0.0.1:{_dead_port()}/planter"
    async with _service_with_ops(host, port, dead_dsn, "it-health-nodb") as (
        health,
        _counters,
        task,
    ):
        assert health.ops_port is not None
        status, body = await _get_json(health.ops_port, "/healthz")
        assert status == 503
        assert body["status"] == "unhealthy"
        assert body["db_connected"] is False
        assert not task.done()
