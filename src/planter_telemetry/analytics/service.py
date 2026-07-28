"""The impure edge: the interval loop, per-pass connections, shutdown.

Deliberately NOT a copy of ingestion's reconnect loop. Ingestion is
message-driven and must hold a broker connection open; analytics is
poll-driven at a human interval, so each pass opens a fresh connection,
does its work, and closes — a failed pass is logged, counted, reflected in
/healthz, and simply retried whole on the next tick. There are no
half-alive states to reason about.

The first pass runs immediately, before the first sleep: the compose
healthcheck's start_period is sized for "service up and first pass done",
and an interval-first loop would sit unhealthy for a full interval on
every start.
"""

import asyncio
import logging
import signal
import time
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import timedelta

import psycopg

from planter_telemetry.analytics.config import AnalyticsSettings
from planter_telemetry.analytics.db import Store
from planter_telemetry.analytics.model import (
    AlertState,
    AlertTransition,
    Forecast,
    Metric,
    Sample,
    Status,
    alert_transition,
    forecast,
)
from planter_telemetry.analytics.notify import post_ntfy
from planter_telemetry.analytics.ops import HealthState, start_ops_server

logger = logging.getLogger("planter_telemetry.analytics")


@dataclass
class Counters:
    """Running totals; logged periodically, served on /metrics, and
    injectable by tests. Monotonic only — the per-pass observations that go
    up AND down live in Gauges."""

    passes: int = 0
    pass_failures: int = 0
    forecasts_written: int = 0
    alerts_fired: int = 0
    alerts_cleared: int = 0
    notify_failures: int = 0


@dataclass
class Gauges:
    """Last-pass observations for /metrics; not counters, not cumulative."""

    last_pass_duration_seconds: float = 0.0
    devices_forecast: int = 0


async def _wait_for_stop(stop: asyncio.Event, timeout: float) -> None:
    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout)


def _alert_title(device_id: str, kind: str) -> str:
    return f"planter {device_id}: {kind} attention needed"


def _alert_message(device_id: str, fc: Forecast, horizon_seconds: float) -> str:
    days = horizon_seconds / 86400.0
    crosses = fc.crosses_at.strftime("%Y-%m-%d %H:%M UTC") if fc.crosses_at else "now"
    noun = "empty" if fc.kind == "water" else "below charge threshold"
    return f"{fc.kind} {noun} in {days:.1f} days of device time ({crosses})"


async def _handle_alert(
    store: Store,
    settings: AnalyticsSettings,
    counters: Counters,
    device_id: str,
    metric: Metric,
    fc: Forecast,
    previous: AlertState | None,
) -> None:
    """Apply the pure transition rule; record the decision, then (opt-in)
    deliver. The decision row is written before the POST is attempted, so a
    hung notifier can lose a push but never a decision."""
    transition: AlertTransition | None = alert_transition(
        fc, metric, previous, cooldown_seconds=settings.alert_cooldown_hours * 3600.0
    )
    if transition is None or fc.as_of is None or fc.crosses_at is None:
        return
    horizon_seconds = max((fc.crosses_at - fc.as_of).total_seconds(), 0.0)
    event_id = await store.insert_alert_event(
        device_id,
        metric.kind,
        transition,
        fc.as_of,
        fc.crosses_at,
        horizon_seconds,
        metric.alert_horizon_seconds,
    )
    if event_id is None:
        return  # same decision already recorded (re-run over unchanged data)
    extra = {
        "device_id": device_id,
        "kind": metric.kind,
        "horizon_days": round(horizon_seconds / 86400.0, 2),
        "crosses_at": fc.crosses_at.isoformat(),
        "as_of": fc.as_of.isoformat(),
    }
    if transition == "firing":
        counters.alerts_fired += 1
        logger.warning("alert_firing", extra=extra)
        if settings.ntfy_url:
            error = await post_ntfy(
                settings.ntfy_url,
                _alert_title(device_id, metric.kind),
                _alert_message(device_id, fc, horizon_seconds),
            )
            await store.mark_notified(event_id, error)
            if error is not None:
                counters.notify_failures += 1
                # The error is a type name only; the URL never reaches a log.
                logger.warning("notify_failed", extra={"device_id": device_id, "error": error})
    else:
        counters.alerts_cleared += 1
        logger.info("alert_cleared", extra=extra)


async def run_pass(settings: AnalyticsSettings, counters: Counters, gauges: Gauges) -> None:
    """One analytics pass: read the window, forecast every (device, metric),
    write idempotently, apply alert transitions. Integration tests call this
    directly — it owns its connection and is safe to re-run any number of
    times over the same data (the done-criterion)."""
    metrics = settings.metrics()
    store = await Store.connect(settings.db_dsn)
    try:
        now_data = await store.data_now()
        if now_data is None:
            # Empty database: a successful pass with zero forecasts — a fresh
            # volume races the simulator on first up, and unhealthy would be
            # the wrong answer.
            gauges.devices_forecast = 0
            logger.info("pass_complete", extra={"devices": 0, "written": 0, "empty": True})
            return
        lookback = max(metric.lookback_seconds for metric in metrics)
        window = await store.load_window(now_data - timedelta(seconds=lookback))
        alert_states = await store.latest_alert_states()
        written = 0
        for device_id, rows in window.items():
            for metric, samples in (
                (metrics[0], [Sample(at, water) for at, water, _ in rows]),
                (metrics[1], [Sample(at, battery) for at, _, battery in rows]),
            ):
                fc = forecast(samples, metric, now_data)
                if fc.status is Status.NOT_DEPLETING and metric.kind == "water":
                    # Impossible in-segment by construction (water depletion
                    # is monotone): a segmenter bug signal, not weather.
                    logger.warning("water_not_depleting", extra={"device_id": device_id})
                if await store.insert_forecast(device_id, fc):
                    written += 1
                    counters.forecasts_written += 1
                await _handle_alert(
                    store,
                    settings,
                    counters,
                    device_id,
                    metric,
                    fc,
                    alert_states.get((device_id, metric.kind)),
                )
        gauges.devices_forecast = len(window)
        logger.info(
            "pass_complete",
            extra={"devices": len(window), "written": written, "data_now": now_data.isoformat()},
        )
    finally:
        await store.close()


async def _log_stats(counters: Counters, interval_seconds: float) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        logger.info("stats", extra=asdict(counters))


async def run(
    settings: AnalyticsSettings,
    *,
    counters: Counters | None = None,
    gauges: Gauges | None = None,
    stop: asyncio.Event | None = None,
    health: HealthState | None = None,
) -> None:
    """Forecast on an interval until SIGTERM/SIGINT (or an injected stop)."""
    counters = counters if counters is not None else Counters()
    gauges = gauges if gauges is not None else Gauges()
    stop = stop if stop is not None else asyncio.Event()
    health = health if health is not None else HealthState()
    loop = asyncio.get_running_loop()
    handled_signals: list[signal.Signals] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
            handled_signals.append(sig)

    # Before the first pass, so /healthz answers 503 (rather than
    # connection-refused) while the database is unreachable.
    ops_runner, health.ops_port = await start_ops_server(
        settings.ops_host,
        settings.ops_port,
        health,
        counters,
        gauges,
        settings.interval_seconds,
    )
    stats_task = asyncio.create_task(_log_stats(counters, settings.interval_seconds * 4))
    if not settings.ntfy_url:
        # Once at startup, not per pass. Deliberately no URL in the other
        # branch either: the topic URL is a write capability.
        logger.info("notifications_disabled", extra={})
    try:
        while not stop.is_set():
            began = time.monotonic()
            try:
                await run_pass(settings, counters, gauges)
                counters.passes += 1
                health.last_pass_ok = True
            except (psycopg.Error, OSError) as exc:
                # Operational failures only: the model is total, so anything
                # else is a bug that should crash loudly, not loop quietly.
                counters.pass_failures += 1
                health.last_pass_ok = False
                logger.warning(
                    "pass_failed",
                    extra={"error_type": type(exc).__name__, "error": str(exc)},
                )
            health.last_pass_monotonic = time.monotonic()
            gauges.last_pass_duration_seconds = health.last_pass_monotonic - began
            await _wait_for_stop(stop, settings.interval_seconds)
    finally:
        stats_task.cancel()
        with suppress(asyncio.CancelledError):
            await stats_task
        await ops_runner.cleanup()
        for sig in handled_signals:
            loop.remove_signal_handler(sig)
        logger.info("shutdown", extra=asdict(counters))
