"""The impure edge: MQTT consumption, DB writes, reconnects, shutdown.

Delivery semantics (documented in docs/ingestion.md): QoS 1 plus a persistent
session gives at-least-once delivery from the broker; the idempotent insert
makes redelivery harmless. The service never crashes on message content —
anything unparseable becomes a dead-letter row.
"""

import asyncio
import logging
import signal
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import aiomqtt
import psycopg

from planter_telemetry.contract import TELEMETRY_TOPIC_FILTER
from planter_telemetry.ingestion.config import IngestSettings
from planter_telemetry.ingestion.core import ValidReading, classify
from planter_telemetry.ingestion.db import Writer

logger = logging.getLogger("planter_telemetry.ingestion")


@dataclass
class Counters:
    """Running totals; logged periodically and injectable by tests."""

    ingested: int = 0
    deduplicated: int = 0
    dead_lettered: int = 0


async def _wait_for_stop(stop: asyncio.Event, timeout: float) -> None:
    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout)


async def _next_message(
    messages: aiomqtt.client.MessagesIterator, stop: asyncio.Event
) -> aiomqtt.Message | None:
    """Return the next message, or None once stop is set.

    Races the queue against the stop event so shutdown never cancels a
    message that is already being handled: if a message wins (or ties), it
    is returned and fully processed before the next call notices stop.
    """
    get: asyncio.Task[aiomqtt.Message] = asyncio.ensure_future(anext(messages))
    halt: asyncio.Task[Any] = asyncio.ensure_future(stop.wait())
    try:
        done, _ = await asyncio.wait({get, halt}, return_when=asyncio.FIRST_COMPLETED)
        if get in done:
            return get.result()  # may raise MqttError; handled by the reconnect loop
        get.cancel()
        with suppress(asyncio.CancelledError, aiomqtt.MqttError):
            await get
        return None
    finally:
        halt.cancel()
        with suppress(asyncio.CancelledError):
            await halt


async def _handle_one(item: tuple[str, bytes], writer: Writer, counters: Counters) -> None:
    topic, payload = item
    outcome = classify(topic, payload)
    if isinstance(outcome, ValidReading):
        if await writer.insert_reading(outcome.reading):
            counters.ingested += 1
        else:
            counters.deduplicated += 1
            logger.debug(
                "deduplicated %s @ %s",
                outcome.reading.device_id,
                outcome.reading.measured_at.isoformat(),
            )
    else:
        await writer.insert_dead_letter(outcome)
        counters.dead_lettered += 1
        logger.warning("dead-lettered message on %s: %s", outcome.topic, outcome.reason)


async def _log_stats(counters: Counters, interval_seconds: float) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        logger.info(
            "ingested=%d deduplicated=%d dead_lettered=%d",
            counters.ingested,
            counters.deduplicated,
            counters.dead_lettered,
        )


async def run(
    settings: IngestSettings,
    *,
    counters: Counters | None = None,
    stop: asyncio.Event | None = None,
) -> None:
    """Consume telemetry until SIGTERM/SIGINT (or an injected stop event).

    One reconnect path: on broker *or* database loss both connections are
    torn down and rebuilt with exponential backoff, so there are no
    half-alive states to reason about. Compose healthchecks only gate the
    first start; this loop is what survives mid-run restarts.
    """
    counters = counters if counters is not None else Counters()
    stop = stop if stop is not None else asyncio.Event()
    loop = asyncio.get_running_loop()
    handled_signals: list[signal.Signals] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        # docker compose stop/down sends SIGTERM; drain instead of dying
        # mid-write. KeyboardInterrupt is unreliable inside asyncio.
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
            handled_signals.append(sig)

    stats_task = asyncio.create_task(_log_stats(counters, settings.stats_interval_seconds))
    backoff = settings.reconnect_initial_seconds
    # A message the broker already PUBACKed but whose DB write failed: held
    # across the reconnect and replayed first — dropping it would be silent
    # loss, and idempotency makes a double-write harmless.
    pending: tuple[str, bytes] | None = None
    try:
        while not stop.is_set():
            writer: Writer | None = None
            try:
                writer = await Writer.connect(settings.db_dsn)
                async with aiomqtt.Client(
                    settings.mqtt_host,
                    settings.mqtt_port,
                    identifier=settings.client_id,
                    clean_session=False,  # MQTT 3.1.1 persistent session
                ) as client:
                    # With a persistent session the broker remembers this
                    # subscription, but re-subscribing is idempotent and also
                    # covers a fresh or expired session.
                    await client.subscribe(TELEMETRY_TOPIC_FILTER, qos=1)
                    logger.info(
                        "consuming %s from %s:%d as %r",
                        TELEMETRY_TOPIC_FILTER,
                        settings.mqtt_host,
                        settings.mqtt_port,
                        settings.client_id,
                    )
                    backoff = settings.reconnect_initial_seconds
                    messages = client.messages
                    if pending is not None:
                        await _handle_one(pending, writer, counters)
                        pending = None
                    while not stop.is_set():
                        message = await _next_message(messages, stop)
                        if message is None:
                            break
                        item = (message.topic.value, message.payload)
                        pending = item
                        await _handle_one(item, writer, counters)
                        pending = None
            except (aiomqtt.MqttError, psycopg.OperationalError) as exc:
                if stop.is_set():
                    break
                logger.warning(
                    "connection lost (%s: %s); reconnecting in %.1fs",
                    type(exc).__name__,
                    exc,
                    backoff,
                )
                await _wait_for_stop(stop, backoff)
                backoff = min(backoff * 2, settings.reconnect_max_seconds)
            finally:
                if writer is not None:
                    with suppress(Exception):
                        await writer.close()
    finally:
        stats_task.cancel()
        with suppress(asyncio.CancelledError):
            await stats_task
        for sig in handled_signals:
            loop.remove_signal_handler(sig)
        logger.info(
            "shutting down: ingested=%d deduplicated=%d dead_lettered=%d",
            counters.ingested,
            counters.deduplicated,
            counters.dead_lettered,
        )
