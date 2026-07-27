"""The impure edge: MQTT consumption, DB writes, reconnects, shutdown.

Delivery semantics (documented in docs/ingestion.md): QoS 1 plus a persistent
session gives at-least-once delivery from the broker; the idempotent insert
makes redelivery harmless. The service never crashes on message content —
anything unparseable becomes a dead-letter row.

paho acks a QoS 1 message the moment it is buffered, before this service
processes it, so acked-but-unwritten messages are treated as radioactive:
drained to the database on shutdown, salvaged into memory across DB loss,
and flushed (or loudly reported) before exit. See run() and docs/ingestion.md.
"""

import asyncio
import logging
import signal
from collections import deque
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import aiomqtt
import psycopg

from planter_telemetry.contract import TELEMETRY_TOPIC_FILTER
from planter_telemetry.ingestion.config import IngestSettings
from planter_telemetry.ingestion.core import ValidReading, classify
from planter_telemetry.ingestion.db import Writer
from planter_telemetry.ingestion.ops import HealthState, start_ops_server

logger = logging.getLogger("planter_telemetry.ingestion")

# Shutdown-time budget for writing out acked-but-unwritten messages after the
# database was lost; compose's default SIGTERM grace period is 10 s.
_FINAL_FLUSH_TIMEOUT_SECONDS = 5.0


@dataclass
class Counters:
    """Running totals; logged periodically, served on /metrics, and
    injectable by tests.

    out_of_order counts valid readings that arrived older than the newest
    already seen for their device — an in-memory arrival observation (like
    the ingest_events log, a QoS 1 redelivery of an old reading counts as
    both deduplicated and out_of_order). The dashboard's source of truth
    stays the SQL derivation over received_at vs measured_at.
    """

    ingested: int = 0
    deduplicated: int = 0
    dead_lettered: int = 0
    out_of_order: int = 0


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


async def _salvage_queue(
    messages: aiomqtt.client.MessagesIterator, pending: deque[tuple[str, bytes]]
) -> None:
    """Move every message aiomqtt has already buffered into `pending`.

    paho sends the QoS 1 PUBACK when it enqueues a message — before the app
    sees it — so anything in this queue is acked and the broker will never
    redeliver it. Leaving it behind on teardown would be silent loss.
    __anext__ returns queued messages in preference to noticing a
    disconnect, so this drains even when the broker connection is gone.
    """
    with suppress(aiomqtt.MqttError):
        while len(messages) > 0:
            message = await anext(messages)
            pending.append((message.topic.value, message.payload))


async def _handle_one(
    item: tuple[str, bytes],
    writer: Writer,
    counters: Counters,
    latest: dict[str, datetime],
) -> None:
    topic, payload = item
    outcome = classify(topic, payload)
    if isinstance(outcome, ValidReading):
        reading = outcome.reading
        newest = latest.get(reading.device_id)
        if newest is not None and reading.measured_at < newest:
            counters.out_of_order += 1
            logger.debug(
                "out_of_order",
                extra={
                    "device_id": reading.device_id,
                    "measured_at": reading.measured_at.isoformat(),
                },
            )
        else:
            latest[reading.device_id] = reading.measured_at
        # Registry first, so a reading never exists without its device row.
        # Unconditional on purpose: the upsert is idempotent, and a pure
        # redelivery (same measured_at) is a no-op on the registry —
        # last_seen only advances with genuinely new readings.
        await writer.upsert_device(outcome.reading)
        if await writer.insert_reading(outcome.reading):
            counters.ingested += 1
        else:
            await writer.record_dedupe(outcome.reading)
            counters.deduplicated += 1
            logger.debug(
                "deduplicated",
                extra={
                    "device_id": outcome.reading.device_id,
                    "measured_at": outcome.reading.measured_at.isoformat(),
                },
            )
    else:
        await writer.insert_dead_letter(outcome)
        counters.dead_lettered += 1
        logger.warning("dead_letter", extra={"topic": outcome.topic, "reason": outcome.reason})


async def _log_stats(counters: Counters, interval_seconds: float) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        logger.info("stats", extra=asdict(counters))


async def run(
    settings: IngestSettings,
    *,
    counters: Counters | None = None,
    stop: asyncio.Event | None = None,
    health: HealthState | None = None,
) -> None:
    """Consume telemetry until SIGTERM/SIGINT (or an injected stop event).

    One reconnect path: on broker *or* database loss both connections are
    torn down and rebuilt with exponential backoff, so there are no
    half-alive states to reason about. Compose healthchecks only gate the
    first start; this loop is what survives mid-run restarts.
    """
    counters = counters if counters is not None else Counters()
    stop = stop if stop is not None else asyncio.Event()
    health = health if health is not None else HealthState()
    loop = asyncio.get_running_loop()
    handled_signals: list[signal.Signals] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        # docker compose stop/down sends SIGTERM; drain instead of dying
        # mid-write. KeyboardInterrupt is unreliable inside asyncio.
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
            handled_signals.append(sig)

    # Before the reconnect loop, so /healthz answers 503 (rather than
    # connection-refused) while the broker or database is unreachable.
    ops_runner, health.ops_port = await start_ops_server(
        settings.ops_host, settings.ops_port, health, counters
    )
    stats_task = asyncio.create_task(_log_stats(counters, settings.stats_interval_seconds))
    backoff = settings.reconnect_initial_seconds
    # Messages the broker already PUBACKed but that are not yet in the
    # database: the current message plus, on DB loss, everything salvaged
    # from aiomqtt's internal queue. Held across reconnects and replayed
    # first — dropping any of them would be silent loss, and idempotency
    # makes a double-write harmless.
    pending: deque[tuple[str, bytes]] = deque()
    # Newest measured_at seen per device, for the out_of_order counter.
    latest: dict[str, datetime] = {}
    try:
        while not stop.is_set():
            writer: Writer | None = None
            try:
                writer = await Writer.connect(settings.db_dsn)
                health.db_connected = True
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
                    health.broker_connected = True
                    logger.info(
                        "consuming",
                        extra={
                            "topic_filter": TELEMETRY_TOPIC_FILTER,
                            "host": settings.mqtt_host,
                            "port": settings.mqtt_port,
                            "client_id": settings.client_id,
                        },
                    )
                    backoff = settings.reconnect_initial_seconds
                    messages = client.messages
                    try:
                        while pending and not stop.is_set():
                            await _handle_one(pending[0], writer, counters, latest)
                            pending.popleft()
                        while not stop.is_set():
                            message = await _next_message(messages, stop)
                            if message is None:
                                break
                            item = (message.topic.value, message.payload)
                            pending.append(item)
                            await _handle_one(item, writer, counters, latest)
                            pending.popleft()
                        # Stop requested: everything left in aiomqtt's queue
                        # is already acked — write it out before the client
                        # context (and the queue with it) is torn down.
                        while len(messages) > 0:
                            message = await anext(messages)
                            item = (message.topic.value, message.payload)
                            pending.append(item)
                            await _handle_one(item, writer, counters, latest)
                            pending.popleft()
                    except psycopg.OperationalError:
                        # DB gone, but this client — and its queue of acked
                        # messages — is still alive: salvage into `pending`
                        # before the context manager discards the queue.
                        await _salvage_queue(messages, pending)
                        raise
            except (aiomqtt.MqttError, psycopg.OperationalError) as exc:
                if stop.is_set():
                    break  # shutting down; the final flush below owns `pending`
                logger.warning(
                    "reconnect",
                    extra={
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "backoff_seconds": backoff,
                    },
                )
                await _wait_for_stop(stop, backoff)
                backoff = min(backoff * 2, settings.reconnect_max_seconds)
            finally:
                # Both flags drop together: one reconnect path, no half-alive
                # states (a clean stop also lands here, which is accurate —
                # the service is no longer consuming).
                health.broker_connected = False
                health.db_connected = False
                if writer is not None:
                    with suppress(Exception):
                        await writer.close()
        if pending:
            # Last chance for acked-but-unwritten messages (the DB was down
            # when stop arrived): one bounded flush attempt, then loud loss —
            # the broker considers these delivered and will not redeliver.
            try:
                async with asyncio.timeout(_FINAL_FLUSH_TIMEOUT_SECONDS):
                    flush_writer = await Writer.connect(settings.db_dsn)
                    try:
                        while pending:
                            await _handle_one(pending[0], flush_writer, counters, latest)
                            pending.popleft()
                    finally:
                        await flush_writer.close()
            except Exception:
                # The broker considers these delivered and will not redeliver.
                logger.error("acked_messages_dropped", extra={"count": len(pending)})
    finally:
        stats_task.cancel()
        with suppress(asyncio.CancelledError):
            await stats_task
        await ops_runner.cleanup()
        for sig in handled_signals:
            loop.remove_signal_handler(sig)
        logger.info("shutdown", extra=asdict(counters))
