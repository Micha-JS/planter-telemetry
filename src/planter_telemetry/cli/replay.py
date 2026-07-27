"""Replay edge: read JSONL, publish through MQTT — never into the database.

Replayed traffic must exercise the exact ingestion path live traffic takes;
idempotent writes are what make that safe to repeat. Payloads pass through
byte-identical (measured_at is inside the payload and is deliberately not
rewritten — rewriting would forge new readings and defeat idempotency).
"""

import asyncio
import itertools
import time
from collections.abc import Iterator
from typing import TextIO

import aiomqtt

from planter_telemetry.cli.records import CapturedMessage, decode_record
from planter_telemetry.cli.schedule import replay_offsets


def _decoded(source: TextIO) -> Iterator[CapturedMessage]:
    for number, line in enumerate(source, start=1):
        if not line.strip():
            continue
        try:
            yield decode_record(line)
        except ValueError as exc:
            raise ValueError(f"line {number}: {exc}") from exc


async def replay(*, host: str, port: int, source: TextIO, speed: float | None) -> int:
    """Publish every record on its original topic (QoS 1, awaiting PUBACK).

    speed=None is firehose mode (--no-delay); otherwise captured gaps are
    compressed by `speed`, sleeping against offsets from replay_offsets so
    jitter never accumulates. Streams throughout — stdin never buffers.
    """
    records = _decoded(source)
    offsets: Iterator[float]
    if speed is None:
        offsets = itertools.repeat(0.0)
    else:
        # tee + lockstep zip: the pure pacing math stays in schedule.py and
        # the buffer never grows past one record.
        for_timing, records = itertools.tee(records)
        offsets = replay_offsets((r.captured_at for r in for_timing), speed)

    published = 0
    start = time.monotonic()
    async with aiomqtt.Client(host, port) as client:
        for record, offset in zip(records, offsets, strict=False):
            delay = start + offset - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            await client.publish(record.topic, record.payload, qos=1)
            published += 1
    return published
