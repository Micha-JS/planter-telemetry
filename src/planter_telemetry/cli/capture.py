"""Capture edge: subscribe to the telemetry topic tree, write JSONL.

The only impurities are the MQTT client, the clock, and the output stream;
record encoding lives in records.py. Each line is flushed as written so
`--out -` pipes cleanly into other tools.
"""

import asyncio
from datetime import UTC, datetime
from typing import TextIO

import aiomqtt

from planter_telemetry.cli.records import CapturedMessage, encode_record


async def capture(
    *,
    host: str,
    port: int,
    topic: str,
    out: TextIO,
    count: int | None,
    duration_seconds: float | None,
) -> int:
    """Write one record per received message; stop after `count` messages
    or `duration_seconds`, whichever comes first. Returns messages written.

    The caller guarantees at least one bound (argparse enforces it) — with
    neither, this would subscribe forever.
    """
    written = 0
    try:
        async with asyncio.timeout(duration_seconds):  # None = no time bound
            async with aiomqtt.Client(host, port) as client:
                await client.subscribe(topic, qos=1)
                async for message in client.messages:
                    record = CapturedMessage(
                        topic=message.topic.value,
                        payload=message.payload,
                        captured_at=datetime.now(UTC),
                    )
                    out.write(encode_record(record) + "\n")
                    out.flush()
                    written += 1
                    if count is not None and written >= count:
                        break
    except TimeoutError:
        pass  # duration reached: a normal way to finish
    return written
