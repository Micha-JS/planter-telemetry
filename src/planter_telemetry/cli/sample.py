"""Offline sample generation from the seeded simulator. Pure: no broker,
no clock, no IO — captured_at is the emission's virtual publish time.

The committed sample (samples/telemetry-window.jsonl) is generated from
exactly this module, so replay works on a fresh clone with zero prior
capture. Its window starts in a fixed past moment, keeping replayed rows
disjoint from the now-anchored live simulator — the replay smoke test's
windowed queries rely on that. Regenerate with `make sample`; the
determinism guard in tests/test_sample.py compares bytes.
"""

from collections.abc import Iterator
from datetime import UTC, datetime
from itertools import islice
from typing import Final

from planter_telemetry.cli.records import CapturedMessage
from planter_telemetry.contract import telemetry_topic
from planter_telemetry.simulator.config import SimulatorSettings
from planter_telemetry.simulator.stream import build_streams
from planter_telemetry.simulator.wire import corrupt, encode

SAMPLE_START: Final = datetime(2026, 1, 1, tzinfo=UTC)
SAMPLE_MESSAGE_COUNT: Final = 200
# What ingesting the sample must produce, pinned so drift fails loudly:
# tests/test_sample.py recomputes both through the ingestion classifier, and
# the Makefile's replay-smoke asserts the row count against the live stack.
SAMPLE_UNIQUE_VALID_ROWS: Final = 185
SAMPLE_DEAD_LETTERS: Final = 2


def sample_settings() -> SimulatorSettings:
    """Every generation-relevant knob passed explicitly: SimulatorSettings
    is a BaseSettings, and a stray SIM_* environment variable must not be
    able to silently change the "deterministic" sample."""
    return SimulatorSettings(
        device_count=3,
        seed=42,
        interval_seconds=300.0,
        duplicate_rate=0.05,
        out_of_order_rate=0.03,
        malformed_rate=0.02,
        missed_checkin_rate=0.03,
    )


def sample_records() -> Iterator[CapturedMessage]:
    """The sample stream: 3 devices, ~5.5 virtual hours, imperfections on
    (so a replay demonstrably exercises dedupe, dead-letter, and
    out-of-order handling)."""
    for emission in islice(build_streams(sample_settings(), SAMPLE_START), SAMPLE_MESSAGE_COUNT):
        payload = encode(emission.reading)
        if emission.corruption is not None:
            payload = corrupt(payload, emission.corruption)
        yield CapturedMessage(
            topic=telemetry_topic(emission.reading.device_id),
            payload=payload,
            captured_at=emission.publish_at,
        )
