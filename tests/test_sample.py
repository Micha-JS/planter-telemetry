"""Determinism guard for the committed sample capture.

The committed file must equal regeneration byte-for-byte — replay on a
fresh clone depends on it, and the pinned row counts are what the CI
replay smoke asserts against the live stack.
"""

from itertools import islice
from pathlib import Path

from planter_telemetry.cli.records import decode_record, encode_record
from planter_telemetry.cli.sample import (
    SAMPLE_DEAD_LETTERS,
    SAMPLE_MESSAGE_COUNT,
    SAMPLE_START,
    SAMPLE_UNIQUE_VALID_ROWS,
    sample_records,
    sample_settings,
)
from planter_telemetry.ingestion.core import ValidReading, classify
from planter_telemetry.simulator.stream import build_streams

SAMPLE_PATH = Path(__file__).parent.parent / "samples" / "telemetry-window.jsonl"


def test_committed_sample_equals_regeneration() -> None:
    regenerated = "".join(encode_record(record) + "\n" for record in sample_records())
    assert SAMPLE_PATH.read_text(encoding="utf-8") == regenerated, (
        "samples/telemetry-window.jsonl is stale — regenerate with `make sample`"
    )


def test_pinned_counts_match_the_classifier() -> None:
    unique_valid = set()
    dead_letters = 0
    records = list(sample_records())
    assert len(records) == SAMPLE_MESSAGE_COUNT
    for record in records:
        outcome = classify(record.topic, record.payload)
        if isinstance(outcome, ValidReading):
            unique_valid.add((outcome.reading.device_id, outcome.reading.measured_at))
        else:
            dead_letters += 1
    assert len(unique_valid) == SAMPLE_UNIQUE_VALID_ROWS
    assert dead_letters == SAMPLE_DEAD_LETTERS


def test_sample_exercises_every_imperfection() -> None:
    emissions = list(islice(build_streams(sample_settings(), SAMPLE_START), SAMPLE_MESSAGE_COUNT))
    kinds = {emission.kind for emission in emissions}
    assert "duplicate" in kinds  # dedupe path
    assert "late" in kinds  # out-of-order path
    assert any(emission.corruption is not None for emission in emissions)  # dead-letter path


def test_sample_window_is_disjoint_from_live_data() -> None:
    # The live simulator anchors at "now"; a fixed past window keeps the
    # replay smoke's windowed queries unambiguous.
    for record in sample_records():
        assert record.captured_at.year == 2026
        assert record.captured_at.month == 1
        decoded = decode_record(encode_record(record))
        assert decoded.payload == record.payload  # codec-transparent, always
