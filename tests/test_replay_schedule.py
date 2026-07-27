"""Replay pacing: pure function, zero sleeps."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from itertools import islice

import pytest

from planter_telemetry.cli.schedule import replay_offsets

START = datetime(2026, 1, 1, tzinfo=UTC)


def _times(*seconds: float) -> list[datetime]:
    return [START + timedelta(seconds=s) for s in seconds]


def test_empty_stream_yields_nothing() -> None:
    assert list(replay_offsets([], 100.0)) == []


def test_single_message_publishes_immediately() -> None:
    assert list(replay_offsets(_times(0), 100.0)) == [0.0]


def test_uniform_gaps_compress_by_speed() -> None:
    # 300 s captured gaps at 100x -> 3 s wall-clock steps.
    offsets = list(replay_offsets(_times(0, 300, 600, 900), 100.0))
    assert offsets == [0.0, 3.0, 6.0, 9.0]


def test_speed_one_is_identity() -> None:
    offsets = list(replay_offsets(_times(0, 10, 25), 1.0))
    assert offsets == [0.0, 10.0, 25.0]


def test_negative_gap_clamps_to_zero_and_preserves_order() -> None:
    # An out-of-order capture (late injection): the late message goes out
    # immediately, file order intact, and later gaps still count.
    offsets = list(replay_offsets(_times(0, 300, 100, 400), 100.0))
    assert offsets == [0.0, 3.0, 3.0, 6.0]
    assert offsets == sorted(offsets)  # non-decreasing, always


def test_mixed_gaps_accumulate() -> None:
    offsets = list(replay_offsets(_times(0, 50, 50, 250), 10.0))
    assert offsets == [0.0, 5.0, 5.0, 25.0]


@pytest.mark.parametrize("speed", [0.0, -1.0])
def test_non_positive_speed_rejected(speed: float) -> None:
    with pytest.raises(ValueError, match="speed"):
        list(replay_offsets(_times(0), speed))


def test_streaming_not_buffering() -> None:
    # The iterator must consume lazily — stdin replay depends on it.
    def infinite() -> Iterator[datetime]:
        second = 0
        while True:
            yield START + timedelta(seconds=second)
            second += 300

    offsets = list(islice(replay_offsets(infinite(), 100.0), 3))
    assert offsets == [0.0, 3.0, 6.0]
