"""argparse surface: defaults, mutual exclusions, rejected values."""

import pytest

from planter_telemetry.cli.app import (
    DEFAULT_SPEED,
    CaptureCommand,
    ReplayCommand,
    SampleCommand,
    parse_command,
)
from planter_telemetry.contract import TELEMETRY_TOPIC_FILTER


def test_replay_defaults() -> None:
    command = parse_command(["replay"])
    assert command == ReplayCommand(host="localhost", port=1883, speed=DEFAULT_SPEED, source="-")


def test_replay_no_delay_means_no_pacing() -> None:
    command = parse_command(["replay", "--no-delay", "capture.jsonl"])
    assert isinstance(command, ReplayCommand)
    assert command.speed is None
    assert command.source == "capture.jsonl"


def test_replay_explicit_speed() -> None:
    command = parse_command(["replay", "--speed", "10", "--host", "mosquitto"])
    assert command == ReplayCommand(host="mosquitto", port=1883, speed=10.0, source="-")


def test_replay_speed_and_no_delay_are_mutually_exclusive() -> None:
    with pytest.raises(SystemExit):
        parse_command(["replay", "--speed", "10", "--no-delay"])


@pytest.mark.parametrize("speed", ["0", "-5"])
def test_replay_non_positive_speed_rejected(speed: str) -> None:
    with pytest.raises(SystemExit):
        parse_command(["replay", "--speed", speed])


def test_capture_defaults_with_count() -> None:
    command = parse_command(["capture", "--count", "10"])
    assert command == CaptureCommand(
        host="localhost",
        port=1883,
        topic=TELEMETRY_TOPIC_FILTER,
        count=10,
        duration_seconds=None,
        out="-",
    )


def test_capture_duration_only() -> None:
    command = parse_command(["capture", "--duration", "5.5", "--out", "window.jsonl"])
    assert isinstance(command, CaptureCommand)
    assert command.count is None
    assert command.duration_seconds == 5.5
    assert command.out == "window.jsonl"


def test_capture_without_any_bound_rejected() -> None:
    with pytest.raises(SystemExit):
        parse_command(["capture"])


@pytest.mark.parametrize(
    "argv",
    [
        ["capture", "--count", "0"],
        ["capture", "--duration", "0"],
        ["capture", "--duration", "-1"],
    ],
)
def test_capture_non_positive_bounds_rejected(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        parse_command(argv)


def test_sample_defaults_to_stdout() -> None:
    assert parse_command(["sample"]) == SampleCommand(out="-")


def test_unknown_command_rejected() -> None:
    with pytest.raises(SystemExit):
        parse_command(["teleport"])
