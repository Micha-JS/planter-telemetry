"""argparse wiring and the console-script entrypoint.

Parsing lands in typed frozen dataclasses so the typed surface is ours,
not the framework's; dispatch opens the streams (the only file IO here)
and hands off to the pure core / async edges.
"""

import argparse
import asyncio
import logging
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TextIO

from planter_telemetry.cli.capture import capture
from planter_telemetry.cli.records import encode_record
from planter_telemetry.cli.replay import replay
from planter_telemetry.cli.sample import sample_records
from planter_telemetry.contract import TELEMETRY_TOPIC_FILTER
from planter_telemetry.jsonlog import configure

logger = logging.getLogger("planter_telemetry.cli")

DEFAULT_SPEED = 100.0


@dataclass(frozen=True)
class CaptureCommand:
    host: str
    port: int
    topic: str
    count: int | None
    duration_seconds: float | None
    out: str


@dataclass(frozen=True)
class ReplayCommand:
    host: str
    port: int
    speed: float | None  # None = firehose (--no-delay)
    source: str


@dataclass(frozen=True)
class SampleCommand:
    out: str


Command = CaptureCommand | ReplayCommand | SampleCommand


def _add_broker_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--host", default="localhost", help="broker host (default: %(default)s)")
    parser.add_argument("--port", type=int, default=1883, help="broker port (default: %(default)s)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="planter-telemetry",
        description="Capture a telemetry window to JSONL, or replay one through the broker.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture_parser = subparsers.add_parser(
        "capture", help="subscribe and write one JSONL record per message"
    )
    _add_broker_arguments(capture_parser)
    capture_parser.add_argument(
        "--topic",
        default=TELEMETRY_TOPIC_FILTER,
        help="topic filter to subscribe to (default: %(default)s)",
    )
    capture_parser.add_argument("--count", type=int, help="stop after N messages")
    capture_parser.add_argument(
        "--duration", type=float, metavar="SECONDS", help="stop after this many seconds"
    )
    capture_parser.add_argument(
        "--out", default="-", help="output file, '-' for stdout (default: %(default)s)"
    )

    replay_parser = subparsers.add_parser(
        "replay",
        help="publish a captured JSONL file through the broker "
        "(via MQTT, never the database — idempotency makes repeats safe)",
    )
    _add_broker_arguments(replay_parser)
    pacing = replay_parser.add_mutually_exclusive_group()
    pacing.add_argument(
        "--speed",
        type=float,
        help=f"compress captured gaps by this factor (default: {DEFAULT_SPEED:g})",
    )
    pacing.add_argument(
        "--no-delay", action="store_true", help="firehose: publish with no pacing at all"
    )
    replay_parser.add_argument(
        "source", nargs="?", default="-", metavar="FILE", help="JSONL file, '-' for stdin (default)"
    )

    sample_parser = subparsers.add_parser(
        "sample", help="regenerate the committed seeded sample capture (offline, deterministic)"
    )
    sample_parser.add_argument(
        "--out", default="-", help="output file, '-' for stdout (default: %(default)s)"
    )
    return parser


def parse_command(argv: Sequence[str]) -> Command:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "capture":
        if args.count is None and args.duration is None:
            parser.error("capture needs --count and/or --duration (it would never stop)")
        if args.count is not None and args.count < 1:
            parser.error("--count must be at least 1")
        if args.duration is not None and args.duration <= 0:
            parser.error("--duration must be positive")
        return CaptureCommand(
            host=args.host,
            port=args.port,
            topic=args.topic,
            count=args.count,
            duration_seconds=args.duration,
            out=args.out,
        )
    if args.command == "replay":
        if args.speed is not None and args.speed <= 0:
            parser.error("--speed must be positive")
        speed = None if args.no_delay else (args.speed if args.speed is not None else DEFAULT_SPEED)
        return ReplayCommand(host=args.host, port=args.port, speed=speed, source=args.source)
    return SampleCommand(out=args.out)


@contextmanager
def _open_out(path: str) -> Iterator[TextIO]:
    if path == "-":
        yield sys.stdout
    else:
        with open(path, "w", encoding="utf-8") as stream:
            yield stream


@contextmanager
def _open_source(path: str) -> Iterator[TextIO]:
    if path == "-":
        yield sys.stdin
    else:
        with open(path, encoding="utf-8") as stream:
            yield stream


def main(argv: Sequence[str] | None = None) -> int:
    command = parse_command(sys.argv[1:] if argv is None else argv)
    configure(service="cli")  # stderr only: stdout may be carrying JSONL
    match command:
        case CaptureCommand():
            with _open_out(command.out) as out:
                written = asyncio.run(
                    capture(
                        host=command.host,
                        port=command.port,
                        topic=command.topic,
                        out=out,
                        count=command.count,
                        duration_seconds=command.duration_seconds,
                    )
                )
            logger.info("capture_complete", extra={"messages": written})
        case ReplayCommand():
            with _open_source(command.source) as source:
                published = asyncio.run(
                    replay(
                        host=command.host,
                        port=command.port,
                        source=source,
                        speed=command.speed,
                    )
                )
            logger.info("replay_complete", extra={"messages": published})
        case SampleCommand():
            with _open_out(command.out) as out:
                for record in sample_records():
                    out.write(encode_record(record) + "\n")
    return 0
