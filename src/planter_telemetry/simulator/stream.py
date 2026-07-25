"""Imperfection injection and multi-device merge. Pure: virtual time only.

Takes the clean reading series from dynamics and decorates it with the
failure modes the pipeline must survive: duplicates, out-of-order delivery,
malformed payloads, and missed check-ins. Same fixed-draw-order rule as
dynamics: every wake draws all decision values even when unused.
"""

import heapq
import itertools
import random
from collections import deque
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from planter_telemetry.contract import TelemetryV1
from planter_telemetry.simulator.config import SimulatorSettings
from planter_telemetry.simulator.dynamics import (
    device_readings,
    draw_device_params,
)
from planter_telemetry.simulator.wire import CorruptionMode


@dataclass(frozen=True)
class Emission:
    """One message as it should hit the wire, in virtual time.

    publish_at is when the message is sent; for kind="late" it is later than
    reading.measured_at (the reading was held back in transit).
    """

    publish_at: datetime
    reading: TelemetryV1
    corruption: CorruptionMode | None
    kind: Literal["normal", "duplicate", "late"]


def device_emissions(
    readings: Iterator[TelemetryV1],
    rng: random.Random,
    settings: SimulatorSettings,
) -> Iterator[Emission]:
    """Decorate one device's reading series with imperfections.

    Per wake: a missed check-in consumes the reading but emits nothing (the
    device slept through; state still advanced). An out-of-order reading is
    deferred 1-3 wakes and released as kind="late" with its old measured_at.
    A duplicate re-emits the current wake's payload — same bytes, same
    (device_id, measured_at) — mimicking QoS 1 redelivery. Malformed wakes
    carry a corruption tag; modes rotate so every mode occurs.
    """
    corruption_modes = itertools.cycle(CorruptionMode)
    deferred: deque[tuple[int, Emission]] = deque()
    for wake, reading in enumerate(readings):
        missed_roll = rng.random()
        malformed_roll = rng.random()
        duplicate_roll = rng.random()
        defer_roll = rng.random()
        defer_wakes = rng.randint(1, 3)

        # Late messages arrive from the network regardless of whether the
        # device wakes this cycle.
        while deferred and deferred[0][0] <= wake:
            _, held = deferred.popleft()
            yield Emission(
                publish_at=reading.measured_at,
                reading=held.reading,
                corruption=held.corruption,
                kind="late",
            )

        if missed_roll < settings.missed_checkin_rate:
            continue

        corruption = None
        if malformed_roll < settings.malformed_rate:
            corruption = next(corruption_modes)

        emission = Emission(
            publish_at=reading.measured_at,
            reading=reading,
            corruption=corruption,
            kind="normal",
        )
        if defer_roll < settings.out_of_order_rate:
            deferred.append((wake + defer_wakes, emission))
            continue

        yield emission
        if duplicate_roll < settings.duplicate_rate:
            yield Emission(
                publish_at=reading.measured_at,
                reading=reading,
                corruption=corruption,
                kind="duplicate",
            )


def merged_emissions(streams: Sequence[Iterator[Emission]]) -> Iterator[Emission]:
    """Deterministic total order over the interleaved fleet."""
    return heapq.merge(*streams, key=lambda emission: emission.publish_at)


def build_streams(settings: SimulatorSettings, start: datetime) -> Iterator[Emission]:
    """Wire the fleet together: seeded per-device RNGs, staggered phases.

    Devices are independent — each seeds its own RNGs from the master seed
    and its id, so changing device_count never perturbs existing devices.
    String seeds hash platform-independently in CPython, so a seed produces
    the same stream everywhere.
    """
    interval = timedelta(seconds=settings.interval_seconds)
    streams = []
    for index in range(settings.device_count):
        device_id = f"planter-{index:02d}"
        dynamics_rng = random.Random(f"{settings.seed}:{device_id}:dynamics")
        imperfection_rng = random.Random(f"{settings.seed}:{device_id}:imperfections")
        params = draw_device_params(dynamics_rng)
        # Phase comes from a per-device RNG, not from index/device_count:
        # deriving it from the fleet size would shift every device's
        # measured_at series — and thus the (device_id, measured_at)
        # idempotency keys — whenever SIM_DEVICE_COUNT changes.
        phase_rng = random.Random(f"{settings.seed}:{device_id}:phase")
        phase = timedelta(seconds=phase_rng.random() * settings.interval_seconds)
        readings = device_readings(device_id, params, dynamics_rng, start + phase, interval)
        streams.append(device_emissions(readings, imperfection_rng, settings))
    return merged_emissions(streams)
