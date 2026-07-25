"""Fixed-seed sanity checks on the pure device physics."""

import itertools
import random
from datetime import UTC, datetime, timedelta

from planter_telemetry.contract import TelemetryV1
from planter_telemetry.simulator.dynamics import device_readings, draw_device_params

START = datetime(2026, 1, 1, tzinfo=UTC)
INTERVAL = timedelta(minutes=5)
WAKES = 5000


def _series(seed: str = "test:planter-00", wakes: int = WAKES) -> list[TelemetryV1]:
    rng = random.Random(seed)
    params = draw_device_params(rng)
    readings = device_readings("planter-00", params, rng, START, INTERVAL)
    return list(itertools.islice(readings, wakes))


def test_water_level_bounded() -> None:
    assert all(0.0 <= r.water_level <= 100.0 for r in _series())


def test_water_monotonic_between_refills_and_refills_jump_high() -> None:
    series = _series()
    increases = [
        (prev.water_level, cur.water_level)
        for prev, cur in itertools.pairwise(series)
        if cur.water_level > prev.water_level
    ]
    # Any increase is a refill, and every refill jumps back to near-full.
    assert all(new >= 90.0 for _, new in increases)


def test_refills_occur() -> None:
    series = _series()
    refills = sum(
        1 for prev, cur in itertools.pairwise(series) if cur.water_level > prev.water_level
    )
    # ~17 virtual days with a 2-6 day tank: multiple refills expected.
    assert refills >= 1


def test_battery_bounded_and_net_declining_between_recharges() -> None:
    series = _series()
    voltages = [r.battery_voltage for r in series]
    assert all(2.5 <= v <= 4.4 for v in voltages)

    # Split at recharges (large upward jumps) and check net decline within
    # each long segment; ADC noise makes single steps non-monotonic.
    segment_starts = [0] + [
        i for i in range(1, len(voltages)) if voltages[i] - voltages[i - 1] > 0.5
    ]
    segments = list(itertools.pairwise([*segment_starts, len(voltages)]))
    for begin, end in segments:
        if end - begin > 100:
            assert voltages[begin] > voltages[end - 1]


def test_timestamps_strictly_increasing() -> None:
    series = _series()
    assert all(cur.measured_at > prev.measured_at for prev, cur in itertools.pairwise(series))


def test_same_seed_same_series() -> None:
    assert _series() == _series()


def test_different_seeds_differ() -> None:
    assert _series("test:planter-00") != _series("test:planter-01")
