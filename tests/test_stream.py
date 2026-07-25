"""Fixed-seed checks on imperfection injection and the fleet merge."""

import itertools
import random
from datetime import UTC, datetime, timedelta

from planter_telemetry.simulator.config import SimulatorSettings
from planter_telemetry.simulator.dynamics import device_readings, draw_device_params
from planter_telemetry.simulator.stream import Emission, build_streams, device_emissions
from planter_telemetry.simulator.wire import CorruptionMode, corrupt, encode

START = datetime(2026, 1, 1, tzinfo=UTC)
WAKES = 4000


def _emissions(settings: SimulatorSettings, wakes: int = WAKES) -> list[Emission]:
    params = draw_device_params(random.Random("params:planter-00"))
    readings = device_readings(
        "planter-00",
        params,
        random.Random("dyn:planter-00"),
        START,
        timedelta(seconds=settings.interval_seconds),
    )
    # Bound the wake count (not the emission count): pulling exactly `wakes`
    # readings gives a known N for rate assertions.
    bounded = itertools.islice(readings, wakes)
    return list(device_emissions(bounded, random.Random("imp:planter-00"), settings))


def _default_settings() -> SimulatorSettings:
    return SimulatorSettings(
        duplicate_rate=0.05,
        out_of_order_rate=0.03,
        malformed_rate=0.02,
        missed_checkin_rate=0.03,
    )


def test_injection_rates_roughly_match_config() -> None:
    settings = _default_settings()
    emissions = _emissions(settings)

    duplicates = sum(1 for e in emissions if e.kind == "duplicate")
    late = sum(1 for e in emissions if e.kind == "late")
    malformed = sum(1 for e in emissions if e.corruption is not None and e.kind != "duplicate")
    # Each non-duplicate emission consumes one wake's reading; the remainder
    # were missed check-ins (± a handful still deferred when the run ends).
    missed = WAKES - sum(1 for e in emissions if e.kind != "duplicate")

    def close(observed: int, rate: float) -> bool:
        expected = rate * WAKES
        return abs(observed - expected) <= 0.4 * expected

    assert close(duplicates, settings.duplicate_rate)
    assert close(late, settings.out_of_order_rate)
    assert close(malformed, settings.malformed_rate)
    assert close(missed, settings.missed_checkin_rate)


def test_duplicates_are_byte_identical_to_predecessor() -> None:
    emissions = _emissions(_default_settings())
    for prev, cur in itertools.pairwise(emissions):
        if cur.kind == "duplicate":
            assert cur.reading is prev.reading
            assert (cur.reading.device_id, cur.reading.measured_at) == (
                prev.reading.device_id,
                prev.reading.measured_at,
            )
            prev_bytes = encode(prev.reading)
            cur_bytes = encode(cur.reading)
            if prev.corruption is not None:
                prev_bytes = corrupt(prev_bytes, prev.corruption)
            if cur.corruption is not None:
                cur_bytes = corrupt(cur_bytes, cur.corruption)
            assert cur_bytes == prev_bytes


def test_late_emissions_are_out_of_order() -> None:
    emissions = _emissions(_default_settings())
    late = [e for e in emissions if e.kind == "late"]
    assert late
    for e in late:
        assert e.publish_at > e.reading.measured_at
    # At least one late reading arrives after a newer reading was published.
    seen_newest = START
    truly_out_of_order = 0
    for e in emissions:
        if e.kind == "late" and e.reading.measured_at < seen_newest:
            truly_out_of_order += 1
        seen_newest = max(seen_newest, e.reading.measured_at)
    assert truly_out_of_order >= 1


def test_all_corruption_modes_occur() -> None:
    emissions = _emissions(_default_settings())
    modes = {e.corruption for e in emissions if e.corruption is not None}
    assert modes == set(CorruptionMode)


def test_zero_rates_yield_clean_ordered_stream() -> None:
    settings = SimulatorSettings(
        duplicate_rate=0,
        out_of_order_rate=0,
        malformed_rate=0,
        missed_checkin_rate=0,
    )
    emissions = _emissions(settings, wakes=500)
    assert len(emissions) == 500
    assert all(e.kind == "normal" and e.corruption is None for e in emissions)
    assert all(
        cur.reading.measured_at > prev.reading.measured_at
        for prev, cur in itertools.pairwise(emissions)
    )


def test_deterministic_across_runs() -> None:
    settings = _default_settings()
    assert _emissions(settings, wakes=1000) == _emissions(settings, wakes=1000)


def test_build_streams_merges_fleet_in_publish_order() -> None:
    settings = SimulatorSettings(device_count=4, seed=42)
    emissions = list(itertools.islice(build_streams(settings, START), 500))
    assert {e.reading.device_id for e in emissions} == {
        "planter-00",
        "planter-01",
        "planter-02",
        "planter-03",
    }
    assert all(cur.publish_at >= prev.publish_at for prev, cur in itertools.pairwise(emissions))


def test_build_streams_deterministic_and_seed_sensitive() -> None:
    settings = SimulatorSettings(device_count=2, seed=42)
    first = list(itertools.islice(build_streams(settings, START), 200))
    second = list(itertools.islice(build_streams(settings, START), 200))
    assert first == second

    other_seed = SimulatorSettings(device_count=2, seed=43)
    assert first != list(itertools.islice(build_streams(other_seed, START), 200))


def test_more_devices_do_not_perturb_existing_ones() -> None:
    small = SimulatorSettings(device_count=2, seed=42)
    large = SimulatorSettings(device_count=4, seed=42)
    small_emissions = list(itertools.islice(build_streams(small, START), 400))
    large_emissions = list(itertools.islice(build_streams(large, START), 800))
    # Check every device of the smaller fleet, not just planter-00: phase
    # must not depend on device_count either.
    for device_id in ("planter-00", "planter-01"):
        small_dev = [e for e in small_emissions if e.reading.device_id == device_id]
        large_dev = [e for e in large_emissions if e.reading.device_id == device_id]
        prefix = min(len(small_dev), len(large_dev))
        assert prefix > 0
        assert small_dev[:prefix] == large_dev[:prefix]
