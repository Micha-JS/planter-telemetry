"""Per-device physics in virtual time. Pure: no wall clock, no I/O.

Every wake-up draws its random values in a fixed order, even when a draw ends
up unused — this keeps the RNG stream aligned so a branch taken on one wake
never shifts the randomness of later wakes, which is what makes fixed-seed
tests (and M2/M7 replay tests) stable across refactors.
"""

import random
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from planter_telemetry.contract import TelemetryV1

_BATTERY_RECHARGE_BELOW_V = 3.0
_BATTERY_FULL_V = 4.2
_BATTERY_WORKING_RANGE_V = _BATTERY_FULL_V - _BATTERY_RECHARGE_BELOW_V


@dataclass(frozen=True)
class DeviceParams:
    """Fixed per-device characteristics, drawn once from the device RNG."""

    depletion_pct_per_hour: float
    refill_threshold_pct: float
    refill_prob_per_wake: float
    battery_decay_v_per_hour: float
    jitter_seconds: float


def draw_device_params(rng: random.Random) -> DeviceParams:
    """Draw plausible planter characteristics: full tank lasts 2-6 virtual
    days, the owner refills somewhere below 5-25 %, a battery charge lasts
    2-4 virtual weeks."""
    tank_days = rng.uniform(2.0, 6.0)
    battery_days = rng.uniform(14.0, 28.0)
    return DeviceParams(
        depletion_pct_per_hour=100.0 / (tank_days * 24.0),
        refill_threshold_pct=rng.uniform(5.0, 25.0),
        refill_prob_per_wake=rng.uniform(0.2, 0.6),
        battery_decay_v_per_hour=_BATTERY_WORKING_RANGE_V / (battery_days * 24.0),
        jitter_seconds=rng.uniform(0.0, 15.0),
    )


def device_readings(
    device_id: str,
    params: DeviceParams,
    rng: random.Random,
    start: datetime,
    interval: timedelta,
) -> Iterator[TelemetryV1]:
    """Infinite series of true readings for one device.

    Water level is strictly non-increasing between refills (no additive
    noise; 0.1 % quantization preserves monotonicity). Refills jump to
    92-100 %. Battery decays linearly with small ADC noise on the reported
    value and recharges to ~4.2 V when it falls below 3.0 V.
    """
    level = rng.uniform(40.0, 100.0)
    battery = rng.uniform(3.7, _BATTERY_FULL_V)
    # Cap cadence jitter so a short interval can never make time run backwards.
    jitter_limit = min(params.jitter_seconds, interval.total_seconds() / 4.0)
    at = start
    prev_at = start
    while True:
        jitter = rng.uniform(-jitter_limit, jitter_limit)
        refill_roll = rng.random()
        refill_target = rng.uniform(92.0, 100.0)
        recharge_target = rng.uniform(4.15, 4.25)
        adc_noise = rng.uniform(-0.005, 0.005)

        elapsed_hours = (at - prev_at).total_seconds() / 3600.0
        level = max(0.0, level - params.depletion_pct_per_hour * elapsed_hours)
        battery -= params.battery_decay_v_per_hour * elapsed_hours
        if battery < _BATTERY_RECHARGE_BELOW_V:
            battery = recharge_target
        if level < params.refill_threshold_pct and refill_roll < params.refill_prob_per_wake:
            level = refill_target

        yield TelemetryV1(
            device_id=device_id,
            measured_at=at,
            water_level=round(level, 1),
            battery_voltage=min(4.4, max(2.5, round(battery + adc_noise, 3))),
        )
        prev_at = at
        at = at + interval + timedelta(seconds=jitter)
