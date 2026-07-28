"""The forecasting core, tested against synthetic series and the simulator's
known ground truth.

Accuracy tolerances are derived, not guessed — each accuracy test's docstring
carries the arithmetic from the simulator's quantization/noise model, and the
assertion uses the derived bound so a reseed cannot turn the test into a
coin flip. Ground truth always comes from DeviceParams and the un-rounded
initial state (replayed from the same seed), never from the reported values.
"""

import itertools
import random
import time
from datetime import UTC, datetime, timedelta

import pytest

from planter_telemetry.analytics.model import (
    BATTERY,
    CROSSING_STATUSES,
    WATER,
    AlertState,
    Forecast,
    Metric,
    Sample,
    Status,
    alert_transition,
    forecast,
    split_segments,
    theil_sen,
)
from planter_telemetry.simulator.dynamics import DeviceParams, device_readings

START = datetime(2026, 1, 1, tzinfo=UTC)
STEP = timedelta(minutes=5)


def _series(values: list[float], step: timedelta = STEP) -> list[Sample]:
    return [Sample(START + i * step, value) for i, value in enumerate(values)]


def _ramp(
    n: int, start_value: float, slope_per_hour: float, step: timedelta = STEP
) -> list[Sample]:
    hours = step.total_seconds() / 3600.0
    return [Sample(START + i * step, start_value + slope_per_hour * i * hours) for i in range(n)]


def _sim_samples(params: DeviceParams, seed: str, n: int) -> tuple[list[Sample], list[Sample]]:
    """(water, battery) series from the simulator's own physics."""
    readings = list(
        itertools.islice(device_readings("planter-00", params, random.Random(seed), START, STEP), n)
    )
    water = [Sample(r.measured_at, r.water_level) for r in readings]
    battery = [Sample(r.measured_at, r.battery_voltage) for r in readings]
    return water, battery


def _initial_state(seed: str) -> tuple[float, float]:
    """The un-rounded initial (level, battery) device_readings will draw.

    Replays the generator's first two RNG draws from the same seed — the
    analytic ground truth the accuracy tests compare against.
    """
    rng = random.Random(seed)
    return rng.uniform(40.0, 100.0), rng.uniform(3.7, 4.2)


def _steady(depletion_pct_per_hour: float = 2.0, battery_decay: float = 0.0025) -> DeviceParams:
    """Refills disabled, no jitter: an exactly linear device."""
    return DeviceParams(
        depletion_pct_per_hour=depletion_pct_per_hour,
        refill_threshold_pct=0.0,
        refill_prob_per_wake=0.0,
        battery_decay_v_per_hour=battery_decay,
        jitter_seconds=0.0,
    )


# --- segmentation ---


def test_refill_points_recovered_exactly() -> None:
    """Refills enabled and frequent: every upward jump in the reported series
    is a segment boundary, and nothing else is."""
    params = DeviceParams(
        depletion_pct_per_hour=3.0,
        refill_threshold_pct=25.0,
        refill_prob_per_wake=1.0,
        battery_decay_v_per_hour=0.002,
        jitter_seconds=0.0,
    )
    water, _ = _sim_samples(params, "segments", 2000)
    expected_breaks = [
        i for i in range(1, len(water)) if water[i].value - water[i - 1].value >= WATER.refill_jump
    ]
    assert len(expected_breaks) > 5  # the series genuinely cycles

    segments = split_segments(water, jump=WATER.refill_jump, gap_factor=WATER.gap_factor)
    breaks = list(itertools.accumulate(len(s) for s in segments[:-1]))
    assert breaks == expected_breaks
    assert sum(len(s) for s in segments) == len(water)
    assert all(segments)  # no empty segments, ever


def test_quantized_depletion_never_splits() -> None:
    """0.1-pct quantization jitters the step sizes but never steps upward, so
    a refill-free series stays one segment."""
    water, _ = _sim_samples(_steady(), "no-split", 1000)
    segments = split_segments(water, jump=WATER.refill_jump, gap_factor=WATER.gap_factor)
    assert len(segments) == 1


def test_consecutive_jumps_yield_length_one_segment() -> None:
    samples = _series([50.0, 40.0, 90.0, 95.0, 90.0, 80.0])
    segments = split_segments(samples, jump=5.0, gap_factor=6.0)
    assert [[s.value for s in seg] for seg in segments] == [
        [50.0, 40.0],
        [90.0],
        [95.0, 90.0, 80.0],
    ]


def test_degenerate_lengths() -> None:
    assert split_segments([], jump=5.0, gap_factor=6.0) == []
    one = _series([50.0])
    assert split_segments(one, jump=5.0, gap_factor=6.0) == [one]
    two = _series([50.0, 49.0])
    assert split_segments(two, jump=5.0, gap_factor=6.0) == [two]


def test_gap_rule_splits_and_tolerates_jitter() -> None:
    """An 8x-median gap splits (device offline, possibly refilled invisibly);
    a 5x gap — below the 6x factor — does not. The gap rule is unreachable
    against the simulator (six consecutive Bernoulli(0.03) misses ~ 7e-10),
    so synthetic series are the only place it gets exercised."""
    before = _ramp(40, 80.0, -1.0)
    gap_start = before[-1].at + 8 * STEP
    after = [Sample(gap_start + i * STEP, 60.0 - i * 0.1) for i in range(40)]
    segments = split_segments(before + after, jump=5.0, gap_factor=6.0)
    assert [len(s) for s in segments] == [40, 40]

    mild_gap = [Sample(before[-1].at + 5 * STEP + i * STEP, 60.0 - i * 0.1) for i in range(40)]
    assert len(split_segments(before + mild_gap, jump=5.0, gap_factor=6.0)) == 1


# --- fit ---


def test_theil_sen_exact_on_noiseless_ramp() -> None:
    fit = theil_sen(_ramp(50, 90.0, -1.5))
    assert fit is not None
    assert fit.slope_per_hour == pytest.approx(-1.5, abs=1e-12)
    assert fit.value_at_anchor == pytest.approx(_ramp(50, 90.0, -1.5)[-1].value, abs=1e-12)
    assert fit.residual_mad == pytest.approx(0.0, abs=1e-12)
    assert fit.anchor_at == START + 49 * STEP
    # The CI collapses onto the point estimate when every pair agrees.
    assert fit.slope_low_per_hour == pytest.approx(-1.5, abs=1e-12)
    assert fit.slope_high_per_hour == pytest.approx(-1.5, abs=1e-12)


def test_water_slope_within_quantization_bound() -> None:
    """Reported water is round(true, 1) on an exactly linear ramp: every
    sample sits within 0.05 of the line, so over a span of T hours no fit
    consistent with the points can be more than 0.1/T off in slope. That
    bound is ~16x the 1-sigma spread of the Theil-Sen estimate (sigma_q =
    0.1/sqrt(12) = 0.0289 pct; sd = sigma*sqrt(12)/(T*sqrt(n))/sqrt(0.955)
    = 2.5e-4 pct/h at T~24 h, n=288), so it holds for any seed."""
    params = _steady(depletion_pct_per_hour=2.0)
    water, _ = _sim_samples(params, "water-accuracy", 288)
    fit = theil_sen(water)
    assert fit is not None
    span_hours = fit.span_seconds / 3600.0
    assert abs(fit.slope_per_hour - (-2.0)) <= 0.1 / span_hours


@pytest.mark.parametrize("seed", ["b1", "b2", "b3", "b4", "b5"])
def test_battery_slope_within_noise_bound(seed: str) -> None:
    """Battery noise: ADC uniform(-0.005, 0.005) plus 0.0005 rounding, in
    quadrature sigma = 2.90e-3 V. Theil-Sen sd = sigma*sqrt(12)/(T*sqrt(n))
    / sqrt(0.955) = 2.53e-5 V/h at T=24 h, n=288. The asserted 1.0e-4 V/h is
    ~4 sigma — parametrized over five seeds to back the claim that this is a
    bound, not a lucky draw. (1e-4 is 5.6% of the slowest simulated decay,
    1.786e-3 V/h, so the bound still means something.)"""
    params = _steady(battery_decay=0.0025)
    _, battery = _sim_samples(params, seed, 288)
    fit = theil_sen(battery)
    assert fit is not None
    assert abs(fit.slope_per_hour - (-0.0025)) <= 1.0e-4


def test_one_glitch_point_does_not_flip_the_fit() -> None:
    """The reason Theil-Sen over least squares: a single glitch (sensor
    dropout reporting 0) inside a clean ramp leaves the median of pairwise
    slopes essentially untouched, where a least-squares slope would be
    dragged far off. This is the estimator protecting against the failure of
    the code above it (a segmenter bug) and below it (a flaky sensor)."""
    samples = _ramp(100, 90.0, -1.0)
    samples[50] = Sample(samples[50].at, 0.0)
    fit = theil_sen(samples)
    assert fit is not None
    assert abs(fit.slope_per_hour - (-1.0)) <= 0.05


def test_duplicate_timestamps_do_not_crash_theil_sen() -> None:
    """Equal-x pairs are skipped per the standard definition; the fit over
    the remaining pairs is still exact on a clean ramp."""
    samples = _ramp(20, 90.0, -1.0)
    samples.insert(10, samples[10])
    fit = theil_sen(samples)
    assert fit is not None
    assert fit.slope_per_hour == pytest.approx(-1.0, abs=1e-9)


def test_fit_cost_stays_bounded() -> None:
    """max_fit_points=300 means 44,850 pairwise slopes. This guard exists so
    nobody raises the cap casually: 5,000 points would be 12.5M pairs and a
    multi-second pass."""
    samples = _ramp(300, 90.0, -0.5)
    begin = time.perf_counter()
    assert theil_sen(samples) is not None
    assert time.perf_counter() - begin < 0.25


# --- forecast statuses and honesty ---


def _fresh(fc_samples: list[Sample]) -> datetime:
    return fc_samples[-1].at


def test_no_data() -> None:
    fc = forecast([], WATER, START)
    assert fc.status is Status.NO_DATA
    assert fc.as_of is None
    assert fc.crosses_at is None


def test_invalid_series_unsorted() -> None:
    samples = _series([50.0, 49.0, 48.0])
    swapped = [samples[1], samples[0], samples[2]]
    assert forecast(swapped, WATER, _fresh(samples)).status is Status.INVALID_SERIES


def test_invalid_series_duplicate_timestamp() -> None:
    samples = _series([50.0, 49.0])
    samples.append(Sample(samples[-1].at, 48.0))
    assert forecast(samples, WATER, _fresh(samples)).status is Status.INVALID_SERIES


def test_invalid_series_non_finite() -> None:
    samples = _series([50.0, float("nan"), 48.0])
    assert forecast(samples, WATER, _fresh(samples)).status is Status.INVALID_SERIES


def test_invalid_series_naive_datetime() -> None:
    samples = [Sample(datetime(2026, 1, 1), 50.0), Sample(datetime(2026, 1, 1, 0, 5), 49.0)]
    assert forecast(samples, WATER, START).status is Status.INVALID_SERIES


def test_stale_before_level_checks() -> None:
    """A device dark past the staleness window reports STALE even when its
    last reading was below target: data too old to act on outranks what the
    data said."""
    samples = _series([50.0, 5.0])
    now = samples[-1].at + timedelta(seconds=WATER.stale_after_seconds + 60)
    assert forecast(samples, WATER, now).status is Status.STALE


def test_at_or_below_target_precedes_fit_gates() -> None:
    """Two samples would never pass min_points — but a planter already at
    the target is dry NOW, and 'not enough points' would be absurd."""
    samples = _series([12.0, 9.0])
    fc = forecast(samples, WATER, _fresh(samples))
    assert fc.status is Status.AT_OR_BELOW_TARGET
    assert fc.crosses_at == samples[-1].at  # horizon zero: attention needed now


def test_level_pinned_at_zero_is_dry_not_not_depleting() -> None:
    """The clamped-at-zero tail is flat (slope 0); precedence must answer
    'dry now', never NOT_DEPLETING."""
    samples = _ramp(200, 60.0, -2.0) + [Sample(START + (200 + i) * STEP, 0.0) for i in range(100)]
    fc = forecast(samples, WATER, _fresh(samples))
    assert fc.status is Status.AT_OR_BELOW_TARGET


def test_insufficient_points_after_refill_never_uses_old_segment() -> None:
    """THE correctness trap: a refill on the newest samples must not fall
    back to the pre-refill segment — that would tell a just-watered planter
    it is about to run dry. The honest answer is INSUFFICIENT_POINTS until
    the new segment has enough history."""
    old_segment = _ramp(300, 80.0, -2.0)
    refilled = [Sample(old_segment[-1].at + (i + 1) * STEP, 95.0 - 0.2 * i) for i in range(5)]
    fc = forecast(old_segment + refilled, WATER, refilled[-1].at)
    assert fc.status is Status.INSUFFICIENT_POINTS
    assert fc.crosses_at is None
    assert fc.fit is None


def test_insufficient_span_on_dense_replay_data() -> None:
    """A replayed capture can deliver hundreds of points spanning minutes;
    min_points alone would pass, and the span gate is what stands between a
    20-minute window and a confident nonsense slope."""
    dense = _ramp(120, 80.0, -2.0, step=timedelta(seconds=30))
    fc = forecast(dense, WATER, _fresh(dense))
    assert fc.status is Status.INSUFFICIENT_SPAN


def test_not_depleting_on_flat_series() -> None:
    flat = _series([50.0] * 40)
    fc = forecast(flat, WATER, _fresh(flat))
    assert fc.status is Status.NOT_DEPLETING
    assert fc.crosses_at is None
    assert fc.fit is not None  # the fit exists; it just never crosses


def test_beyond_horizon_keeps_the_crossing() -> None:
    """A 0.01 pct/h drip crosses in ~9 months — a real forecast, clamped for
    display, not a failure."""
    slow = _ramp(60, 75.0, -0.01)
    fc = forecast(slow, WATER, _fresh(slow))
    assert fc.status is Status.BEYOND_HORIZON
    assert fc.crosses_at is not None
    assert (fc.crosses_at - slow[-1].at).total_seconds() > WATER.max_horizon_seconds


def test_crossing_invariant() -> None:
    """crosses_at is present iff the status says so — over every status this
    suite can produce."""
    cases: list[Forecast] = [
        forecast([], WATER, START),
        forecast(_series([50.0, 40.0, 30.0])[::-1], WATER, START + 2 * STEP),
        forecast(_series([12.0, 9.0]), WATER, START + STEP),
        forecast(_series([50.0] * 40), WATER, START + 39 * STEP),
        forecast(_ramp(60, 75.0, -0.01), WATER, START + 59 * STEP),
        forecast(_ramp(300, 80.0, -2.0), WATER, START + 299 * STEP),
        forecast(_ramp(10, 80.0, -2.0), WATER, START + 9 * STEP),
    ]
    for fc in cases:
        assert (fc.crosses_at is not None) == (fc.status in CROSSING_STATUSES), fc.status


# --- forecast accuracy against the simulator's ground truth ---


def test_water_crossing_within_one_percent() -> None:
    """True empty time is analytic: the un-rounded initial level (replayed
    from the seed) depleting at exactly 2 pct/h crosses the 10-pct target at
    start + (level0-10)/2 hours. Error budget: slope bound 0.1/24h of 2 pct/h
    (0.2%) plus 0.05-pct quantization on the anchor value over a ~20-40 pct
    head-room (~0.2%) — asserted at 1%."""
    params = _steady(depletion_pct_per_hour=2.0)
    seed = "water-crossing"
    level0, _ = _initial_state(seed)
    water, _ = _sim_samples(params, seed, 288)
    fc = forecast(water, WATER, _fresh(water))
    assert fc.status is Status.OK
    assert fc.crosses_at is not None

    true_crossing = START + timedelta(hours=(level0 - WATER.target) / 2.0)
    horizon = (true_crossing - START).total_seconds()
    assert abs((fc.crosses_at - true_crossing).total_seconds()) <= 0.01 * horizon
    # The band is ordered around the point estimate. It is NOT asserted to
    # bracket the truth: water's quantization residuals are a correlated
    # sawtooth, so its interval is optimistically narrow — exactly the
    # documented caveat on theil_sen's CI.
    assert fc.crosses_at_earliest is not None
    assert fc.crosses_at_earliest <= fc.crosses_at
    if fc.crosses_at_latest is not None:
        assert fc.crosses_at_latest >= fc.crosses_at


@pytest.mark.parametrize("seed", ["bc1", "bc2", "bc3"])
def test_battery_crossing_within_eight_percent(seed: str) -> None:
    """Battery is structurally ~10x less precise than water: per-sample decay
    (0.0002 V at 5-minute cadence) is ~4% of the ADC noise band. Budget:
    slope bound 1e-4/2.5e-3 (4%) plus 0.0055 V anchor error over ~0.5 V
    head-room (~1%) — asserted at 8% of the true horizon."""
    params = _steady(battery_decay=0.0025)
    _, battery0 = _initial_state(seed)
    _, battery = _sim_samples(params, seed, 288)
    fc = forecast(battery, BATTERY, _fresh(battery))
    assert fc.status in (Status.OK, Status.BEYOND_HORIZON)
    assert fc.crosses_at is not None

    true_crossing = START + timedelta(hours=(battery0 - BATTERY.target) / 0.0025)
    horizon = (true_crossing - START).total_seconds()
    assert abs((fc.crosses_at - true_crossing).total_seconds()) <= 0.08 * horizon


def test_staleness_adapts_to_a_slow_check_in_cadence() -> None:
    """`now_data_time` is the FLEET watermark, so on hardware that deep-sleeps
    for an hour every pod except the most recent reporter is legitimately a
    cadence behind it. A fixed 30-minute rule would report a healthy fleet as
    dark and never forecast it; the constant is a floor, and the observed
    cadence (x gap_factor, as in the segmenter) raises it."""
    hourly = _ramp(60, 80.0, -0.5, step=timedelta(hours=1))
    assert forecast(hourly, WATER, hourly[-1].at + timedelta(hours=2)).status is not Status.STALE
    # Past 6 x the observed one-hour cadence: dark by the same rule that
    # breaks a segment on an implausible gap.
    assert forecast(hourly, WATER, hourly[-1].at + timedelta(hours=7)).status is Status.STALE


def test_dense_cadence_keeps_the_configured_staleness_floor() -> None:
    """The adaptive rule must not shorten the limit: at the demo's 5-minute
    cadence 6 x 300 s is exactly the 1800 s floor, and dense replay data must
    not make a device 'dark' after a minute."""
    dense = _ramp(60, 80.0, -2.0, step=timedelta(seconds=10))
    assert forecast(dense, WATER, dense[-1].at + timedelta(seconds=1700)).status is not Status.STALE
    assert forecast(dense, WATER, dense[-1].at + timedelta(seconds=1900)).status is Status.STALE


def test_truncated_means_the_window_edge_not_a_lone_segment() -> None:
    """`truncated` is a claim about the WINDOW: that the lookback edge, not a
    refill, chose where the segment begins. A device younger than the window
    has exactly one segment starting at its own first-ever reading — calling
    that truncated claims we know less than we do, and in a fresh deployment
    that is every battery series."""
    samples = _ramp(300, 80.0, -2.0)

    # Window opened three days before the device's first reading: nothing was
    # clipped, and the fit's span is the device's whole life.
    fc = forecast(samples, WATER, _fresh(samples), window_start=samples[0].at - timedelta(days=3))
    assert fc.fit is not None
    assert fc.fit.truncated is False

    # The series runs right up to the edge: older data may well exist beyond
    # it, so the span is a lower bound.
    fc = forecast(samples, WATER, _fresh(samples), window_start=samples[0].at - STEP)
    assert fc.fit is not None
    assert fc.fit.truncated is True

    # No window given: the conservative answer, since the caller has told us
    # nothing about where the data was cut.
    fc = forecast(samples, WATER, _fresh(samples))
    assert fc.fit is not None
    assert fc.fit.truncated is True


def test_truncated_is_false_when_a_refill_starts_the_segment() -> None:
    """Two segments means the newest one starts at a refill, whatever the
    window did."""
    old = _ramp(300, 80.0, -2.0)
    hours = STEP.total_seconds() / 3600.0
    refilled = [Sample(old[-1].at + (i + 1) * STEP, 95.0 - 2.0 * i * hours) for i in range(60)]
    samples = old + refilled
    fc = forecast(samples, WATER, _fresh(samples), window_start=samples[0].at - STEP)
    assert fc.fit is not None
    assert fc.fit.truncated is False


def test_crossing_is_utc_aware() -> None:
    fc = forecast(_ramp(300, 80.0, -2.0), WATER, START + 299 * STEP)
    assert fc.crosses_at is not None
    assert fc.crosses_at.utcoffset() is not None


# --- alert transitions ---


def _fc_with_horizon(hours: float, as_of: datetime = START) -> Forecast:
    return Forecast(
        kind="water",
        status=Status.OK,
        as_of=as_of,
        target=10.0,
        latest_value=30.0,
        crosses_at=as_of + timedelta(hours=hours),
        crosses_at_earliest=None,
        crosses_at_latest=None,
        fit=None,
    )


def _no_crossing_fc(status: Status) -> Forecast:
    return Forecast(
        kind="water",
        status=status,
        as_of=START,
        target=10.0,
        latest_value=50.0,
        crosses_at=None,
        crosses_at_earliest=None,
        crosses_at_latest=None,
        fit=None,
    )


COOLDOWN = 24 * 3600.0


def test_alert_fires_on_transition() -> None:
    fc = _fc_with_horizon(24.0)  # below the 48 h water alert horizon
    assert alert_transition(fc, WATER, None, cooldown_seconds=COOLDOWN) == "firing"
    previous_cleared = AlertState("cleared", START - timedelta(hours=1))
    assert alert_transition(fc, WATER, previous_cleared, cooldown_seconds=COOLDOWN) == "firing"


def test_alert_does_not_refire_within_cooldown() -> None:
    fc = _fc_with_horizon(24.0, as_of=START + timedelta(hours=6))
    previous = AlertState("firing", START)
    assert alert_transition(fc, WATER, previous, cooldown_seconds=COOLDOWN) is None


def test_alert_refires_after_cooldown() -> None:
    """A standing condition gets a periodic reminder, not silence forever."""
    fc = _fc_with_horizon(24.0, as_of=START + timedelta(hours=25))
    previous = AlertState("firing", START)
    assert alert_transition(fc, WATER, previous, cooldown_seconds=COOLDOWN) == "firing"


def test_alert_clears_only_past_hysteresis() -> None:
    previous = AlertState("firing", START)
    barely_recovered = _fc_with_horizon(50.0, as_of=START + timedelta(hours=1))
    assert alert_transition(barely_recovered, WATER, previous, cooldown_seconds=COOLDOWN) is None
    recovered = _fc_with_horizon(80.0, as_of=START + timedelta(hours=1))  # > 1.5 x 48 h
    assert alert_transition(recovered, WATER, previous, cooldown_seconds=COOLDOWN) == "cleared"


def test_alert_ignores_statuses_without_crossing() -> None:
    """STALE / INSUFFICIENT_* carry no evidence the condition ended; the
    state machine waits for a real forecast in either direction."""
    previous = AlertState("firing", START)
    for status in (Status.STALE, Status.INSUFFICIENT_POINTS, Status.NOT_DEPLETING):
        fc = _no_crossing_fc(status)
        assert alert_transition(fc, WATER, previous, cooldown_seconds=COOLDOWN) is None
        assert alert_transition(fc, WATER, None, cooldown_seconds=COOLDOWN) is None


def test_alert_fires_again_after_cleared_cycle() -> None:
    fire = _fc_with_horizon(24.0, as_of=START + timedelta(days=2))
    previous = AlertState("cleared", START + timedelta(days=1))
    assert alert_transition(fire, WATER, previous, cooldown_seconds=COOLDOWN) == "firing"


def test_at_or_below_target_fires_immediately() -> None:
    fc = Forecast(
        kind="water",
        status=Status.AT_OR_BELOW_TARGET,
        as_of=START,
        target=10.0,
        latest_value=8.0,
        crosses_at=START,  # horizon zero
        crosses_at_earliest=START,
        crosses_at_latest=START,
        fit=None,
    )
    assert alert_transition(fc, WATER, None, cooldown_seconds=COOLDOWN) == "firing"


# --- config-shaped overrides stay in the core's vocabulary ---


def test_metric_replace_keeps_core_kind_free() -> None:
    """The config layer retunes targets with dataclasses.replace; the core
    never sees settings. This pins the intended extension mechanism."""
    from dataclasses import replace

    custom = replace(WATER, target=25.0)
    samples = _series([30.0, 24.0])
    assert forecast(samples, custom, _fresh(samples)).status is Status.AT_OR_BELOW_TARGET
    assert isinstance(custom, Metric)
