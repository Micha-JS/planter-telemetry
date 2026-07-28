"""Pure forecasting core: samples in, a dated crossing (or an honest status) out.

One code path serves water and battery. Everything the two differ in lives in
`Metric`; nothing below `Metric` branches on kind, so a third falling metric
is a new constant, not a new function. Total by construction, like
ingestion.core.classify: a malformed series (unsorted, duplicate timestamps,
non-finite, naive datetimes) returns INVALID_SERIES rather than raising, so
the service never has to trust the database.

All time here is DATA time (measured_at). The entry point takes an explicit
`now_data_time` — the fleet-wide newest measured_at, supplied by the caller —
and nothing in this module reads a clock. Under the simulator's accelerated
clock that is what makes "days until empty" mean days of *device* time; on
real hardware data time ≈ wall time and the distinction costs nothing.

The model is linear-per-segment, deliberately: the simulator's dynamics are
exactly linear between refills, and for real hardware a straight line over
the most recent depletion segment is the simplest model that is honest about
its inputs. A better model (LiPo discharge curves, temperature-aware uptake)
slots into `theil_sen` without touching segmentation, gating, or the service.
See docs/analytics.md for what the linearity assumption hides on real
hardware.
"""

import itertools
import math
import statistics
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from statistics import NormalDist
from typing import Literal

_MIN_FIT_POINTS = 3  # below this a slope median is meaningless


@dataclass(frozen=True)
class Sample:
    """One (device time, value) observation."""

    at: datetime
    value: float


@dataclass(frozen=True)
class Metric:
    """The only place water and battery differ.

    Both metrics fall toward a floor and are replenished by an upward step,
    so the core splits segments on upward jumps only. A metric that rose
    toward a ceiling would need a sign parameter; that parameter is
    deliberately absent until such a metric exists.
    """

    kind: str  # "water" | "battery"; the DB discriminator
    unit: str  # "pct" | "V"
    target: float  # the level the forecast answers "when do we cross" for
    refill_jump: float  # upward step between consecutive samples => new segment
    min_points: int  # newest-segment floor before any fit is attempted
    min_span_seconds: float  # fitted-span floor (dense replay data defeats min_points)
    max_fit_points: int  # Theil-Sen is O(n^2); hard cap, most recent points win
    lookback_seconds: float  # window handed to the fit; exceeds the longest segment
    gap_factor: float  # x median inter-sample interval => segment break
    stale_after_seconds: float  # FLOOR for the staleness limit; the cadence rule may raise it
    max_horizon_seconds: float  # crossings further out than this are BEYOND_HORIZON
    alert_horizon_seconds: float  # horizon at or below this => alert fires


# Water: target 10 % because wick delivery fails before the tank reads 0 —
# waiting for 0 % forecasts a moment that arrives after the plant is already
# dry. refill_jump 5.0 is a real-sensor margin, not simulator-derived: the
# simulator's refills jump >= 67 pct (from below 25 to 92-100) and its
# depletion is noise-free, so against the demo anything above one 0.1-pct
# quantization step would do; capacitive soil probes on real hardware jitter
# far more. min_points/min_span: 36 points and 3 h — below a 3 h span the
# 0.1-pct quantization alone allows a slope error larger than 5 % of the
# slowest simulated depletion rate (derivation in docs/analytics.md).
WATER = Metric(
    kind="water",
    unit="pct",
    target=10.0,
    refill_jump=5.0,
    min_points=36,
    min_span_seconds=3 * 3600.0,
    max_fit_points=300,
    lookback_seconds=10 * 86400.0,
    gap_factor=6.0,
    stale_after_seconds=1800.0,
    max_horizon_seconds=30 * 86400.0,
    alert_horizon_seconds=2 * 86400.0,
)

# Battery: target 3.4 V — the pod's working cutoff is ~3.0 V, and the point
# of a forecast is warning *before* the cutoff, with margin to actually act.
# refill_jump 0.05 V sits 4.5x above the worst-case spurious upward step from
# ADC noise (2 x 0.005 V + 0.001 V rounding) and 23x below a real recharge
# (>= 1.15 V). min_span 12 h because battery decay per sample is ~5 % of the
# per-sample ADC noise: below 12 h the fitted slope is mostly noise
# (docs/analytics.md has the arithmetic — battery needs 4x water's span for
# the same relative slope error). Lookback 35 d: battery segments run to 28
# virtual days, and a lookback shorter than the longest segment truncates it,
# under-reporting span.
BATTERY = Metric(
    kind="battery",
    unit="V",
    target=3.4,
    refill_jump=0.05,
    min_points=144,
    min_span_seconds=12 * 3600.0,
    max_fit_points=300,
    lookback_seconds=35 * 86400.0,
    gap_factor=6.0,
    stale_after_seconds=1800.0,
    max_horizon_seconds=60 * 86400.0,
    alert_horizon_seconds=3 * 86400.0,
)

METRICS: tuple[Metric, ...] = (WATER, BATTERY)


class Status(StrEnum):
    """Why a forecast does or does not carry a crossing time. Always populated.

    Only OK, BEYOND_HORIZON and AT_OR_BELOW_TARGET carry `crosses_at`; the
    invariant is asserted in tests. BEYOND_HORIZON is not a failure — the
    crossing is real, just further out than the display horizon. NOT_DEPLETING
    on water is impossible by construction inside a segment (depletion is
    monotone), so the service logs it as a segmenter-bug signal; on battery it
    is ordinary noise on a short window.
    """

    OK = "ok"
    NO_DATA = "no_data"
    STALE = "stale"
    INVALID_SERIES = "invalid_series"
    INSUFFICIENT_POINTS = "insufficient_points"
    INSUFFICIENT_SPAN = "insufficient_span"
    AT_OR_BELOW_TARGET = "at_or_below_target"
    NOT_DEPLETING = "not_depleting"
    BEYOND_HORIZON = "beyond_horizon"


# Statuses whose Forecast carries a crossing time — the invariant tests assert.
CROSSING_STATUSES = frozenset({Status.OK, Status.BEYOND_HORIZON, Status.AT_OR_BELOW_TARGET})


@dataclass(frozen=True)
class Fit:
    """Robust linear fit over one segment, in metric units per hour.

    x is centred on the newest sample, so `value_at_anchor` is the fitted
    value *now* — the number the crossing extrapolation starts from — with no
    second evaluation and no cancellation from epoch-sized coordinates.
    """

    slope_per_hour: float
    slope_low_per_hour: float  # distribution-free CI over the pairwise slopes
    slope_high_per_hour: float
    value_at_anchor: float
    anchor_at: datetime
    residual_mad: float  # median |residual|, same units as the values
    points: int  # points actually fitted (<= max_fit_points)
    segment_points: int  # points in the whole segment
    span_seconds: float  # span of the fitted points
    truncated: bool  # segment began at the lookback edge, so its true span may be longer


@dataclass(frozen=True)
class Forecast:
    """One device, one metric, as of one device-time watermark.

    `as_of` is the newest sample that fed the decision — the idempotency key
    in the forecasts table — and is None only when there were no samples at
    all (NO_DATA), in which case there is nothing to key a row on.
    """

    kind: str
    status: Status
    as_of: datetime | None
    target: float
    latest_value: float | None
    crosses_at: datetime | None
    crosses_at_earliest: datetime | None
    crosses_at_latest: datetime | None
    fit: Fit | None


def _is_valid_series(samples: Sequence[Sample]) -> bool:
    """Finite values, aware datetimes, strictly increasing timestamps.

    The db layer's ORDER BY plus the (device_id, measured_at) uniqueness
    guarantee all of this for rows read from telemetry; the core validates
    anyway because it accepts a Sequence[Sample] from anywhere — tests,
    replay files, hardware adapters — and a duplicate timestamp would other-
    wise divide by zero deep inside the pairwise slopes.
    """
    for sample in samples:
        if not math.isfinite(sample.value) or sample.at.tzinfo is None:
            return False
    return all(a.at < b.at for a, b in itertools.pairwise(samples))


def cadence_seconds(samples: Sequence[Sample]) -> float | None:
    """The observed check-in interval: median gap between consecutive samples.

    One definition, three users — the segmenter's gap rule, the staleness
    rule, and the window-edge test behind `truncated` — so all three adapt to
    the fleet's actual cadence instead of to a configured one. None when
    there is no positive gap to measure (fewer than two samples).
    """
    deltas = [d for a, b in itertools.pairwise(samples) if (d := (b.at - a.at).total_seconds()) > 0]
    return statistics.median(deltas) if deltas else None


def split_segments(
    samples: Sequence[Sample], *, jump: float, gap_factor: float
) -> list[list[Sample]]:
    """Break the series at refills/recharges and at implausible time gaps.

    The value rule is what actually fires against the simulator: a refill
    jumps from below 25 % to 92-100 %, so the smallest possible step is 67
    pct, and even a refill landing on a missed check-in still shows up as a
    step on the next emitted sample. The time rule exists for the hardware
    case a value rule cannot see — a device offline for a day that returns
    refilled and partly depleted, which would otherwise smear two regimes
    into one fit. Against the simulator it is effectively unreachable
    (missed check-ins are Bernoulli(0.03), so six in a row is ~7e-10 per
    wake), which is exactly why it is unit-tested on synthetic series rather
    than trusted to the demo.

    The gap limit adapts to the observed cadence (median inter-sample
    interval) instead of a configured one, so replayed captures and hardware
    with a different deep-sleep interval need no retuning.
    """
    series = list(samples)
    if len(series) < 2:
        return [series] if series else []
    cadence = cadence_seconds(series)
    gap_limit = cadence * gap_factor if cadence is not None else math.inf

    segments: list[list[Sample]] = []
    start = 0
    for index, (a, b) in enumerate(itertools.pairwise(series), start=1):
        if b.value - a.value >= jump or (b.at - a.at).total_seconds() > gap_limit:
            segments.append(series[start:index])
            start = index
    segments.append(series[start:])
    return segments


def theil_sen(samples: Sequence[Sample], *, confidence: float = 0.95) -> Fit | None:
    """Median of pairwise slopes, with a distribution-free CI over them.

    Why Theil-Sen and not least squares: not for textbook outlier robustness
    — stored telemetry has no outliers (duplicates are absorbed by the
    primary key, malformed payloads never reach the table) — but because the
    estimator should not amplify a bug in the code above it. If a refill
    slips past split_segments, or a real sensor glitches, one 67-pct step
    inside a 300-point window destroys a least-squares slope; the median of
    pairwise slopes tolerates ~29 % contamination and still answers
    correctly. The efficiency cost is <= 5 % (asymptotic relative efficiency
    0.955 under Gaussian noise, ~1.0 under the simulator's uniform ADC
    noise), and there are zero tuning knobs to defend — trimmed least
    squares would need a trim fraction and an iteration rule.

    The CI is the classic nonparametric interval for the Theil-Sen slope:
    rank offsets of z*sqrt(n(n-1)(2n+5)/18) around the middle of the sorted
    pairwise slopes (Kendall's tau variance; stdlib NormalDist supplies z).
    It assumes independent observations — water's quantization residuals are
    a correlated sawtooth, so its interval is optimistically narrow. It is
    an uncertainty *proxy*, not a calibrated CI; docs/analytics.md says so
    plainly.

    Cost is O(n^2): at the 300-point cap that is 44,850 pairs, tens of
    milliseconds in CPython. The cap is enforced by the caller and guarded
    by a unit test, because raising it to 5,000 would mean 12.5 M pairs per
    device per pass.

    Pairs with equal x are skipped (the standard definition, and the reason
    a duplicate timestamp cannot divide by zero here). Returns None when
    fewer than _MIN_FIT_POINTS samples or no valid pairs exist.
    """
    n = len(samples)
    if n < _MIN_FIT_POINTS:
        return None
    anchor = samples[-1].at
    xs = [(s.at - anchor).total_seconds() / 3600.0 for s in samples]
    ys = [s.value for s in samples]

    slopes = sorted(
        (ys[j] - ys[i]) / (xs[j] - xs[i])
        for i in range(n)
        for j in range(i + 1, n)
        if xs[j] != xs[i]
    )
    m = len(slopes)
    if m == 0:
        return None
    slope = statistics.median(slopes)

    z = NormalDist().inv_cdf(0.5 + confidence / 2.0)
    spread = z * math.sqrt(n * (n - 1) * (2 * n + 5) / 18.0)
    low = min(max(math.floor((m - spread) / 2.0), 0), m - 1)
    high = min(max(math.ceil((m + spread) / 2.0), 0), m - 1)

    # Intercept and residual scale, both median-based to stay consistent
    # with the robust slope.
    intercept = statistics.median(y - slope * x for x, y in zip(xs, ys, strict=True))
    residual_mad = statistics.median(
        abs(y - (slope * x + intercept)) for x, y in zip(xs, ys, strict=True)
    )
    return Fit(
        slope_per_hour=slope,
        slope_low_per_hour=slopes[low],
        slope_high_per_hour=slopes[high],
        value_at_anchor=intercept,
        anchor_at=anchor,
        residual_mad=residual_mad,
        points=n,
        segment_points=n,  # caller overrides when the fit was capped
        span_seconds=-xs[0] * 3600.0,
        truncated=False,  # caller overrides when the segment hit the window edge
    )


def _crossing(
    value_now: float, target: float, slope_per_hour: float, anchor: datetime
) -> datetime | None:
    """When a line from (anchor, value_now) with the given slope reaches target.

    None when the slope never crosses (>= 0). Clamped at the anchor: a fitted
    value already below target crosses "now", not in the past.
    """
    if slope_per_hour >= 0.0:
        return None
    hours = max(0.0, (value_now - target) / -slope_per_hour)
    return anchor + timedelta(hours=hours)


def forecast(
    samples: Sequence[Sample],
    metric: Metric,
    now_data_time: datetime,
    *,
    window_start: datetime | None = None,
) -> Forecast:
    """The one entry point: called once per (device, metric) per pass.

    `window_start` is the cutoff `samples` were read from, and is used for
    exactly one thing: deciding whether the lookback edge — rather than a
    refill — chose where the newest segment begins (the `truncated` flag).
    Omitting it means "unknown", and a lone segment is then reported as
    truncated, the conservative answer.

    Status precedence, tested as a table:

      NO_DATA -> INVALID_SERIES -> STALE -> AT_OR_BELOW_TARGET
      -> INSUFFICIENT_POINTS -> INSUFFICIENT_SPAN -> NOT_DEPLETING
      -> BEYOND_HORIZON -> OK

    AT_OR_BELOW_TARGET precedes the fit gates on purpose: a planter pinned at
    0 % is dry *now*, and answering "not enough points" would be absurd. It
    also protects the fit from the clamped-at-zero tail, whose flat samples
    would bias the slope shallow.

    The fit always uses the most recent segment only — never a fallback to
    the one before it. A refill on the newest sample therefore yields
    INSUFFICIENT_POINTS for the next few hours of device time, which is the
    honest answer: the pre-refill depletion rate belongs to the old tank of
    water, and reporting it as current would tell a just-watered planter it
    is about to run dry.
    """

    def no_crossing(status: Status, as_of: datetime | None, latest: float | None) -> Forecast:
        return Forecast(
            kind=metric.kind,
            status=status,
            as_of=as_of,
            target=metric.target,
            latest_value=latest,
            crosses_at=None,
            crosses_at_earliest=None,
            crosses_at_latest=None,
            fit=None,
        )

    if not samples:
        return no_crossing(Status.NO_DATA, None, None)
    if not _is_valid_series(samples) or now_data_time.tzinfo is None:
        newest = samples[-1]
        as_of = newest.at if newest.at.tzinfo is not None else None
        value = newest.value if math.isfinite(newest.value) else None
        return no_crossing(Status.INVALID_SERIES, as_of, value)

    newest = samples[-1]
    cadence = cadence_seconds(samples)
    # Staleness adapts to the observed cadence for the same reason the gap
    # rule does: hardware that deep-sleeps for 45 minutes is not "dark" at
    # 30. The constant is a floor, so the demo (5 device-minutes between
    # samples, x6 = the same 1800 s) is unchanged, while a slow fleet stops
    # reporting every pod but the newest reporter as STALE — `now_data_time`
    # is the FLEET watermark, so on a slow fleet every other device is
    # legitimately one cadence behind it.
    stale_after = metric.stale_after_seconds
    if cadence is not None:
        stale_after = max(stale_after, cadence * metric.gap_factor)
    if (now_data_time - newest.at).total_seconds() > stale_after:
        return no_crossing(Status.STALE, newest.at, newest.value)
    if newest.value <= metric.target:
        return Forecast(
            kind=metric.kind,
            status=Status.AT_OR_BELOW_TARGET,
            as_of=newest.at,
            target=metric.target,
            latest_value=newest.value,
            crosses_at=newest.at,  # already crossed: the horizon is zero
            crosses_at_earliest=newest.at,
            crosses_at_latest=newest.at,
            fit=None,
        )

    segments = split_segments(samples, jump=metric.refill_jump, gap_factor=metric.gap_factor)
    segment = segments[-1]
    if len(segment) < metric.min_points:
        return no_crossing(Status.INSUFFICIENT_POINTS, newest.at, newest.value)
    fit_samples = segment[-metric.max_fit_points :]
    span_seconds = (fit_samples[-1].at - fit_samples[0].at).total_seconds()
    if span_seconds < metric.min_span_seconds:
        return no_crossing(Status.INSUFFICIENT_SPAN, newest.at, newest.value)

    fit = theil_sen(fit_samples)
    if fit is None:  # unreachable past the gates above; stay total anyway
        return no_crossing(Status.INSUFFICIENT_POINTS, newest.at, newest.value)
    # A lone segment starts at the window's first sample — but only when that
    # sample sits AT the window edge did the edge, rather than the device's
    # own history, decide where the segment begins. A device younger than the
    # lookback (every device in a fresh deployment, and every battery series
    # until the demo is 35 days old) has one segment whose start is its
    # first-ever reading; calling that truncated claims we know less than we
    # do. "At the edge" is one gap-limit of slack, the same threshold the
    # segmenter uses: a larger hole than that would have split the segment
    # anyway had the older data been there.
    edge_slack = cadence * metric.gap_factor if cadence is not None else 0.0
    reached_edge = (
        window_start is None or (segment[0].at - window_start).total_seconds() <= edge_slack
    )
    fit = replace(fit, segment_points=len(segment), truncated=len(segments) == 1 and reached_edge)

    crosses_at = _crossing(fit.value_at_anchor, metric.target, fit.slope_per_hour, fit.anchor_at)
    if crosses_at is None:
        return Forecast(
            kind=metric.kind,
            status=Status.NOT_DEPLETING,
            as_of=newest.at,
            target=metric.target,
            latest_value=newest.value,
            crosses_at=None,
            crosses_at_earliest=None,
            crosses_at_latest=None,
            fit=fit,
        )

    status = Status.OK
    if (crosses_at - fit.anchor_at).total_seconds() > metric.max_horizon_seconds:
        status = Status.BEYOND_HORIZON
    return Forecast(
        kind=metric.kind,
        status=status,
        as_of=newest.at,
        target=metric.target,
        latest_value=newest.value,
        # slope_low is the steepest plausible depletion => earliest crossing;
        # slope_high may not cross at all => an honest open-ended band.
        crosses_at=crosses_at,
        crosses_at_earliest=_crossing(
            fit.value_at_anchor, metric.target, fit.slope_low_per_hour, fit.anchor_at
        ),
        crosses_at_latest=_crossing(
            fit.value_at_anchor, metric.target, fit.slope_high_per_hour, fit.anchor_at
        ),
        fit=fit,
    )


AlertTransition = Literal["firing", "cleared"]


@dataclass(frozen=True)
class AlertState:
    """The latest recorded alert decision for one (device, kind)."""

    state: AlertTransition
    as_of: datetime


def alert_transition(
    fc: Forecast,
    metric: Metric,
    previous: AlertState | None,
    *,
    cooldown_seconds: float,
    clear_factor: float = 1.5,
) -> AlertTransition | None:
    """Decide whether this forecast changes the alert state. Pure.

    Fires on the *transition* into the alerting condition (horizon at or
    below the metric's alert horizon), not on the condition itself — at the
    simulator's accelerated clock a condition-based rule would repeat the
    same notification every few wall minutes for as long as a planter stayed
    dry. The cooldown is a backstop reminder for a standing condition, not
    the primary mechanism.

    Clears with hysteresis (horizon must recover past clear_factor x the
    alert horizon) so a forecast wobbling around the threshold cannot flap
    fire/clear pairs. Statuses without a crossing (STALE, INSUFFICIENT_*,
    NOT_DEPLETING, ...) change nothing in either direction: they carry no
    evidence the condition ended — a refill announces itself soon enough as
    a fresh segment with a long horizon, and *that* clears.

    `previous` is the latest row from alert_events, never process memory, so
    a service restart cannot re-fire an already-firing alert.
    """
    if fc.crosses_at is None or fc.as_of is None:
        return None
    horizon_seconds = max((fc.crosses_at - fc.as_of).total_seconds(), 0.0)
    if horizon_seconds <= metric.alert_horizon_seconds:
        if previous is None or previous.state == "cleared":
            return "firing"
        if (fc.as_of - previous.as_of).total_seconds() >= cooldown_seconds:
            return "firing"  # standing condition: periodic reminder
        return None
    if (
        previous is not None
        and previous.state == "firing"
        and horizon_seconds > metric.alert_horizon_seconds * clear_factor
    ):
        return "cleared"
    return None
