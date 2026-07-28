# Analytics: the refill forecast

The M7 headline: the dashboard tells you *when* to water, not that you
should have. A pure forecasting core
([`analytics/model.py`](../src/planter_telemetry/analytics/model.py)) turns
each device's recent telemetry into a dated crossing — "empty in ~4 days" —
or an explicit reason why no honest forecast exists. A small service wraps
it: read window, fit, write forecast, decide alert, sleep. That split is the
whole reason the accuracy tests can exist.

## The model, in one paragraph

Split the series into depletion segments at refill events (upward jumps),
fit a robust line to the most recent segment, extrapolate to the target
level, report the crossing time with a confidence band — or refuse, with a
reason. Linear-per-segment, deliberately: everything interesting in this
milestone is in the honesty rules and the verification, not in the
estimator. A better model slots into one function (`theil_sen`) without
touching segmentation, gating, the service, or the tests' structure.

## Segmentation: value jumps first, time gaps second

A segment breaks where the value steps *up* by at least `refill_jump`
(water 5 pct, battery 0.05 V) between consecutive samples. Against the
simulator the value rule is sufficient by construction: refills jump from
below 25 % to 92–100 % (a step of at least 67 pct), and water depletion
carries no additive noise, so any upward step at all is a refill. The 5 pct
threshold is a real-sensor margin — capacitive soil probes jitter — not a
simulator-derived number. Battery: worst-case spurious upward step from ADC
noise is ~0.011 V; 0.05 sits 4.5× above the noise and 23× below a real
recharge (≥ 1.15 V).

A segment also breaks on a time gap longer than 6× the observed median
inter-sample interval. This rule is unreachable against the simulator
(missed check-ins are independent at 3 %; six consecutive is ~7×10⁻¹⁰ per
wake) and exists for the hardware case a value rule cannot see: a device
offline for a day that comes back refilled *and* partly depleted — no
visible jump, two regimes in one apparent segment. It is unit-tested on
synthetic series, because the demo will never exercise it.

## The estimator: Theil–Sen, for an unusual reason

The fit is the median of pairwise slopes over the most recent ≤ 300 samples
of the newest segment. Not for textbook outlier robustness — stored
telemetry has no outliers (duplicates die on the primary key, malformed
payloads never leave the dead-letter table) — but because **the estimator
should not amplify a bug in the code above it**. If a refill ever slips
past segmentation, or a real sensor glitches, a single 67-pct step inside a
300-point window destroys a least-squares slope; the median of pairwise
slopes tolerates ~29 % contamination and still answers correctly. The price
is ≤ 5 % efficiency (asymptotic relative efficiency 0.955 under Gaussian
noise, ~1.0 under the simulator's uniform ADC noise) and zero tuning knobs —
trimmed least squares would need a trim fraction and an iteration rule, each
a number to defend.

## Uncertainty: a crossing band, not a score

The confidence signal is the classic distribution-free interval on the
Theil–Sen slope (rank offsets of `z·√(n(n−1)(2n+5)/18)` into the sorted
pairwise slopes — stdlib `NormalDist`, two index lookups), propagated
through the crossing arithmetic to `crosses_at_earliest` / `crosses_at_latest`:
"dry between Tuesday and Thursday". It is unit-free, so one rule serves
both metrics without per-kind thresholds.

Honest caveat, stated here rather than hidden: the interval assumes
independent observations. Water's residuals are a correlated quantization
sawtooth (the only "noise" on water is `round(level, 1)`), so its band is
optimistically narrow — it is an uncertainty *proxy*, not a calibrated CI.
The tests therefore assert band *ordering*, never truth-bracketing, for
water. Battery's ±5 mV ADC noise is much closer to the assumption.

## Honesty rules

Every forecast carries a status; only three statuses carry a crossing time
(an invariant the tests assert). Precedence is fixed and tested:

| status | meaning | crossing? |
|---|---|---|
| `no_data` | no samples in the lookback window | no |
| `invalid_series` | unsorted / duplicate timestamps / non-finite — the core never trusts its input | no |
| `stale` | newest sample too old vs the fleet's data-time "now" (30 min floor, raised to 6× the device's own observed cadence) | no |
| `at_or_below_target` | already at/below target — dry NOW; checked before any fit gate | yes (horizon 0) |
| `insufficient_points` | newest segment shorter than 36 (water) / 144 (battery) samples | no |
| `insufficient_span` | fitted span under 3 h (water) / 12 h (battery) — dense replay data defeats a point count alone | no |
| `not_depleting` | fitted slope ≥ 0; on water this is impossible in-segment and logged as a segmenter-bug signal | no |
| `beyond_horizon` | real crossing, further out than the display horizon — a good battery, not a failure | yes |
| `ok` | | yes |

Two rules matter more than the rest:

- **A refill on the newest samples answers `insufficient_points`** — never a
  fall-back to the pre-refill segment, which would tell a just-watered
  planter it is about to run dry. The attention panel shows "warming up"
  for the next 3 device-hours (one wall-minute at the demo's 180×); that is
  correct behaviour, not a gap.
- **All time is data time.** The core takes an explicit `now_data_time`
  (the fleet-wide `max(measured_at)`) and nothing below the service reads a
  clock. Under the accelerated demo clock "days until empty" means days of
  *device* time; on real hardware data time ≈ wall time and the discipline
  costs nothing.

## Why raw telemetry, not the M3 aggregates

Hourly averaging smears a refill step across its bucket: the bucket
containing the refill averages the two regimes and leaves *no step to
detect* — the aggregate destroys exactly the signal segmentation depends
on. The fit therefore reads raw `telemetry` (one query per pass, grouped in
Python by device *and metric kind*, so a metric is never paired with a
column by position), which at demo volume costs nothing. One read covers
both metrics at the longest lookback and each metric then sees exactly its
own window — water's 10 days is a real bound, not a decorative constant.

A pass whose fleet watermark has not advanced since the previous one skips
straight out after a single `max(measured_at)`: the model is a pure
function of the window, so unchanged data cannot change a forecast. On real
hardware — a 5-minute check-in cadence against a 30-second pass interval —
that is most passes, and the alternative is re-running an O(n²) fit per
device to write nothing.

## Accuracy, derived rather than claimed

The simulator's depletion is exactly linear with known configured rates, so
the tests compare against analytic ground truth and assert *derived*
bounds ([`tests/test_analytics_model.py`](../tests/test_analytics_model.py)
carries the arithmetic in each docstring):

- **Water** — reported values are `round(true, 1)` on a noiseless ramp, so
  every point sits within 0.05 pct of the line and no fit consistent with
  the points can be more than `0.1/T` off in slope over a span of T hours
  (≈ 16σ of the Theil–Sen spread at T = 24 h, n = 288 — flake-proof for any
  seed). Crossing time asserted within **1 %** of the true horizon.
- **Battery** — ADC noise σ ≈ 2.9 mV against a per-sample decay of
  ~0.2 mV: the per-sample SNR is ~0.07, which is why battery needs the 12 h
  span gate where water needs 3 h. Slope asserted within 1×10⁻⁴ V/h (≈ 4σ,
  parametrized over five seeds); crossing within **8 %**.

Battery forecasts are structurally ~10× less precise than water forecasts —
that is what the confidence band is for, and saying it is worth more than
hiding it.

The integration suite closes the loop end-to-end: seeded devices with known
rates through the real database land within the same tolerances; a second
analytics pass over unchanged data leaves `forecasts` and `alert_events`
byte-identical (the M2 replay invariant, extended); and the horizon of
consecutive forecasts shrinks in lockstep with device time — one day per
day, the cheapest possible proof that the forecast is self-consistent.

## Alerting: decisions in a table, delivery as a side effect

The done-criterion is "an alert fires", so the *decision* is the durable,
testable artifact: a row in `alert_events`, written before any delivery is
attempted. Delivery is an opt-in POST to [ntfy](https://ntfy.sh)
(`ANALYTICS_NTFY_URL`, empty by default — a fresh clone never posts to
anyone's topic). The URL is a write capability and never appears in logs,
tables, or error strings.

Alerts fire on the **transition** into the alerting condition (horizon at
or below `ANALYTICS_WATER_ALERT_DAYS` / `ANALYTICS_BATTERY_ALERT_DAYS`),
clear with hysteresis at 1.5× the threshold, and repeat only after a
data-time cooldown (`ANALYTICS_ALERT_COOLDOWN_HOURS` default 24) as a
standing reminder. The previous state is read from `alert_events` itself — never
process memory — so a service restart cannot re-fire a standing alert. At
the demo's 180× clock, a naive condition-based rule would push the same
notification every few wall-minutes for as long as a planter stayed dry;
the transition rule is what makes the accelerated demo livable.

Why a Python notifier instead of Grafana alerting: provisioned Grafana
alert rules are a large JSON surface with no test story, they would need a
contact point provisioned anyway, and the repeat-suppression logic would
live in YAML. Here the decision logic is ~30 pure lines with a unit-test
truth table, and its output is a queryable table the dashboard can show.

## What the simplicity hides (deliberately)

- **Real LiPo discharge is not linear** — it plateaus and then falls off a
  knee. The linear fit is exact *here* only because the simulator makes it
  so; on real hardware the battery forecast degrades gracefully (the recent
  window tracks the local slope) but a production version wants a
  voltage-to-state-of-charge lookup before the fit.
- **Real plant uptake varies** with temperature, light, and growth; the
  most-recent-segment fit adapts within days but knows no seasons.
- **A late out-of-order reading** older than a device's forecast watermark
  does not retrigger a fit; the marginally better forecast it would enable
  is dropped, keeping the idempotency key honest.
- **A device that goes dark keeps its watermark**, so its `stale` forecast
  shares a key with the last healthy one. The write is therefore an update
  gated on the status changing — without it the dashboard would show a
  frozen "ok" and an unchanging horizon for a planter that stopped
  reporting days ago. Unchanged data still writes nothing.
- **Staleness is a fleet-relative rule with an adaptive limit.** "Now" is
  the newest `measured_at` in the fleet, so on hardware that deep-sleeps
  for an hour every pod except the most recent reporter is legitimately a
  cadence behind it; a fixed 30-minute rule would report a healthy fleet as
  dark and never forecast it. The constant is a floor, raised to
  `gap_factor ×` the device's observed cadence — the same threshold the
  segmenter uses to call a hole implausible.
- **Anomaly detection is out of scope** — leak and wick-failure baselines
  are the documented future work (see the plan), and nothing here blocks
  them: they would be new consumers of the same segments.

## Proof

CI-verified (unit + testcontainers integration + compose smoke):

- segment detection recovers the simulator's refill points exactly and
  never splits on quantization jitter; the gap rule fires on synthetic gaps;
- fitted slopes and crossing times match configured device rates within the
  derived tolerances above, through the pure core and end-to-end through
  TimescaleDB;
- every no-forecast status appears under its documented condition, in the
  documented precedence, and `crosses_at` exists exactly for the three
  statuses that promise it;
- a refill mid-window resets the forecast through `insufficient_points` to
  the new segment's rate — never a negative or absurd rate, never the old
  tank's rate;
- re-running an analytics pass over the same data window changes neither
  `forecasts` nor `alert_events` (count and content), and a pass whose
  watermark has not moved does no work at all (proven by deleting the rows
  first: a pass that re-fitted would write them back);
- a device that stops reporting loses its forecast — the status flips to
  `stale` at the watermark it already occupies, and the view reports no
  horizon rather than a frozen one;
- an alert decision fires when a seeded fast-depleting device's horizon
  crosses the threshold, refuses to re-fire across a restart-equivalent
  pass, and clears after a refill (`make analytics-smoke` proves the same
  live: with the default seed, planter-00's ~1.9 pct/h depletion guarantees
  a firing row);
- an empty database is a successful zero-forecast pass, and /healthz
  reflects pass success and staleness, not process liveness;
- `grafana_reader` executes every panel query in the dashboard JSON —
  grants, views, and columns proven, not assumed.
