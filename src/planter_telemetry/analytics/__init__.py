"""Analytics service: per-device depletion forecasts over TimescaleDB.

Layered like ingestion so the interesting parts are pure and unit-testable
without a database:

- model: the forecasting core — samples in, a dated crossing (or an honest
  status) out; plus the alert transition rule
- db: the database edge — the telemetry window read, idempotent forecast and
  alert-event writes
- notify: the ntfy delivery edge (opt-in)
- service: the impure wiring — the interval loop, per-pass connections,
  shutdown
- ops: health/metrics over the shared ops server

Every timestamp the model touches is DATA time (measured_at): under the
simulator's accelerated clock "days until empty" means days of device time,
and nothing below the service layer ever calls datetime.now(). The model and
its accuracy tolerances are documented in docs/analytics.md.
"""
