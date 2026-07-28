"""Analytics ops: /healthz truth table, counters + gauges metric split."""

from prometheus_client import generate_latest

from planter_telemetry.analytics.ops import GaugesCollector, HealthState, healthz_payload
from planter_telemetry.analytics.service import Counters, Gauges
from planter_telemetry.ops import build_registry

INTERVAL = 30.0


def test_healthy_after_recent_successful_pass() -> None:
    health = HealthState(last_pass_ok=True, last_pass_monotonic=1000.0)
    status, body = healthz_payload(health, Counters(), INTERVAL, now_monotonic=1010.0)
    assert status == 200
    assert body["status"] == "healthy"
    assert body["seconds_since_last_pass"] == 10.0


def test_unhealthy_before_first_pass() -> None:
    status, body = healthz_payload(HealthState(), Counters(), INTERVAL, now_monotonic=0.0)
    assert status == 503
    assert body["seconds_since_last_pass"] is None


def test_unhealthy_after_failed_pass() -> None:
    """A spinning loop whose passes all throw must read unhealthy — the
    healthcheck proves the dependency, not the process."""
    health = HealthState(last_pass_ok=False, last_pass_monotonic=1000.0)
    status, _ = healthz_payload(health, Counters(), INTERVAL, now_monotonic=1010.0)
    assert status == 503


def test_unhealthy_when_pass_overdue() -> None:
    """A wedged loop stops updating last_pass_monotonic; staleness past
    2x interval + 30 s grace flips to 503 even though the last pass was ok."""
    health = HealthState(last_pass_ok=True, last_pass_monotonic=1000.0)
    ok_status, _ = healthz_payload(health, Counters(), INTERVAL, now_monotonic=1000.0 + 89.0)
    assert ok_status == 200
    stale_status, _ = healthz_payload(health, Counters(), INTERVAL, now_monotonic=1000.0 + 91.0)
    assert stale_status == 503


def test_healthz_carries_counters() -> None:
    counters = Counters(passes=5, forecasts_written=40, alerts_fired=2)
    _, body = healthz_payload(HealthState(), counters, INTERVAL, now_monotonic=0.0)
    assert body["counters"] == {
        "passes": 5,
        "pass_failures": 0,
        "forecasts_written": 40,
        "alerts_fired": 2,
        "alerts_cleared": 0,
        "notify_failures": 0,
    }


def test_metrics_split_counters_from_gauges() -> None:
    """Counters expose as _total; gauges expose without the suffix — pushing
    a gauge through a counter family would be a wrong metric type."""
    counters = Counters()
    gauges = Gauges()
    registry = build_registry(
        counters, "planter_analytics_", extra_collectors=(GaugesCollector(gauges),)
    )
    counters.forecasts_written = 8
    gauges.devices_forecast = 4
    gauges.last_pass_duration_seconds = 0.25
    text = generate_latest(registry).decode()
    assert "planter_analytics_forecasts_written_total 8.0" in text
    assert "planter_analytics_devices_forecast 4.0" in text
    assert "planter_analytics_last_pass_duration_seconds 0.25" in text
    assert "planter_analytics_devices_forecast_total" not in text
    assert "python_info" in text  # platform basics ride along
