"""Health and metrics for the analytics service, over the shared ops server.

GET /healthz — 200 when the most recent pass succeeded AND completed
recently enough; 503 otherwise. House rule: a healthcheck proves the
dependency, not the process — a service whose passes throw on every
iteration while its event loop spins happily is *unhealthy*. "Recently
enough" is measured against the configured interval with slack for one
missed cycle, on the monotonic clock (the only wall-adjacent clock in this
package; forecast arithmetic never touches it).

GET /metrics — planter_analytics_<counter>_total for the monotonic counters,
plus explicit gauges for the two observations that are NOT counters
(last-pass duration, devices forecast this pass). Pushing a gauge through
CounterMetricFamily would be a wrong metric type; the split is the point.
"""

import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass, fields
from typing import TYPE_CHECKING

from aiohttp import web
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from planter_telemetry import ops

if TYPE_CHECKING:
    from planter_telemetry.analytics.service import Counters, Gauges

_METRICS_PREFIX = "planter_analytics_"
# One skipped cycle plus scheduling slack before /healthz calls a pass overdue.
_OVERDUE_FACTOR = 2.0
_OVERDUE_GRACE_SECONDS = 30.0


@dataclass
class HealthState:
    """Pass outcome flags owned and updated by service.run().

    ops_port is filled in once the HTTP server is bound — with an ephemeral
    port (ops_port=0 in settings) it is the only way tests learn the port.
    last_pass_monotonic is time.monotonic() at the end of the last attempt,
    successful or not; staleness against it catches a wedged loop.
    """

    last_pass_ok: bool = False
    last_pass_monotonic: float | None = None
    ops_port: int | None = None


def healthz_payload(
    health: HealthState,
    counters: "Counters",
    interval_seconds: float,
    now_monotonic: float,
) -> tuple[int, dict[str, object]]:
    """Status code and body for /healthz. Pure — the caller supplies the
    monotonic now, so the truth table is testable without a clock."""
    seconds_since: float | None = None
    if health.last_pass_monotonic is not None:
        seconds_since = now_monotonic - health.last_pass_monotonic
    overdue_after = interval_seconds * _OVERDUE_FACTOR + _OVERDUE_GRACE_SECONDS
    healthy = health.last_pass_ok and seconds_since is not None and seconds_since <= overdue_after
    body: dict[str, object] = {
        "status": "healthy" if healthy else "unhealthy",
        "last_pass_ok": health.last_pass_ok,
        "seconds_since_last_pass": seconds_since,
        "counters": asdict(counters),
    }
    return (200 if healthy else 503), body


class GaugesCollector(Collector):
    """Expose every Gauges field as planter_analytics_<field> (no _total:
    these go up and down)."""

    def __init__(self, gauges: "Gauges") -> None:
        self._gauges = gauges

    def collect(self) -> Iterable[Metric]:
        for field in fields(self._gauges):
            yield GaugeMetricFamily(
                f"{_METRICS_PREFIX}{field.name}",
                f"{field.name.replace('_', ' ').capitalize()}, as of the last pass.",
                value=getattr(self._gauges, field.name),
            )


async def start_ops_server(
    host: str,
    port: int,
    health: HealthState,
    counters: "Counters",
    gauges: "Gauges",
    interval_seconds: float,
) -> tuple[web.AppRunner, int]:
    """Serve /healthz and /metrics; returns the runner (for cleanup) and the
    bound port (meaningful when port=0 requested an ephemeral one)."""
    registry = ops.build_registry(
        counters, _METRICS_PREFIX, extra_collectors=(GaugesCollector(gauges),)
    )
    return await ops.start_ops_server(
        host,
        port,
        lambda: healthz_payload(health, counters, interval_seconds, time.monotonic()),
        registry,
    )
