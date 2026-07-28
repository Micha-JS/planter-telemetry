"""Shared ops surface: the aiohttp /healthz + /metrics server both long-running
services (ingestion, analytics) sit behind.

What is shared is deliberately mechanical — the HTTP plumbing, the
counters-as-metrics live view, the registry construction. What health *means*
stays with each service: the caller supplies a zero-argument payload callable
(status code + JSON body), because ingestion's health is "connected to broker
and database" while analytics' is "the last pass succeeded recently", and
flattening those into one shape would blur exactly the distinction that makes
each healthcheck prove its dependency rather than its process.

Metrics follow the same split: every field of a service's Counters dataclass
is exposed as <prefix><field>_total via a live view over the single Counters
instance (never a second bookkeeping system), and a service with
non-monotonic observations (gauges) registers its own extra collector.
"""

from collections.abc import Callable, Iterable
from dataclasses import fields
from typing import TYPE_CHECKING

from aiohttp import hdrs, web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_client.core import CollectorRegistry, CounterMetricFamily, Metric
from prometheus_client.platform_collector import PlatformCollector
from prometheus_client.process_collector import ProcessCollector
from prometheus_client.registry import Collector

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

# A service's health, rendered: HTTP status code plus JSON body.
HealthzPayload = Callable[[], tuple[int, dict[str, object]]]


class CountersCollector(Collector):
    """Expose every field of a Counters dataclass as <prefix><field>_total."""

    def __init__(
        self,
        counters: "DataclassInstance",
        prefix: str,
        help_format: str = "Total {} since service start.",
    ) -> None:
        self._counters = counters
        self._prefix = prefix
        self._help_format = help_format

    def collect(self) -> Iterable[Metric]:
        for field in fields(self._counters):
            yield CounterMetricFamily(
                f"{self._prefix}{field.name}",
                self._help_format.format(field.name.replace("_", " ")),
                value=getattr(self._counters, field.name),
            )


def build_registry(
    counters: "DataclassInstance",
    prefix: str,
    help_format: str = "Total {} since service start.",
    extra_collectors: Iterable[Collector] = (),
) -> CollectorRegistry:
    """Fresh registry per server (never the process-global REGISTRY, so tests
    can build as many as they like): the counters view, any service-specific
    collectors (gauges), plus process and platform basics."""
    registry = CollectorRegistry()
    registry.register(CountersCollector(counters, prefix, help_format))
    for collector in extra_collectors:
        registry.register(collector)
    ProcessCollector(registry=registry)
    PlatformCollector(registry=registry)
    return registry


_HEALTHZ_KEY: web.AppKey[HealthzPayload] = web.AppKey("healthz")
_REGISTRY_KEY: web.AppKey[CollectorRegistry] = web.AppKey("registry", CollectorRegistry)


async def _healthz(request: web.Request) -> web.Response:
    status, body = request.app[_HEALTHZ_KEY]()
    return web.json_response(body, status=status)


async def _metrics(request: web.Request) -> web.Response:
    body = generate_latest(request.app[_REGISTRY_KEY])
    # CONTENT_TYPE_LATEST carries a charset, which aiohttp's content_type=
    # argument rejects; set the header verbatim instead.
    return web.Response(body=body, headers={hdrs.CONTENT_TYPE: CONTENT_TYPE_LATEST})


async def start_ops_server(
    host: str, port: int, healthz: HealthzPayload, registry: CollectorRegistry
) -> tuple[web.AppRunner, int]:
    """Serve /healthz and /metrics; returns the runner (for cleanup) and the
    bound port (meaningful when port=0 requested an ephemeral one)."""
    app = web.Application()
    app[_HEALTHZ_KEY] = healthz
    app[_REGISTRY_KEY] = registry
    app.router.add_get("/healthz", _healthz)
    app.router.add_get("/metrics", _metrics)
    # access_log=None: the compose healthcheck probes every 5 s, and access
    # lines through the JSON formatter would flood stderr with non-constant
    # "event" values — breaking the event-name-plus-extra log convention.
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    bound_port = int(runner.addresses[0][1])
    return runner, bound_port
