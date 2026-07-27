"""Health and metrics endpoints for the ingestion service.

GET /healthz — 200 when the service is subscribed to the broker AND writing
to the database, 503 otherwise. There is deliberately no "degraded" state:
the service has one reconnect path that tears down and rebuilds both
connections together (see service.run), so broker and database health cannot
diverge for longer than a reconnect cycle. 503 therefore covers both "still
starting" (absorbed by the compose healthcheck's start_period) and
"reconnecting"; the body reports both flags for diagnosis.

GET /metrics — Prometheus text format. The metrics are a live view over the
service's single Counters instance, not a second bookkeeping system: a
custom collector reads the dataclass at scrape time. Wiring an actual
Prometheus server is future work; the endpoint is scrape-ready.

The pure parts (payload/registry construction) sit on top; the aiohttp edge
is at the bottom.
"""

from collections.abc import Iterable
from dataclasses import asdict, dataclass, fields
from typing import TYPE_CHECKING

from aiohttp import hdrs, web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_client.core import CollectorRegistry, CounterMetricFamily, Metric
from prometheus_client.platform_collector import PlatformCollector
from prometheus_client.process_collector import ProcessCollector
from prometheus_client.registry import Collector

if TYPE_CHECKING:
    from planter_telemetry.ingestion.service import Counters


@dataclass
class HealthState:
    """Connection flags owned and updated by service.run().

    ops_port is filled in once the HTTP server is bound — with an ephemeral
    port (ops_port=0 in settings) it is the only way tests learn the port.
    """

    broker_connected: bool = False
    db_connected: bool = False
    ops_port: int | None = None


def healthz_payload(health: HealthState, counters: "Counters") -> tuple[int, dict[str, object]]:
    """Status code and body for /healthz. Pure."""
    healthy = health.broker_connected and health.db_connected
    body: dict[str, object] = {
        "status": "healthy" if healthy else "unhealthy",
        "broker_connected": health.broker_connected,
        "db_connected": health.db_connected,
        "counters": asdict(counters),
    }
    return (200 if healthy else 503), body


class CountersCollector(Collector):
    """Expose every Counters field as planter_ingestion_<field>_total."""

    def __init__(self, counters: "Counters") -> None:
        self._counters = counters

    def collect(self) -> Iterable[Metric]:
        for field in fields(self._counters):
            yield CounterMetricFamily(
                f"planter_ingestion_{field.name}",
                f"Messages {field.name.replace('_', ' ')} since service start.",
                value=getattr(self._counters, field.name),
            )


def build_registry(counters: "Counters") -> CollectorRegistry:
    """Fresh registry per server (never the process-global REGISTRY, so
    tests can build as many as they like): the counters view plus process
    and platform basics."""
    registry = CollectorRegistry()
    registry.register(CountersCollector(counters))
    ProcessCollector(registry=registry)
    PlatformCollector(registry=registry)
    return registry


_HEALTH_KEY: web.AppKey[HealthState] = web.AppKey("health", HealthState)
_COUNTERS_KEY: "web.AppKey[Counters]" = web.AppKey("counters")
_REGISTRY_KEY: web.AppKey[CollectorRegistry] = web.AppKey("registry", CollectorRegistry)


async def _healthz(request: web.Request) -> web.Response:
    status, body = healthz_payload(request.app[_HEALTH_KEY], request.app[_COUNTERS_KEY])
    return web.json_response(body, status=status)


async def _metrics(request: web.Request) -> web.Response:
    body = generate_latest(request.app[_REGISTRY_KEY])
    # CONTENT_TYPE_LATEST carries a charset, which aiohttp's content_type=
    # argument rejects; set the header verbatim instead.
    return web.Response(body=body, headers={hdrs.CONTENT_TYPE: CONTENT_TYPE_LATEST})


async def start_ops_server(
    host: str, port: int, health: HealthState, counters: "Counters"
) -> tuple[web.AppRunner, int]:
    """Serve /healthz and /metrics; returns the runner (for cleanup) and the
    bound port (meaningful when port=0 requested an ephemeral one)."""
    app = web.Application()
    app[_HEALTH_KEY] = health
    app[_COUNTERS_KEY] = counters
    app[_REGISTRY_KEY] = build_registry(counters)
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
