"""Health and metrics for the ingestion service, over the shared ops server.

GET /healthz — 200 when the service is subscribed to the broker AND writing
to the database, 503 otherwise. There is deliberately no "degraded" state:
the service has one reconnect path that tears down and rebuilds both
connections together (see service.run), so broker and database health cannot
diverge for longer than a reconnect cycle. 503 therefore covers both "still
starting" (absorbed by the compose healthcheck's start_period) and
"reconnecting"; the body reports both flags for diagnosis.

GET /metrics — Prometheus text format, a live view over the service's single
Counters instance (planter_ingestion_<field>_total). The HTTP plumbing and
the counters-as-metrics machinery live in planter_telemetry.ops; this module
supplies only what health *means* here.
"""

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from aiohttp import web
from prometheus_client.core import CollectorRegistry

from planter_telemetry import ops

if TYPE_CHECKING:
    from planter_telemetry.ingestion.service import Counters

_METRICS_PREFIX = "planter_ingestion_"
_METRICS_HELP_FORMAT = "Messages {} since service start."


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


def build_registry(counters: "Counters") -> CollectorRegistry:
    """The shared registry with this service's metric prefix."""
    return ops.build_registry(counters, _METRICS_PREFIX, _METRICS_HELP_FORMAT)


async def start_ops_server(
    host: str, port: int, health: HealthState, counters: "Counters"
) -> tuple[web.AppRunner, int]:
    """Serve /healthz and /metrics; returns the runner (for cleanup) and the
    bound port (meaningful when port=0 requested an ephemeral one)."""
    return await ops.start_ops_server(
        host, port, lambda: healthz_payload(health, counters), build_registry(counters)
    )
