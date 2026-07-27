"""Session-scoped docker containers for the integration suite.

Only integration-marked tests request these fixtures, so a plain `pytest`
run (unit mode, the default) never touches docker. The generic
DockerContainer is used for both services — no testcontainers extras, and
readiness is checked the same way the services themselves would.
"""

import time
from collections.abc import Iterator

import psycopg
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from planter_telemetry import migrate

# Keep in sync with docker-compose.yml.
TIMESCALE_IMAGE = "timescale/timescaledb:2.28.3-pg17"
MOSQUITTO_IMAGE = "eclipse-mosquitto:2"


@pytest.fixture(scope="session")
def mqtt_endpoint() -> Iterator[tuple[str, int]]:
    container = (
        DockerContainer(MOSQUITTO_IMAGE)
        # The image bundles this config (listener 1883, anonymous access);
        # without a config mosquitto 2 binds loopback-only inside the
        # container and the mapped port would refuse connections.
        .with_command("mosquitto -c /mosquitto-no-auth.conf")
        .with_exposed_ports(1883)
    )
    with container:
        wait_for_logs(container, r"mosquitto version .* running", timeout=30)
        yield container.get_container_host_ip(), int(container.get_exposed_port(1883))


def _wait_for_db(dsn: str, timeout: float = 60.0) -> None:
    # A successful TCP connection implies the real server: the temporary
    # initdb-phase server listens on the container's unix socket only.
    deadline = time.monotonic() + timeout
    while True:
        try:
            psycopg.connect(dsn).close()
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.5)
        else:
            return


@pytest.fixture(scope="session")
def db_dsn() -> Iterator[str]:
    container = (
        DockerContainer(TIMESCALE_IMAGE)
        .with_env("POSTGRES_USER", "planter")
        .with_env("POSTGRES_PASSWORD", "planter")
        .with_env("POSTGRES_DB", "planter")
        .with_exposed_ports(5432)
    )
    with container:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(5432))
        dsn = f"postgresql://planter:planter@{host}:{port}/planter"
        _wait_for_db(dsn)
        # The same code path the compose `migrate` service runs: every
        # integration test therefore runs against the migrated schema.
        migrate.upgrade(dsn)
        yield dsn


@pytest.fixture
def clean_db(db_dsn: str) -> str:
    """Per-test isolation on the shared session container."""
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        conn.execute("TRUNCATE telemetry, dead_letter, devices RESTART IDENTITY")
        # The continuous aggregates are truncated explicitly: whether raw-
        # hypertable TRUNCATE writes cagg invalidations is undocumented, and
        # cagg TRUNCATE also resets the real-time watermark (fixed in 2.15) —
        # without that, stale materialized state could leak between tests.
        conn.execute("TRUNCATE telemetry_hourly")
        conn.execute("TRUNCATE telemetry_daily")
    return db_dsn
