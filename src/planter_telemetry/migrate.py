"""Bring the database to the current schema: python -m planter_telemetry.migrate

Runs `alembic upgrade head` programmatically. Databases created by the M2 init
script predate Alembic and already contain migration 0001's tables, so those
are detected and stamped at the baseline revision first — migration 0001 is an
exact reproduction of the M2 schema, which is what makes the stamp safe.
"""

import logging
import time
from importlib.resources import files

import psycopg
from alembic import command
from alembic.config import Config
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import TupleRow
from sqlalchemy.engine import URL

from planter_telemetry.ingestion.config import IngestSettings
from planter_telemetry.jsonlog import configure

logger = logging.getLogger(__name__)

BASELINE_REVISION = "0001"


def sqlalchemy_url(dsn: str) -> str:
    """Rewrite a libpq DSN for SQLAlchemy: plain postgresql:// would select
    the absent psycopg2 driver instead of psycopg 3.

    Parses through psycopg's own conninfo machinery so every DSN form that
    `psycopg.connect` accepts elsewhere in this codebase (postgresql://,
    postgres://, key=value) works here too, and a malformed DSN fails with
    libpq's parse error rather than an opaque SQLAlchemy one.
    """
    if dsn.startswith("postgresql+psycopg://"):
        return dsn
    params = {key: str(value) for key, value in conninfo_to_dict(dsn).items()}
    port = params.pop("port", None)
    url = URL.create(
        "postgresql+psycopg",
        username=params.pop("user", None),
        password=params.pop("password", None),
        host=params.pop("host", None),
        port=int(port) if port else None,
        database=params.pop("dbname", None),
        # Everything else (sslmode, connect_timeout, ...) rides along as
        # query parameters, which the psycopg dialect hands back to libpq.
        query=params,
    )
    return url.render_as_string(hide_password=False)


def build_config(dsn: str) -> Config:
    """Alembic config independent of any ini file or working directory.

    The script location is resolved through the installed package, so this
    works identically for an editable checkout, the Docker image, and the
    test suite.
    """
    config = Config()
    config.set_main_option("script_location", str(files("planter_telemetry") / "migrations"))
    config.set_main_option("sqlalchemy.url", sqlalchemy_url(dsn))
    return config


def _is_pre_alembic_m2(conn: psycopg.Connection[TupleRow]) -> bool:
    row = conn.execute(
        "SELECT to_regclass('public.telemetry') IS NOT NULL"
        " AND to_regclass('public.alembic_version') IS NULL"
    ).fetchone()
    return row is not None and bool(row[0])


def upgrade(dsn: str) -> None:
    """Migrate to head, stamping a pre-Alembic M2 database at 0001 first."""
    config = build_config(dsn)
    with psycopg.connect(dsn) as conn:
        if _is_pre_alembic_m2(conn):
            # Event-name-plus-extra convention: see jsonlog module docstring.
            logger.info("stamping_baseline", extra={"revision": BASELINE_REVISION})
            command.stamp(config, BASELINE_REVISION)
    command.upgrade(config, "head")
    logger.info("schema_at_head")


def wait_for_db(dsn: str, timeout: float = 60.0) -> None:
    """Retry until the server accepts connections (compose healthcheck races)."""
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


def main() -> None:
    configure(service="migrate")
    dsn = IngestSettings().db_dsn
    wait_for_db(dsn)
    upgrade(dsn)


if __name__ == "__main__":
    main()
