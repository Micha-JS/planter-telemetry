"""Alembic environment for raw-SQL migrations.

There is no ``target_metadata``: this repo migrates with hand-written SQL, not
autogenerate. The database URL comes from ``sqlalchemy.url`` when set
programmatically (``planter_telemetry.migrate.build_config``); the CLI
fallback reads the same ``INGEST_DB_DSN`` environment variable the ingestion
service uses, via its settings class (default: the compose stack's host port).
"""

from alembic import context
from sqlalchemy import create_engine

from planter_telemetry.ingestion.config import IngestSettings
from planter_telemetry.migrate import sqlalchemy_url


def _database_url() -> str:
    configured = context.config.get_main_option("sqlalchemy.url")
    if configured:
        return configured
    return sqlalchemy_url(IngestSettings().db_dsn)


def run_migrations_offline() -> None:
    """Emit SQL to stdout (`alembic upgrade --sql`) instead of executing it."""
    context.configure(url=_database_url(), literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_database_url())
    with engine.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
