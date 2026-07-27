"""Unit: migration-chain hygiene, no database required.

`alembic check` only serves autogenerate workflows; for hand-written SQL
migrations the equivalent CI guard is structural: exactly one head (no
accidental branches from concurrent PRs) and the sequential-id convention.
"""

from alembic.script import ScriptDirectory

from planter_telemetry.migrate import build_config, sqlalchemy_url

_UNUSED_DSN = "postgresql://unused:unused@localhost/unused"


def _script_directory() -> ScriptDirectory:
    return ScriptDirectory.from_config(build_config(_UNUSED_DSN))


def test_exactly_one_head() -> None:
    assert len(_script_directory().get_heads()) == 1


def test_revision_ids_are_the_sequential_convention() -> None:
    revisions = [script.revision for script in _script_directory().walk_revisions("base", "heads")]
    revisions.reverse()  # walk_revisions yields newest first
    assert revisions == [f"{n:04d}" for n in range(1, len(revisions) + 1)]
    assert len(revisions) >= 1


def test_sqlalchemy_url_selects_psycopg3() -> None:
    assert (
        sqlalchemy_url("postgresql://u:p@host:5433/db") == "postgresql+psycopg://u:p@host:5433/db"
    )
    # Already-rewritten URLs pass through unchanged.
    assert sqlalchemy_url("postgresql+psycopg://u@host/db") == "postgresql+psycopg://u@host/db"


def test_sqlalchemy_url_accepts_every_libpq_dsn_form() -> None:
    """Anything psycopg.connect accepts must work here too — the migrate
    service and the ingestion Writer read the same INGEST_DB_DSN."""
    # The short scheme libpq also accepts.
    assert sqlalchemy_url("postgres://u:p@host:5433/db") == "postgresql+psycopg://u:p@host:5433/db"
    # key=value conninfo form.
    assert (
        sqlalchemy_url("host=host port=5433 user=u password=p dbname=db")
        == "postgresql+psycopg://u:p@host:5433/db"
    )
    # Extra libpq parameters survive as query parameters.
    assert (
        sqlalchemy_url("postgresql://u@host/db?sslmode=require")
        == "postgresql+psycopg://u@host/db?sslmode=require"
    )
