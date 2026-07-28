"""Integration: the migration chain itself, against a real TimescaleDB.

Each test that needs a database in a specific starting state (empty, M2-era,
half-migrated) creates a throwaway database on the session container —
CREATE DATABASE is cheap, and template1 in the timescale image carries the
extension, so throwaway databases behave exactly like the compose one.
"""

from datetime import timedelta
from pathlib import Path
from typing import Any, LiteralString

import psycopg
import pytest
from alembic import command
from alembic.script import ScriptDirectory
from psycopg import sql

from planter_telemetry import migrate

pytestmark = pytest.mark.integration

_M2_SCHEMA = Path(__file__).parent / "fixtures" / "m2_schema.sql"


def _fresh_db(session_dsn: str, name: str) -> str:
    """A brand-new empty database on the session container."""
    with psycopg.connect(session_dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
        )
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    base, _, _ = session_dsn.rpartition("/")
    return f"{base}/{name}"


def _rows(dsn: str, query: LiteralString) -> list[tuple[Any, ...]]:
    with psycopg.connect(dsn) as conn:
        return conn.execute(query).fetchall()


def _head_revision() -> str:
    script = ScriptDirectory.from_config(migrate.build_config("postgresql://unused/unused"))
    heads = script.get_heads()
    assert len(heads) == 1
    return heads[0]


_DESCRIBE_TABLES: LiteralString = """
    SELECT table_name, column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name IN ('telemetry', 'dead_letter')
    ORDER BY table_name, ordinal_position
"""

_DESCRIBE_CONSTRAINTS: LiteralString = """
    SELECT conrelid::regclass::text, contype, pg_get_constraintdef(oid)
    FROM pg_constraint
    WHERE conrelid IN ('telemetry'::regclass, 'dead_letter'::regclass)
    ORDER BY 1, 2, 3
"""


def test_migrated_from_empty_matches_expected_schema(db_dsn: str) -> None:
    """The session database was migrated from empty by the fixture; assert
    the end state a fresh CI database must land in."""
    assert _rows(db_dsn, "SELECT version_num FROM alembic_version") == [(_head_revision(),)]

    tables = {
        row[0]
        for row in _rows(
            db_dsn,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
        )
    }
    assert {
        "telemetry",
        "dead_letter",
        "devices",
        "ingest_events",
        "forecasts",
        "alert_events",
    } <= tables

    # The ingest event log carries the wall-clock index its rate panels need.
    assert ("ingest_events_occurred_at_idx",) in _rows(
        db_dsn,
        "SELECT indexname FROM pg_indexes WHERE tablename = 'ingest_events'",
    )

    # The alert log carries the latest-decision index its transition rule
    # (and the dashboard) read through.
    assert ("alert_events_state_idx",) in _rows(
        db_dsn,
        "SELECT indexname FROM pg_indexes WHERE tablename = 'alert_events'",
    )

    # Forecast idempotency is the primary key itself: (device, kind, as_of).
    assert (
        "forecasts",
        "p",
        "PRIMARY KEY (device_id, kind, as_of)",
    ) in _rows(
        db_dsn,
        "SELECT conrelid::regclass::text, contype, pg_get_constraintdef(oid)"
        " FROM pg_constraint WHERE conrelid = 'forecasts'::regclass",
    )

    # grafana_reader can read everything the dashboard queries — and only
    # read. Default privileges cover tables from future migrations.
    with psycopg.connect(db_dsn) as conn:
        for relation in (
            "telemetry",
            "telemetry_hourly",
            "devices",
            "dead_letter",
            "ingest_events",
            "forecasts",
            "forecasts_latest",
            "alert_events",
        ):
            grants = conn.execute(
                "SELECT has_table_privilege('grafana_reader', %s, 'SELECT'),"
                " has_table_privilege('grafana_reader', %s, 'INSERT')",
                (relation, relation),
            ).fetchone()
            assert grants == (True, False), relation
    assert _rows(
        db_dsn,
        "SELECT count(*) FROM pg_default_acl a JOIN pg_namespace n ON a.defaclnamespace = n.oid"
        " WHERE n.nspname = 'public'"
        " AND array_to_string(a.defaclacl, ',') LIKE '%grafana_reader=r/%'",
    ) == [(1,)]

    # telemetry is a hypertable partitioned on measured_at in 1-day chunks.
    assert _rows(
        db_dsn,
        "SELECT hypertable_name FROM timescaledb_information.hypertables",
    ) == [("telemetry",)]
    assert _rows(
        db_dsn,
        "SELECT column_name, time_interval FROM timescaledb_information.dimensions"
        " WHERE hypertable_name = 'telemetry'",
    ) == [("measured_at", timedelta(days=1))]

    # The idempotency key survived the conversion.
    constraints = _rows(db_dsn, _DESCRIBE_CONSTRAINTS)
    assert ("telemetry", "u", "UNIQUE (device_id, measured_at)") in constraints

    # Both rollups exist as real-time aggregates (materialized_only = false
    # is the non-default that makes them queryable mid-demo), each with a
    # refresh policy job.
    assert _rows(
        db_dsn,
        "SELECT view_name, materialized_only"
        " FROM timescaledb_information.continuous_aggregates ORDER BY view_name",
    ) == [("telemetry_daily", False), ("telemetry_hourly", False)]
    assert _rows(
        db_dsn,
        "SELECT count(*) FROM timescaledb_information.jobs"
        " WHERE proc_name = 'policy_refresh_continuous_aggregate'",
    ) == [(2,)]

    # Columnstore is enabled on telemetry (segmented by device) with a
    # compression policy job — and there is deliberately NO retention job.
    # (The info view still spells columnstore enablement `compression_enabled`.)
    assert _rows(
        db_dsn,
        "SELECT compression_enabled FROM timescaledb_information.hypertables"
        " WHERE hypertable_name = 'telemetry'",
    ) == [(True,)]
    assert _rows(
        db_dsn,
        "SELECT count(*) FROM timescaledb_information.jobs"
        " WHERE proc_name = 'policy_compression' AND hypertable_name = 'telemetry'",
    ) == [(1,)]
    assert _rows(
        db_dsn,
        "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention'",
    ) == [(0,)]


def test_upgrade_twice_is_a_noop(db_dsn: str) -> None:
    dsn = _fresh_db(db_dsn, "idempotency")
    migrate.upgrade(dsn)
    before_version = _rows(dsn, "SELECT version_num FROM alembic_version")
    before_schema = _rows(dsn, _DESCRIBE_TABLES)

    migrate.upgrade(dsn)  # must not raise, must change nothing
    assert _rows(dsn, "SELECT version_num FROM alembic_version") == before_version
    assert _rows(dsn, _DESCRIBE_TABLES) == before_schema


def test_interrupted_0004_recovers_on_rerun(db_dsn: str) -> None:
    """0004 runs in an autocommit block, so a migrate container killed after
    the first CREATE leaves telemetry_hourly behind with alembic_version
    still at 0003 — and every `docker compose up` re-runs migrate against
    that state. The re-run must reach head, not die on 'already exists'."""
    dsn = _fresh_db(db_dsn, "crashed_mid_0004")
    config = migrate.build_config(dsn)
    command.upgrade(config, "0003")

    # Reproduce the crash state with 0004's own first statement, pulled from
    # the loaded migration module so this can never drift from the real DDL.
    rollup_template = ScriptDirectory.from_config(config).get_revision("0004").module._ROLLUP
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(rollup_template.format(name="telemetry_hourly", bucket="1 hour"))

    migrate.upgrade(dsn)

    assert _rows(dsn, "SELECT version_num FROM alembic_version") == [(_head_revision(),)]
    assert _rows(
        dsn,
        "SELECT view_name FROM timescaledb_information.continuous_aggregates ORDER BY view_name",
    ) == [("telemetry_daily",), ("telemetry_hourly",)]
    assert _rows(
        dsn,
        "SELECT count(*) FROM timescaledb_information.jobs"
        " WHERE proc_name = 'policy_refresh_continuous_aggregate'",
    ) == [(2,)]
    assert _rows(
        dsn,
        "SELECT count(*) FROM timescaledb_information.jobs"
        " WHERE proc_name = 'policy_compression' AND hypertable_name = 'telemetry'",
    ) == [(1,)]


def test_baseline_reproduces_m2_schema_exactly(db_dsn: str) -> None:
    """A database at revision 0001 is indistinguishable from an M2 init-script
    database — the invariant the stamp guard depends on."""
    via_migration = _fresh_db(db_dsn, "baseline_migrated")
    command.upgrade(migrate.build_config(via_migration), migrate.BASELINE_REVISION)

    via_init_script = _fresh_db(db_dsn, "baseline_m2")
    with psycopg.connect(via_init_script, autocommit=True) as conn:
        conn.execute(_M2_SCHEMA.read_bytes())

    assert _rows(via_migration, _DESCRIBE_TABLES) == _rows(via_init_script, _DESCRIBE_TABLES)
    assert _rows(via_migration, _DESCRIBE_CONSTRAINTS) == _rows(
        via_init_script, _DESCRIBE_CONSTRAINTS
    )


def test_stamp_guard_migrates_a_pre_alembic_m2_database(db_dsn: str) -> None:
    """An M2-era database (tables but no alembic_version) with existing rows
    is stamped at 0001 and upgraded; migrate_data carries the rows along."""
    dsn = _fresh_db(db_dsn, "m2_era")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(_M2_SCHEMA.read_bytes())
        conn.execute(
            "INSERT INTO telemetry"
            " (device_id, measured_at, water_level, battery_voltage, schema_version)"
            " VALUES ('planter-legacy', '2026-01-01T00:00:00Z', 55.5, 3.8, 1)"
        )

    migrate.upgrade(dsn)

    assert _rows(dsn, "SELECT version_num FROM alembic_version") == [(_head_revision(),)]
    assert _rows(dsn, "SELECT hypertable_name FROM timescaledb_information.hypertables") == [
        ("telemetry",)
    ]
    # The pre-existing row lives on inside a chunk now.
    assert _rows(dsn, "SELECT device_id, water_level FROM telemetry") == [("planter-legacy", 55.5)]
    assert _rows(dsn, "SELECT count(*) FROM devices") == [(0,)]
