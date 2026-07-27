"""Read-only grafana_reader role: Grafana never connects as the owner.

Least privilege for the M4 dashboard: SELECT on everything in public (which
covers the continuous-aggregate views; their internal materialized tables
need no grant because view access runs with the view owner's privileges),
USAGE on the schema, and default privileges so tables from future
migrations (M7's forecasts) are readable without another grant. The role
also defaults to read-only transactions as a belt over the missing write
grants. Dev-only credentials, same posture as planter/planter.

Roles are cluster-level and CREATE ROLE has no IF NOT EXISTS, hence the DO
guard — it makes the revision safe against re-runs and against clusters
where the role already exists (the integration suite shares one container
across tests). The GRANTs and ALTERs are idempotent, so the rest re-runs
harmlessly too; everything is transactional, no autocommit block needed.

ALTER DEFAULT PRIVILEGES deliberately has no FOR ROLE clause: it then binds
to the role executing this migration, and migrations always run as planter
(compose INGEST_DB_DSN, the test fixtures) — exactly the role that creates
every future table. A FOR ROLE planter spelling would break if a
non-member ever ran migrations, for zero gain.

Downgrade is deliberately not implemented: the chain is forward-only past
0003 (see 0003's docstring).

Revision ID: 0007
Revises: 0006
"""

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
                CREATE ROLE grafana_reader LOGIN PASSWORD 'grafana_reader';
            END IF;
        END $$
        """
    )
    op.execute("GRANT USAGE ON SCHEMA public TO grafana_reader")
    op.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader")
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader")
    op.execute("ALTER ROLE grafana_reader SET default_transaction_read_only = on")


def downgrade() -> None:
    raise NotImplementedError("the schema is forward-only past 0003; restore from backup instead")
