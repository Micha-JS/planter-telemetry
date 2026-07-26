-- M2 schema: plain Postgres tables, applied by the timescale image's
-- /docker-entrypoint-initdb.d on first boot (the compose DB is volume-less, so
-- "first boot" is every `docker compose up`). M3 replaces this with real
-- migrations and converts telemetry into a hypertable.

CREATE TABLE telemetry (
    device_id       text             NOT NULL,
    measured_at     timestamptz      NOT NULL,
    water_level     double precision NOT NULL,
    battery_voltage double precision NOT NULL,
    schema_version  smallint         NOT NULL,
    received_at     timestamptz      NOT NULL DEFAULT now(),
    -- Idempotency key: ingestion writes with ON CONFLICT DO NOTHING against
    -- this constraint. Already hypertable-compatible for M3 (a hypertable's
    -- unique indexes must include the partition column, measured_at).
    UNIQUE (device_id, measured_at)
);

CREATE TABLE dead_letter (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic       text        NOT NULL,
    -- bytea, not text: truncation corruption can split a UTF-8 sequence, and
    -- the raw wire bytes are exactly what we want to preserve for debugging.
    payload     bytea       NOT NULL,
    reason      text        NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);
