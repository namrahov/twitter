--liquibase formatted sql

--changeset twitter:006-snowflake-workers
-- Snowflake worker id leases. Each running instance holds exactly one row; a row is free when expires_at has passed.
-- last_timestamp: the highest id timestamp (ms) the last owner may have used; a new owner waits until its clock is past it.
CREATE TABLE snowflake_workers (
    worker_id INTEGER PRIMARY KEY,
    owner VARCHAR(200),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
    last_timestamp BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT chk_snowflake_workers_id CHECK (worker_id BETWEEN 0 AND 1023)
);
INSERT INTO snowflake_workers (worker_id) SELECT generate_series(0, 1023);
--rollback DROP TABLE snowflake_workers;
