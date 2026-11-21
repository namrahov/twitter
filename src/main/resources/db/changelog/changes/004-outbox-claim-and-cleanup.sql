--liquibase formatted sql

--changeset twitter:007-outbox-locked-until
-- Claim-based publishing: a publisher marks rows as taken until locked_until instead of holding row locks
-- (and a DB connection) while it waits for Kafka. A crashed publisher's claim simply expires.
ALTER TABLE outbox ADD COLUMN locked_until TIMESTAMPTZ;
--rollback ALTER TABLE outbox DROP COLUMN locked_until;

--changeset twitter:008-outbox-published-index
-- Lets the cleanup job find old published rows without scanning the whole table.
CREATE INDEX idx_outbox_published ON outbox (published_at) WHERE status = 'PUBLISHED';
--rollback DROP INDEX idx_outbox_published;
