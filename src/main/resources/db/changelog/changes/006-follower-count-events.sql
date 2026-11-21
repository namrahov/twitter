--liquibase formatted sql

--changeset twitter:010-follower-count-events
-- Ids of follow/unfollow events already applied to users.follower_count. Kafka delivers at least once;
-- inserting the id in the same transaction as the counter update makes a redelivered event a no-op.
CREATE TABLE follower_count_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_follower_count_events_processed_at ON follower_count_events (processed_at);
--rollback DROP TABLE follower_count_events;
