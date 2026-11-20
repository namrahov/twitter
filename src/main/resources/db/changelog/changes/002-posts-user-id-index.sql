--liquibase formatted sql

--changeset twitter:005-posts-user-id-index
-- Snowflake ids are time-ordered, so pages are cut by id ("id < :cursor ORDER BY id DESC").
-- idx_posts_user_created orders by created_at first and cannot serve that without a sort.
CREATE INDEX idx_posts_user_id ON posts (user_id, id DESC);
--rollback DROP INDEX idx_posts_user_id;
