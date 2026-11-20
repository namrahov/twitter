--liquibase formatted sql

--changeset twitter:001-users
CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_users_id CHECK (id > 0),
    CONSTRAINT chk_users_username CHECK (username ~ '^[A-Za-z0-9_]{1,50}$'),
    CONSTRAINT chk_users_display_name CHECK (length(btrim(display_name)) > 0)
);
CREATE UNIQUE INDEX uq_users_username_lower ON users (lower(username));
--rollback DROP TABLE users;

--changeset twitter:002-posts
CREATE TABLE posts (
    id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
    content VARCHAR(280) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_posts_id CHECK (id > 0),
    CONSTRAINT chk_posts_content CHECK (length(btrim(content)) > 0)
);
CREATE INDEX idx_posts_user_created ON posts (user_id, created_at DESC, id DESC);
--rollback DROP TABLE posts;

--changeset twitter:003-follows
CREATE TABLE follows (
    follower_id BIGINT NOT NULL REFERENCES users(id),
    followee_id BIGINT NOT NULL REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (follower_id, followee_id),
    CONSTRAINT chk_follows_no_self_follow CHECK (follower_id <> followee_id)
);
CREATE INDEX idx_follows_followee ON follows (followee_id, follower_id);
--rollback DROP TABLE follows;

--changeset twitter:004-outbox
CREATE TABLE outbox (
    event_id UUID PRIMARY KEY,
    aggregate_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ,
    CONSTRAINT chk_outbox_aggregate_id CHECK (aggregate_id > 0),
    CONSTRAINT chk_outbox_user_id CHECK (user_id > 0),
    CONSTRAINT chk_outbox_event_type CHECK (length(btrim(event_type)) > 0),
    CONSTRAINT chk_outbox_status CHECK (status IN ('PENDING', 'PUBLISHED')),
    CONSTRAINT chk_outbox_attempts CHECK (attempts >= 0),
    CONSTRAINT chk_outbox_published_at CHECK (
        (status = 'PENDING' AND published_at IS NULL)
        OR (status = 'PUBLISHED' AND published_at IS NOT NULL)
    )
);
CREATE INDEX idx_outbox_pending ON outbox (created_at, event_id) WHERE status = 'PENDING';
--rollback DROP TABLE outbox;
