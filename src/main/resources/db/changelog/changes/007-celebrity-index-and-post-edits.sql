--liquibase formatted sql

--changeset twitter:011-users-celebrity-index
-- Every app instance reloads the set of celebrity ids every few seconds (CelebrityDirectory). Partial index:
-- only celebrity rows are in it, so the query is an index-only scan over a small index.
CREATE INDEX idx_users_celebrity ON users (id) WHERE celebrity;
--rollback DROP INDEX idx_users_celebrity;

--changeset twitter:012-posts-edited-at
-- Set when the author edits the tweet; NULL = never edited (clients show an "Edited" label otherwise).
ALTER TABLE posts ADD COLUMN edited_at TIMESTAMPTZ;
--rollback ALTER TABLE posts DROP COLUMN edited_at;
