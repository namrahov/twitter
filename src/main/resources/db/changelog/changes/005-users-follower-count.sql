--liquibase formatted sql

--changeset twitter:009-users-follower-count
-- follower_count: maintained in the follow/unfollow transaction, so deciding "push or pull?" needs no COUNT(*).
-- celebrity: set once follower_count reaches the threshold and never cleared automatically ("sticky"),
-- so an account hovering around the threshold does not flip between push and pull.
ALTER TABLE users ADD COLUMN follower_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE users ADD COLUMN celebrity BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE users ADD CONSTRAINT chk_users_follower_count CHECK (follower_count >= 0);
UPDATE users u SET follower_count = (SELECT count(*) FROM follows f WHERE f.followee_id = u.id);
-- Backfill with the default threshold (twitter.timeline.celebrity-threshold = 10000).
UPDATE users SET celebrity = true WHERE follower_count >= 10000;
--rollback ALTER TABLE users DROP CONSTRAINT chk_users_follower_count; ALTER TABLE users DROP COLUMN celebrity; ALTER TABLE users DROP COLUMN follower_count;
