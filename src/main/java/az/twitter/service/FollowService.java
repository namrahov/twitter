package az.twitter.service;

import az.twitter.dto.event.UserUnfollowedEvent;
import az.twitter.dto.response.CursorPage;
import az.twitter.dto.response.UserResponse;
import az.twitter.entity.UserEntity;
import az.twitter.repository.FollowRepository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Limit;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class FollowService {

    private final FollowRepository follows;
    private final UserService users;
    private final OutboxService outbox;

    public FollowService(FollowRepository follows, UserService users, OutboxService outbox) {
        this.follows = follows;
        this.users = users;
        this.outbox = outbox;
    }

    public void follow(long followerId, long followeeId) {
        if (followerId == followeeId) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Cannot follow yourself");
        }
        try {
            // Single statement, no "does the user exist?" round trip: the FK answers that.
            follows.insertIfAbsent(followerId, followeeId);
        } catch (DataIntegrityViolationException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found");
        }
    }

    /**
     * Deletes the follow and, in the same transaction, queues a UserUnfollowed event; a worker then removes
     * the followee's tweets from the follower's Redis timeline. Until it runs (~1 s), they may still show.
     * No row deleted (already unfollowed) → no event.
     */
    @Transactional
    public void unfollow(long followerId, long followeeId) {
        if (follows.delete(followerId, followeeId) == 0) {
            return;
        }
        var event = new UserUnfollowedEvent(UUID.randomUUID(), followerId, followeeId, Instant.now());
        // Key = follower: one user's follow/unfollow events stay ordered in one partition.
        outbox.append(event.eventId(), UserUnfollowedEvent.TYPE, followeeId, followerId, event);
    }

    public CursorPage<UserResponse> followers(long userId, long afterId, int limit) {
        users.requireExists(userId);
        return page(follows.findFollowers(userId, afterId, Limit.of(limit + 1)), limit);
    }

    public CursorPage<UserResponse> following(long userId, long afterId, int limit) {
        users.requireExists(userId);
        return page(follows.findFollowing(userId, afterId, Limit.of(limit + 1)), limit);
    }

    private static CursorPage<UserResponse> page(List<UserEntity> rows, int limit) {
        return CursorPage.of(rows, limit, UserEntity::getId, UserResponse::from);
    }
}
