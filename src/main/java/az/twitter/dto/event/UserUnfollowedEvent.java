package az.twitter.dto.event;

import java.time.Instant;
import java.util.UUID;

/** followerId stopped following followeeId: followee's tweets must leave follower's Redis timeline. */
public record UserUnfollowedEvent(UUID eventId, long followerId, long followeeId, Instant occurredAt) {

    public static final String TYPE = "UserUnfollowed";
}
