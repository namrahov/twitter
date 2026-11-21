package az.twitter.dto.event;

import java.time.Instant;
import java.util.UUID;

/** followerId started following followeeId: followee's recent tweets are backfilled into follower's timeline. */
public record UserFollowedEvent(UUID eventId, long followerId, long followeeId, Instant occurredAt) {

    public static final String TYPE = "UserFollowed";
}
