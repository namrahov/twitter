package az.twitter.dto.event;

import java.time.Instant;
import java.util.UUID;

/** The row is already gone when this is handled; workers remove the id from every cache that may hold it. */
public record TweetDeletedEvent(UUID eventId, long tweetId, long authorId, Instant deletedAt) {

    public static final String TYPE = "TweetDeleted";
}
