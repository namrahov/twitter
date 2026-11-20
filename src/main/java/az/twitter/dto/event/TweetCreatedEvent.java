package az.twitter.dto.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Only ids: the fan-out worker needs nothing else, and content stays in one place (the posts table).
 * eventId lets consumers detect redeliveries (the outbox is at-least-once).
 */
public record TweetCreatedEvent(UUID eventId, long tweetId, long authorId, Instant createdAt) {

    public static final String TYPE = "TweetCreated";
}
