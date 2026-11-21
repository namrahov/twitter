package az.twitter.dto.event;

import java.time.Instant;
import java.util.UUID;

/** Only ids, like TweetCreated: caches drop their copy and reload the new text from the posts table. */
public record TweetEditedEvent(UUID eventId, long tweetId, long authorId, Instant editedAt) {

    public static final String TYPE = "TweetEdited";
}
