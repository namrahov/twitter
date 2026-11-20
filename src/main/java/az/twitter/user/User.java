package az.twitter.user;

import java.time.Instant;

public record User(long id, String username, String displayName, Instant createdAt) {
}
