package az.twitter.user;

import java.time.Instant;

/** {@code id} is a string: Snowflake ids exceed JavaScript's 2^53 safe integer range. */
public record UserResponse(String id, String username, String displayName, Instant createdAt) {

    public static UserResponse from(User user) {
        return new UserResponse(String.valueOf(user.id()), user.username(), user.displayName(), user.createdAt());
    }
}
