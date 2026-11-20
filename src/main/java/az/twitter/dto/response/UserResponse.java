package az.twitter.dto.response;

import az.twitter.entity.UserEntity;

import java.time.Instant;

/** {@code id} is a string: Snowflake ids exceed JavaScript's 2^53 safe integer range. */
public record UserResponse(String id, String username, String displayName, Instant createdAt) {

    public static UserResponse from(UserEntity user) {
        return new UserResponse(String.valueOf(user.getId()), user.getUsername(), user.getDisplayName(), user.getCreatedAt());
    }
}
