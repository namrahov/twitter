package az.twitter.dto.response;

import az.twitter.entity.PostEntity;

import java.time.Instant;

/** Ids are strings: Snowflake ids exceed JavaScript's 2^53 safe integer range. */
public record PostResponse(String id, String authorId, String content, Instant createdAt) {

    public static PostResponse from(PostEntity post) {
        return new PostResponse(String.valueOf(post.getId()), String.valueOf(post.getUserId()),
                post.getContent(), post.getCreatedAt());
    }
}
