package az.twitter.dto.response;

import az.twitter.dto.cache.CachedPost;
import az.twitter.entity.PostEntity;

import java.time.Instant;

/**
 * Ids are strings: Snowflake ids exceed JavaScript's 2^53 safe integer range.
 *
 * @param editedAt null if the tweet was never edited; otherwise clients show an "Edited" label
 */
public record PostResponse(String id, String authorId, String content, Instant createdAt, Instant editedAt) {

    public static PostResponse from(PostEntity post) {
        return new PostResponse(String.valueOf(post.getId()), String.valueOf(post.getUserId()),
                post.getContent(), post.getCreatedAt(), post.getEditedAt());
    }

    public static PostResponse from(CachedPost post) {
        return new PostResponse(String.valueOf(post.id()), String.valueOf(post.userId()),
                post.content(), post.createdAt(), post.editedAt());
    }
}
