package az.twitter.dto.cache;

import az.twitter.entity.PostEntity;

import java.time.Instant;

/**
 * What is stored per tweet in Redis ({@code tweet:{id}}): everything a timeline item needs, as JSON.
 *
 * @param editedAt null if never edited (also for entries cached before the field existed)
 */
public record CachedPost(long id, long userId, String content, Instant createdAt, Instant editedAt) {

    public static CachedPost from(PostEntity post) {
        return new CachedPost(post.getId(), post.getUserId(), post.getContent(), post.getCreatedAt(),
                post.getEditedAt());
    }
}
