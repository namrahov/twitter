package az.twitter.dto.cache;

import az.twitter.entity.PostEntity;

import java.time.Instant;

/** What is stored per tweet in Redis ({@code tweet:{id}}): everything a timeline item needs, as JSON. */
public record CachedPost(long id, long userId, String content, Instant createdAt) {

    public static CachedPost from(PostEntity post) {
        return new CachedPost(post.getId(), post.getUserId(), post.getContent(), post.getCreatedAt());
    }
}
