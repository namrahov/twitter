package az.twitter.dto.cache;

/**
 * Message on the {@code cache-invalidation} Redis Pub/Sub channel: "drop this key from your L1".
 * Carries only the key, never the data: each instance reloads the fresh value from Redis on its next read.
 *
 * @param cache which L1 cache
 * @param id    the key in it (tweet id, celebrity user id)
 */
public record CacheInvalidation(LocalCache cache, long id) {

    public enum LocalCache {
        /** {@code tweetLocalCache}: a tweet was edited or deleted. */
        TWEET,
        /** {@code celebrityPostsLocalCache}: a celebrity posted a new tweet. */
        CELEBRITY_POSTS
    }
}
