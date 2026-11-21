package az.twitter.config;

import az.twitter.dto.cache.CachedPost;
import az.twitter.dto.cache.CelebrityPostsSnapshot;

import java.time.Duration;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * L1: in-process caches (Caffeine) in front of Redis, only for data that many users read at the same time.
 * They take the hot-key load off Redis: with N app instances, a hot key costs Redis about N reads per TTL
 * instead of one read per request. Each instance has its own copy; changes reach all copies through
 * Redis Pub/Sub invalidation (CacheInvalidationService), with the TTL as the upper bound. Per-user timelines are not here: nobody else reads them, so L1 would not help.
 *
 * <p>Async caches on purpose: a key being loaded holds a future, so concurrent requests missing the same key
 * wait for that one load instead of each loading it again (request coalescing — stampede protection
 * within one instance; {@code RebuildLockRepository} does the same across instances).
 */
@Configuration
public class LocalCacheConfig {

    /**
     * Tweet content. Tweets cannot be edited or deleted yet, so a copy never goes stale; the TTL only bounds
     * how long a cold tweet occupies memory (the size limit evicts least used ones earlier).
     */
    @Bean
    public AsyncCache<Long, CachedPost> tweetLocalCache() {
        return Caffeine.newBuilder()
                .maximumSize(100_000)
                .expireAfterWrite(Duration.ofMinutes(10))
                .buildAsync();
    }

    /**
     * Celebrity posts lists. A new celebrity tweet does not wait for the TTL: the fan-out worker publishes an
     * invalidation (CacheInvalidationService) and every instance drops its copy at once. The TTL is only the
     * safety net for an instance that missed that message (Pub/Sub is not durable), so it can be longer than
     * before (it was 2 s): a celebrity read by millions costs Redis one read per 30 s per instance, or per tweet.
     */
    @Bean
    public AsyncCache<Long, CelebrityPostsSnapshot> celebrityPostsLocalCache() {
        return Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofSeconds(30))
                .buildAsync();
    }
}
