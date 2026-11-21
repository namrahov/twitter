package az.twitter.config;

import az.twitter.dto.cache.CachedPost;
import az.twitter.dto.cache.CelebrityPostsSnapshot;

import java.time.Duration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * L1: in-process caches (Caffeine) in front of Redis, only for data that many users read at the same time.
 * They take the hot-key load off Redis: with N app instances, a hot key costs Redis about N reads per TTL
 * instead of one read per request. Each instance has its own copy, so data can differ between instances
 * for up to the TTL. Per-user timelines are not here: nobody else reads them, so L1 would not help.
 */
@Configuration
public class LocalCacheConfig {

    /**
     * Tweet content. Tweets cannot be edited or deleted yet, so a copy never goes stale; the TTL only bounds
     * how long a cold tweet occupies memory (the size limit evicts least used ones earlier).
     */
    @Bean
    public Cache<Long, CachedPost> tweetLocalCache() {
        return Caffeine.newBuilder()
                .maximumSize(100_000)
                .expireAfterWrite(Duration.ofMinutes(10))
                .build();
    }

    /**
     * Celebrity posts lists. Short TTL: a new celebrity tweet shows up here at most 2 s later
     * (within the 5 s freshness goal), while a celebrity read by millions costs Redis one read per 2 s per instance.
     */
    @Bean
    public Cache<Long, CelebrityPostsSnapshot> celebrityPostsLocalCache() {
        return Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofSeconds(2))
                .build();
    }
}
