package az.twitter.service;

import az.twitter.dto.cache.CacheInvalidation;
import az.twitter.dto.cache.CacheInvalidation.LocalCache;
import az.twitter.dto.cache.CachedPost;
import az.twitter.dto.cache.CelebrityPostsSnapshot;

import com.github.benmanes.caffeine.cache.AsyncCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

/**
 * Keeps the L1 (Caffeine) copies of all app instances in line with Redis.
 *
 * <p>Whoever changes shared data in Redis publishes the changed key on {@link #CHANNEL}; every instance
 * (the publisher included) is subscribed and drops that key from its L1, so its next read goes to Redis.
 *
 * <p>Redis Pub/Sub is fire-and-forget: an instance that is disconnected at that moment never gets the message.
 * The L1 TTLs stay as the safety net: a missed invalidation means a stale copy for at most one TTL.
 */
@Service
public class CacheInvalidationService {

    public static final String CHANNEL = "cache-invalidation";

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationService.class);

    private final StringRedisTemplate redis;
    private final JsonMapper json;
    private final AsyncCache<Long, CachedPost> tweetLocalCache;
    private final AsyncCache<Long, CelebrityPostsSnapshot> celebrityPostsLocalCache;

    public CacheInvalidationService(StringRedisTemplate redis, JsonMapper json,
                                    AsyncCache<Long, CachedPost> tweetLocalCache,
                                    AsyncCache<Long, CelebrityPostsSnapshot> celebrityPostsLocalCache) {
        this.redis = redis;
        this.json = json;
        this.tweetLocalCache = tweetLocalCache;
        this.celebrityPostsLocalCache = celebrityPostsLocalCache;
    }

    /** Call after the Redis write, so an instance reloading on the message already sees the new value. */
    public void publish(LocalCache cache, long id) {
        redis.convertAndSend(CHANNEL, json.writeValueAsString(new CacheInvalidation(cache, id)));
    }

    /** Subscriber side, called for every message on {@link #CHANNEL}. */
    public void onMessage(String payload) {
        CacheInvalidation message;
        try {
            message = json.readValue(payload, CacheInvalidation.class);
        } catch (RuntimeException e) {
            log.warn("Ignoring malformed cache invalidation message: {}", payload);
            return;
        }
        // Invalidating also drops a load still in flight, so an older value read just before the change
        // is not put into L1 when that load completes.
        switch (message.cache()) {
            case TWEET -> tweetLocalCache.synchronous().invalidate(message.id());
            case CELEBRITY_POSTS -> celebrityPostsLocalCache.synchronous().invalidate(message.id());
        }
        log.debug("L1 invalidated: {} {}", message.cache(), message.id());
    }
}
