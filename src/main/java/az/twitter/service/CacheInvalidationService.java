package az.twitter.service;

import az.twitter.dto.cache.CacheInvalidation;
import az.twitter.dto.cache.CacheInvalidation.LocalCache;
import az.twitter.dto.cache.CachedPost;
import az.twitter.dto.cache.CelebrityPostsSnapshot;

import java.time.Duration;
import java.util.UUID;

import com.github.benmanes.caffeine.cache.AsyncCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

/**
 * Keeps the L1 (Caffeine) copies of all app instances in line with Redis.
 *
 * <p>Whoever changes shared data in Redis publishes the changed key on {@link #CHANNEL}; every instance
 * (the publisher included) is subscribed and drops that key from its L1, so its next read goes to Redis.
 *
 * <p><b>Lost messages.</b> Redis Pub/Sub is fire-and-forget: while this instance's subscription is down, messages
 * are not queued for it, they are gone. So an L1 copy is only trusted while the subscription is known to work:
 * <ul>
 *   <li>Lost: the subscription connection dropped (Lettuce reports it at once, see CacheInvalidationConfig), or
 *       this instance's own heartbeat ping has not come back for {@link #STALE_AFTER} (covers a connection that
 *       hangs without dropping). From then on both L1 caches are bypassed: reads go to Redis directly.</li>
 *   <li>Back: the first own ping that arrives again proves the subscription is live. Any invalidation sent during
 *       the gap may be missing, so both L1 caches are cleared before they are trusted again.</li>
 * </ul>
 * A message lost without a disconnect being noticed is still bounded by the L1 TTL (the last safety net).
 */
@Service
public class CacheInvalidationService {

    public static final String CHANNEL = "cache-invalidation";

    private static final long HEARTBEAT_INTERVAL_MS = 1000;
    /** Three missed pings in a row: long enough to ignore a slow one, short enough for the 5 s freshness goal. */
    private static final Duration STALE_AFTER = Duration.ofSeconds(3);

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationService.class);

    private final StringRedisTemplate redis;
    private final JsonMapper json;
    private final AsyncCache<Long, CachedPost> tweetLocalCache;
    private final AsyncCache<Long, CelebrityPostsSnapshot> celebrityPostsLocalCache;

    /** Own channel, so each instance gets only its own pings (one message per second, not one per instance). */
    private final String pingChannel = CHANNEL + ":ping:" + UUID.randomUUID();
    /** Not trusted until the subscription has delivered the first ping. */
    private volatile boolean trusted;
    private volatile long lastPingNanos = System.nanoTime();

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

    /** False while invalidations may be getting lost: then do not read L1, go to Redis. */
    public boolean isLocalCacheTrusted() {
        return trusted;
    }

    public String pingChannel() {
        return pingChannel;
    }

    @Scheduled(fixedRate = HEARTBEAT_INTERVAL_MS)
    void heartbeat() {
        try {
            redis.convertAndSend(pingChannel, "ping");
        } catch (RuntimeException e) {
            log.debug("Heartbeat ping not sent: {}", e.getMessage()); // Redis down: the silence check below catches it
        }
        if (trusted && System.nanoTime() - lastPingNanos > STALE_AFTER.toNanos()) {
            distrust("no heartbeat for " + STALE_AFTER.toSeconds() + " s");
        }
    }

    /** Own ping came back through the subscription: it works (again). */
    public void onPing() {
        lastPingNanos = System.nanoTime();
        if (!trusted) {
            synchronized (this) {
                if (!trusted) {
                    // Clear first, trust after: a request must never read a copy that may have missed a message.
                    celebrityPostsLocalCache.synchronous().invalidateAll();
                    tweetLocalCache.synchronous().invalidateAll();
                    trusted = true;
                    log.info("L1 invalidation channel live; L1 caches cleared and in use again");
                }
            }
        }
    }

    /** The Pub/Sub connection dropped (Lettuce reconnects and resubscribes by itself; {@link #onPing} notices). */
    public void onSubscriptionLost() {
        distrust("subscription connection lost");
    }

    private synchronized void distrust(String reason) {
        if (trusted) {
            trusted = false;
            log.warn("L1 invalidation channel down ({}); L1 caches bypassed until it is back", reason);
        }
    }
}
