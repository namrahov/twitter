package az.twitter.repository;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

/**
 * Cross-instance lock so that only one request rebuilds a missing cache entry from PostgreSQL
 * (cache stampede protection): {@code SET lock:<name> <token> NX PX <ttl>}.
 */
@Repository
public class RebuildLockRepository {

    /** Rebuilds take milliseconds; the TTL only matters if the holder dies, so the lock cannot stay forever. */
    public static final Duration TTL = Duration.ofSeconds(5);

    /**
     * Delete only if the lock still holds our token. Without the check, a holder whose lock already expired
     * (and was taken by someone else) would delete the other request's lock.
     */
    private static final RedisScript<Long> RELEASE = new DefaultRedisScript<>("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
              return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redis;

    public RebuildLockRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** @return the lock token if acquired, empty if another request holds the lock */
    public Optional<String> tryAcquire(String name) {
        String token = UUID.randomUUID().toString();
        Boolean acquired = redis.opsForValue().setIfAbsent(key(name), token, TTL);
        return Boolean.TRUE.equals(acquired) ? Optional.of(token) : Optional.empty();
    }

    public void release(String name, String token) {
        redis.execute(RELEASE, List.of(key(name)), token);
    }

    private static String key(String name) {
        return "lock:" + name;
    }
}
