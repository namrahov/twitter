package az.twitter.repository;

import java.time.Duration;
import java.util.UUID;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

/**
 * Fan-out checkpoint: {@code fanout:progress:{eventId}} = id of the last follower whose timeline was written.
 * If the worker dies halfway, the redelivered event resumes after that follower instead of starting over.
 */
@Repository
public class FanoutProgressRepository {

    /** Long enough to outlive Kafka's redelivery retries; leftovers of abandoned events clean themselves up. */
    private static final Duration TTL = Duration.ofHours(1);

    private final StringRedisTemplate redis;

    public FanoutProgressRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** @return the last processed follower id, or 0 to start from the beginning */
    public long lastFollowerId(UUID eventId) {
        String value = redis.opsForValue().get(key(eventId));
        return value == null ? 0 : Long.parseLong(value);
    }

    public void save(UUID eventId, long lastFollowerId) {
        redis.opsForValue().set(key(eventId), String.valueOf(lastFollowerId), TTL);
    }

    public void clear(UUID eventId) {
        redis.delete(key(eventId));
    }

    private static String key(UUID eventId) {
        return "fanout:progress:" + eventId;
    }
}
