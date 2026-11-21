package az.twitter.repository;

import java.time.Duration;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

/**
 * Precomputed home timelines (push part): {@code timeline:user:{id}} holds ids of the user's own tweets and of
 * the non-celebrity accounts they follow, written by fan-out on write. Celebrity tweets are not in here;
 * they are pulled from {@link CelebrityPostsCacheRepository} at read time.
 */
@Repository
public class TimelineCacheRepository extends SortedIdCache {

    /** Older entries are dropped; reads past them fall back to SQL. */
    public static final int MAX_ENTRIES = 800;

    /** Sliding expiry: refreshed on every read, so only inactive users' timelines expire. */
    public static final Duration TTL = Duration.ofDays(7);

    public TimelineCacheRepository(StringRedisTemplate redis) {
        super(redis, "timeline:user:", MAX_ENTRIES, TTL);
    }
}
