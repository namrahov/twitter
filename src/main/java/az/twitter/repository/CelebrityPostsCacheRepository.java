package az.twitter.repository;

import java.time.Duration;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

/**
 * Recent tweet ids of one celebrity, {@code posts:user:{id}}: stored once and read by all their followers
 * (fan-out on read), instead of being written into millions of follower timelines.
 */
@Repository
public class CelebrityPostsCacheRepository extends SortedIdCache {

    /** A page needs ~20 per celebrity; 200 covers the first pages, deeper paging falls back to SQL. */
    public static final int MAX_ENTRIES = 200;

    /** Refreshed on every read; a celebrity nobody reads for a day drops out of memory. */
    public static final Duration TTL = Duration.ofDays(1);

    public CelebrityPostsCacheRepository(StringRedisTemplate redis) {
        super(redis, "posts:user:", MAX_ENTRIES, TTL);
    }
}
