package az.twitter.repository;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;

/**
 * Precomputed home timelines in Redis: one sorted set per user, {@code timeline:user:{id}}.
 *
 * <p>Every member has score 0 and is the tweet id zero-padded to 19 digits. With equal scores Redis orders
 * members lexicographically, and fixed-width digits sort lexicographically exactly as they do numerically,
 * so the set is ordered by Snowflake id = by time, deterministically, and cursor paging is ZREVRANGEBYLEX.
 * (A timestamp score would tie within one millisecond; the full id does not fit a double's 53 bits.)
 */
@Repository
public class TimelineCacheRepository {

    /** Older entries are dropped; reads past them fall back to SQL. */
    public static final int MAX_ENTRIES = 800;

    private final StringRedisTemplate redis;

    public TimelineCacheRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * Adds the tweet to every user's timeline in one pipelined round trip.
     * Idempotent: re-adding the same member changes nothing, so redelivered events are harmless.
     */
    public void addToTimelines(Collection<Long> userIds, long tweetId) {
        String member = member(tweetId);
        redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> operations) {
                ZSetOperations<String, String> zset = ((RedisOperations<String, String>) operations).opsForZSet();
                for (Long userId : userIds) {
                    String key = key(userId);
                    zset.add(key, member, 0);
                    zset.removeRange(key, 0, -(MAX_ENTRIES + 1)); // keep the newest MAX_ENTRIES
                }
                return null;
            }
        });
    }

    /** ZREM: removes the given tweets from one user's timeline; ids not in it are ignored (idempotent). */
    public void removeFromTimeline(long userId, Collection<Long> tweetIds) {
        if (tweetIds.isEmpty()) {
            return;
        }
        redis.opsForZSet().remove(key(userId), tweetIds.stream().map(TimelineCacheRepository::member).toArray());
    }

    /** Tweet ids older than {@code beforeId}, newest first. */
    public List<Long> page(long userId, long beforeId, int limit) {
        Range<String> range = beforeId == Long.MAX_VALUE
                ? Range.unbounded()
                : Range.leftUnbounded(Range.Bound.exclusive(member(beforeId)));
        Set<String> members = redis.opsForZSet()
                .reverseRangeByLex(key(userId), range, Limit.limit().count(limit));
        return members == null ? List.of() : members.stream().map(Long::parseLong).toList();
    }

    static String key(long userId) {
        return "timeline:user:" + userId;
    }

    static String member(long tweetId) {
        return String.format("%019d", tweetId);
    }
}
