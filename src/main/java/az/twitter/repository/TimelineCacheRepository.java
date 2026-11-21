package az.twitter.repository;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
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
 *
 * <p>Lifecycle: a key is created only by {@link #rebuild} (when its user reads the timeline), lives for
 * {@link #TTL} after the last read, and writers only ever add to keys that already exist. So users who stop
 * reading lose their key and stop costing fan-out writes and memory.
 */
@Repository
public class TimelineCacheRepository {

    /** Older entries are dropped; reads past them fall back to SQL. */
    public static final int MAX_ENTRIES = 800;

    /** Sliding expiry: refreshed on every read, so only inactive users' timelines expire. */
    public static final Duration TTL = Duration.ofDays(7);

    /**
     * ZADD + trim, but only if the key exists. Done in Lua because "check, then write" from the app is not atomic:
     * the key could expire in between, and the ZADD would recreate it without a TTL (a key that never expires).
     * KEYS[1] = timeline key, ARGV[1] = max entries, ARGV[2..] = members. Returns 1 if written, 0 if skipped.
     */
    private static final byte[] ADD_IF_EXISTS = """
            if redis.call('EXISTS', KEYS[1]) == 0 then
              return 0
            end
            for i = 2, #ARGV do
              redis.call('ZADD', KEYS[1], 0, ARGV[i])
            end
            redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[1]) + 1))
            return 1
            """.getBytes(StandardCharsets.UTF_8);

    private static final byte[] MAX_ENTRIES_ARG = bytes(String.valueOf(MAX_ENTRIES));

    private final StringRedisTemplate redis;

    public TimelineCacheRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * Fan-out: adds the tweet to each user's timeline that exists, in one pipelined round trip
     * (one EVAL per key, so keys may live on different Redis Cluster nodes). Users without a key are skipped:
     * their timeline is rebuilt from SQL when they next read it. Idempotent, so redelivered events are harmless.
     */
    public void addToTimelines(Collection<Long> userIds, long tweetId) {
        byte[] member = bytes(member(tweetId));
        redis.executePipelined((RedisCallback<Object>) connection -> {
            for (Long userId : userIds) {
                connection.scriptingCommands().eval(ADD_IF_EXISTS, ReturnType.INTEGER, 1,
                        bytes(key(userId)), MAX_ENTRIES_ARG, member);
            }
            return null;
        });
    }

    /**
     * Backfill: adds several tweets to one user's timeline if it exists. Members sort by id, so older tweets
     * land in their place by time; already present ids are no-ops.
     */
    public void addTweetsToTimeline(long userId, Collection<Long> tweetIds) {
        if (tweetIds.isEmpty()) {
            return;
        }
        byte[][] keysAndArgs = new byte[tweetIds.size() + 2][];
        keysAndArgs[0] = bytes(key(userId));
        keysAndArgs[1] = MAX_ENTRIES_ARG;
        int i = 2;
        for (Long id : tweetIds) {
            keysAndArgs[i++] = bytes(member(id));
        }
        redis.execute((RedisCallback<Object>) connection ->
                connection.scriptingCommands().eval(ADD_IF_EXISTS, ReturnType.INTEGER, 1, keysAndArgs));
    }

    /** ZREM: removes the given tweets from one user's timeline; ids not in it are ignored (idempotent). */
    public void removeFromTimeline(long userId, Collection<Long> tweetIds) {
        if (tweetIds.isEmpty()) {
            return;
        }
        redis.opsForZSet().remove(key(userId), tweetIds.stream().map(TimelineCacheRepository::member).toArray());
    }

    /**
     * Marks the timeline as just read: pushes its expiry {@link #TTL} into the future.
     *
     * @return false if the key does not exist (expired or never built); one EXPIRE answers both questions
     */
    public boolean touch(long userId) {
        return Boolean.TRUE.equals(redis.expire(key(userId), TTL));
    }

    /**
     * Creates the timeline from ids computed in SQL, with a TTL. An empty list creates nothing
     * (Redis has no empty sorted sets): such a user simply reads from SQL until there is something to cache.
     */
    public void rebuild(long userId, List<Long> tweetIds) {
        if (tweetIds.isEmpty()) {
            return;
        }
        String key = key(userId);
        Set<ZSetOperations.TypedTuple<String>> members = new HashSet<>();
        for (Long id : tweetIds) {
            members.add(ZSetOperations.TypedTuple.of(member(id), 0d));
        }
        redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> operations) {
                var ops = (RedisOperations<String, String>) operations;
                ops.opsForZSet().add(key, members);
                ops.expire(key, TTL);
                return null;
            }
        });
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

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
