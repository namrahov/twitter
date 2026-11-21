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

/**
 * A capped, newest-first list of tweet ids per owner, stored as a Redis sorted set {@code <prefix><ownerId>}.
 *
 * <p>Every member has score 0 and is the tweet id zero-padded to 19 digits. With equal scores Redis orders
 * members lexicographically, and fixed-width digits sort lexicographically exactly as they do numerically,
 * so the set is ordered by Snowflake id = by time, deterministically, and cursor paging is ZREVRANGEBYLEX.
 * (A timestamp score would tie within one millisecond; the full id does not fit a double's 53 bits.)
 *
 * <p>Lifecycle: a key is created only by {@link #rebuild} (when it is read and missing), lives for the TTL
 * after the last read ({@link #touch}), and writers only add to keys that already exist. Unread keys expire
 * and stop costing memory and writes.
 */
public abstract class SortedIdCache {

    /**
     * ZADD + trim, but only if the key exists. Done in Lua because "check, then write" from the app is not atomic:
     * the key could expire in between, and the ZADD would recreate it without a TTL (a key that never expires).
     * KEYS[1] = key, ARGV[1] = max entries, ARGV[2..] = members. Returns 1 if written, 0 if skipped.
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

    private final StringRedisTemplate redis;
    private final String keyPrefix;
    private final int maxEntries;
    private final Duration ttl;
    private final byte[] maxEntriesArg;

    protected SortedIdCache(StringRedisTemplate redis, String keyPrefix, int maxEntries, Duration ttl) {
        this.redis = redis;
        this.keyPrefix = keyPrefix;
        this.maxEntries = maxEntries;
        this.ttl = ttl;
        this.maxEntriesArg = bytes(String.valueOf(maxEntries));
    }

    public int maxEntries() {
        return maxEntries;
    }

    /**
     * Adds one id to each owner's list that exists, in one pipelined round trip (one EVAL per key, so keys may
     * live on different Redis Cluster nodes). Idempotent, so redelivered events are harmless.
     */
    public void addToEach(Collection<Long> ownerIds, long id) {
        byte[] member = bytes(member(id));
        redis.executePipelined((RedisCallback<Object>) connection -> {
            for (Long ownerId : ownerIds) {
                connection.scriptingCommands().eval(ADD_IF_EXISTS, ReturnType.INTEGER, 1,
                        bytes(key(ownerId)), maxEntriesArg, member);
            }
            return null;
        });
    }

    /** Adds several ids to one owner's list if it exists; older ids land in their place by time. */
    public void addAll(long ownerId, Collection<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        byte[][] keysAndArgs = new byte[ids.size() + 2][];
        keysAndArgs[0] = bytes(key(ownerId));
        keysAndArgs[1] = maxEntriesArg;
        int i = 2;
        for (Long id : ids) {
            keysAndArgs[i++] = bytes(member(id));
        }
        redis.execute((RedisCallback<Object>) connection ->
                connection.scriptingCommands().eval(ADD_IF_EXISTS, ReturnType.INTEGER, 1, keysAndArgs));
    }

    /** ZREM; ids not in the list are ignored (idempotent). */
    public void removeAll(long ownerId, Collection<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        redis.opsForZSet().remove(key(ownerId), ids.stream().map(SortedIdCache::member).toArray());
    }

    /**
     * Marks the list as just read: pushes its expiry one TTL into the future.
     *
     * @return false if the key does not exist; one EXPIRE answers both questions
     */
    public boolean touch(long ownerId) {
        return Boolean.TRUE.equals(redis.expire(key(ownerId), ttl));
    }

    /**
     * Creates the list from ids computed in SQL, with the TTL. An empty list creates nothing
     * (Redis has no empty sorted sets): reads then fall back to SQL until there is something to cache.
     */
    public void rebuild(long ownerId, List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        String key = key(ownerId);
        Set<ZSetOperations.TypedTuple<String>> members = new HashSet<>();
        for (Long id : ids) {
            members.add(ZSetOperations.TypedTuple.of(member(id), 0d));
        }
        redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> operations) {
                var ops = (RedisOperations<String, String>) operations;
                ops.opsForZSet().add(key, members);
                ops.expire(key, ttl);
                return null;
            }
        });
    }

    /** Ids older than {@code beforeId}, newest first. */
    public List<Long> page(long ownerId, long beforeId, int limit) {
        Range<String> range = beforeId == Long.MAX_VALUE
                ? Range.unbounded()
                : Range.leftUnbounded(Range.Bound.exclusive(member(beforeId)));
        Set<String> members = redis.opsForZSet()
                .reverseRangeByLex(key(ownerId), range, Limit.limit().count(limit));
        return members == null ? List.of() : members.stream().map(Long::parseLong).toList();
    }

    /** Number of ids held; equal to {@link #maxEntries()} means older ids were trimmed away. */
    public long size(long ownerId) {
        Long size = redis.opsForZSet().zCard(key(ownerId));
        return size == null ? 0 : size;
    }

    private String key(long ownerId) {
        return keyPrefix + ownerId;
    }

    static String member(long id) {
        return String.format("%019d", id);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
