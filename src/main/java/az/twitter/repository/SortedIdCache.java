package az.twitter.repository;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
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
 * after the last read ({@link #readPages} refreshes it), and writers only add to keys that already exist. Unread keys expire
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

    /**
     * Placeholder member (id 0, sorts before every real id) that keeps a list's key existing when it has no ids,
     * so "nothing to show" is cached like anything else. Excluded from reads and counts. When a full list is
     * trimmed it is the first member to go, which is fine: the list is no longer empty then.
     */
    private static final String SENTINEL = member(0);
    private static final Range.Bound<String> ABOVE_SENTINEL = Range.Bound.exclusive(SENTINEL);
    private static final Range<String> ALL_IDS = Range.of(ABOVE_SENTINEL, Range.Bound.unbounded());

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

    /** Removes one id from each owner's list, in one pipelined round trip (one ZREM per key). Idempotent. */
    public void removeFromEach(Collection<Long> ownerIds, long id) {
        byte[] member = bytes(member(id));
        redis.executePipelined((RedisCallback<Object>) connection -> {
            for (Long ownerId : ownerIds) {
                connection.zSetCommands().zRem(bytes(key(ownerId)), member);
            }
            return null;
        });
    }

    /** ZREM; ids not in the list are ignored (idempotent). */
    public void removeAll(long ownerId, Collection<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        redis.opsForZSet().remove(key(ownerId), ids.stream().map(SortedIdCache::member).toArray());
    }

    /**
     * Creates the list from ids computed in SQL, with the TTL. Always with the {@link #SENTINEL}: Redis has no empty
     * sorted sets, so without it an empty result would create nothing, and every read would find the key missing
     * and query SQL again (e.g. a new user who follows nobody, on every refresh).
     */
    public void rebuild(long ownerId, List<Long> ids) {
        String key = key(ownerId);
        Set<ZSetOperations.TypedTuple<String>> members = new HashSet<>();
        members.add(ZSetOperations.TypedTuple.of(SENTINEL, 0d));
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

    /** One list to read: which cache, whose list. */
    public record Source(SortedIdCache cache, long ownerId) {
    }

    /**
     * What a read found.
     *
     * @param exists false if the key was missing (nothing was read; rebuild it and read again)
     * @param ids    ids older than the cursor, newest first
     * @param size   total ids held; equal to the cache's max means older ids were trimmed away
     */
    public record SourcePage(Source source, boolean exists, List<Long> ids, long size) {

        public boolean trimmed() {
            return size >= source.cache().maxEntries();
        }
    }

    /** One page to read: ids of {@code source} older than {@code beforeId}, at most {@code limit}. */
    public record Read(Source source, long beforeId, int limit) {
    }

    /**
     * Reads many pages, possibly from different caches, in <b>one</b> pipelined round trip:
     * per list EXPIRE (refresh the sliding TTL and learn whether it exists), ZREVRANGEBYLEX (the page) and
     * ZLEXCOUNT (how many real ids, i.e. without the sentinel).
     * All caches must use the same Redis.
     */
    public static List<SourcePage> readPages(List<Read> reads) {
        if (reads.isEmpty()) {
            return List.of();
        }
        // Through methods, not fields: the caches are Spring beans behind CGLIB proxies, whose own fields are null.
        StringRedisTemplate redis = reads.getFirst().source().cache().template();

        List<Object> replies = redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> operations) {
                var ops = (RedisOperations<String, String>) operations;
                for (Read read : reads) {
                    Source source = read.source();
                    String key = source.cache().keyOf(source.ownerId());
                    // Both ranges exclude the sentinel, so it is never returned or counted.
                    Range<String> range = read.beforeId() == Long.MAX_VALUE
                            ? ALL_IDS
                            : Range.of(ABOVE_SENTINEL, Range.Bound.exclusive(member(read.beforeId())));
                    ops.expire(key, source.cache().ttl());
                    ops.opsForZSet().reverseRangeByLex(key, range, Limit.limit().count(read.limit()));
                    ops.opsForZSet().lexCount(key, ALL_IDS);
                }
                return null;
            }
        });

        List<Source> sources = reads.stream().map(Read::source).toList();
        List<SourcePage> pages = new ArrayList<>(sources.size());
        for (int i = 0; i < sources.size(); i++) {
            boolean exists = Boolean.TRUE.equals(replies.get(3 * i));
            @SuppressWarnings("unchecked")
            Set<String> members = (Set<String>) replies.get(3 * i + 1);
            Long size = (Long) replies.get(3 * i + 2);
            List<Long> ids = members == null ? List.of() : members.stream().map(Long::parseLong).toList();
            pages.add(new SourcePage(sources.get(i), exists, ids, size == null ? 0 : size));
        }
        return pages;
    }

    private String key(long ownerId) {
        return keyPrefix + ownerId;
    }

    // Non-private so that, called on a Spring proxy, they run on the real object (see readPages).

    String keyOf(long ownerId) {
        return key(ownerId);
    }

    Duration ttl() {
        return ttl;
    }

    StringRedisTemplate template() {
        return redis;
    }

    static String member(long id) {
        return String.format("%019d", id);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
