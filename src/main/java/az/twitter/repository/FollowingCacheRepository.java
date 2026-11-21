package az.twitter.repository;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

/**
 * Whom a user follows: {@code following:user:{id}} → Redis set of followee ids. Read on every home timeline
 * request, to know which followed accounts are celebrities (pull part) and to drop tweets of accounts the user
 * no longer follows — without a PostgreSQL query per request.
 *
 * <p>Same lifecycle as the timeline: created only when read and missing ({@link #rebuild}), sliding TTL,
 * follow/unfollow events change only keys that exist. A sentinel member keeps the key alive when the user follows
 * nobody, so "follows nobody" is cached too instead of meaning "not cached".
 */
@Repository
public class FollowingCacheRepository {

    public static final Duration TTL = TimelineCacheRepository.TTL;

    private static final String SENTINEL = "-";

    /** SADD only if the key exists (else it would be created without the full list and without a TTL). */
    private static final byte[] ADD_IF_EXISTS = """
            if redis.call('EXISTS', KEYS[1]) == 1 then
              return redis.call('SADD', KEYS[1], ARGV[1])
            end
            return 0
            """.getBytes(StandardCharsets.UTF_8);

    private final StringRedisTemplate redis;

    public FollowingCacheRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** EXPIRE (refresh TTL, learn whether it exists) + SMEMBERS in one round trip; empty if not cached. */
    public Optional<Set<Long>> read(long userId) {
        String key = key(userId);
        List<Object> replies = redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> operations) {
                var ops = (RedisOperations<String, String>) operations;
                ops.expire(key, TTL);
                ops.opsForSet().members(key);
                return null;
            }
        });
        if (!Boolean.TRUE.equals(replies.get(0))) {
            return Optional.empty();
        }
        @SuppressWarnings("unchecked")
        Set<String> members = (Set<String>) replies.get(1);
        Set<Long> ids = new HashSet<>();
        if (members != null) {
            for (String member : members) {
                if (!SENTINEL.equals(member)) {
                    ids.add(Long.parseLong(member));
                }
            }
        }
        return Optional.of(ids);
    }

    public void rebuild(long userId, List<Long> followeeIds) {
        String key = key(userId);
        String[] members = new String[followeeIds.size() + 1];
        members[0] = SENTINEL;
        for (int i = 0; i < followeeIds.size(); i++) {
            members[i + 1] = String.valueOf(followeeIds.get(i));
        }
        redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> operations) {
                var ops = (RedisOperations<String, String>) operations;
                ops.opsForSet().add(key, members);
                ops.expire(key, TTL);
                return null;
            }
        });
    }

    public void addIfExists(long userId, long followeeId) {
        byte[] key = bytes(key(userId));
        byte[] member = bytes(String.valueOf(followeeId));
        redis.execute((RedisCallback<Object>) connection ->
                connection.scriptingCommands().eval(ADD_IF_EXISTS, ReturnType.INTEGER, 1, key, member));
    }

    public void remove(long userId, long followeeId) {
        redis.opsForSet().remove(key(userId), String.valueOf(followeeId));
    }

    private static String key(long userId) {
        return "following:user:" + userId;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
