package az.twitter.repository;

import az.twitter.dto.cache.CachedPost;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

/**
 * Tweet content by id: {@code tweet:{id}} → JSON. Timeline caches hold only ids; this turns a page of ids into
 * tweets without touching PostgreSQL. A celebrity tweet read by millions is one key, read from Redis, not
 * millions of DB queries.
 */
@Repository
public class PostContentCacheRepository {

    /**
     * Long TTL: an edit or delete removes the entry explicitly ({@link #delete}), so the TTL only bounds memory
     * (and is the last safety net should a removal be missed); LRU evicts cold ones earlier.
     */
    static final Duration TTL = Duration.ofHours(24);

    private final StringRedisTemplate redis;
    private final JsonMapper json;

    public PostContentCacheRepository(StringRedisTemplate redis, JsonMapper json) {
        this.redis = redis;
        this.json = json;
    }

    /** One MGET for the whole page. Missing ids are simply absent from the result. */
    public Map<Long, CachedPost> getAll(List<Long> ids) {
        Map<Long, CachedPost> found = new HashMap<>();
        if (ids.isEmpty()) {
            return found;
        }
        List<String> values = redis.opsForValue().multiGet(ids.stream().map(PostContentCacheRepository::key).toList());
        if (values == null) {
            return found;
        }
        for (int i = 0; i < ids.size(); i++) {
            String value = values.get(i);
            if (value != null) {
                found.put(ids.get(i), json.readValue(value, CachedPost.class));
            }
        }
        return found;
    }

    /** SET ... EX for each post, in one pipelined round trip. */
    public void putAll(Collection<CachedPost> posts) {
        if (posts.isEmpty()) {
            return;
        }
        redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> operations) {
                var ops = (RedisOperations<String, String>) operations;
                for (CachedPost post : posts) {
                    ops.opsForValue().set(key(post.id()), json.writeValueAsString(post), TTL);
                }
                return null;
            }
        });
    }

    /** The next read loads the current row from PostgreSQL (or finds nothing, if the tweet was deleted). */
    public void delete(long id) {
        redis.delete(key(id));
    }

    private static String key(long id) {
        return "tweet:" + id;
    }
}
