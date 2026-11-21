package az.twitter.controller;

import az.twitter.dto.cache.CachedPost;
import az.twitter.dto.cache.CelebrityPostsSnapshot;

import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.github.benmanes.caffeine.cache.AsyncCache;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only look into this instance's L1 (Caffeine) caches, for learning and debugging. Every instance has its own
 * L1, so call each instance (port) separately to compare them. No auth yet, like the other admin endpoints.
 *
 * <p>Ids are returned as strings: Snowflake ids exceed JavaScript's safe integer range.
 */
@RestController
@RequestMapping("/admin/cache/l1")
public class AdminCacheController {

    private final AsyncCache<Long, CachedPost> tweetLocalCache;
    private final AsyncCache<Long, CelebrityPostsSnapshot> celebrityPostsLocalCache;
    private final String instance;

    public AdminCacheController(AsyncCache<Long, CachedPost> tweetLocalCache,
                                AsyncCache<Long, CelebrityPostsSnapshot> celebrityPostsLocalCache,
                                @Value("${server.port}") String port) {
        this.tweetLocalCache = tweetLocalCache;
        this.celebrityPostsLocalCache = celebrityPostsLocalCache;
        this.instance = "port " + port;
    }

    /**
     * @param limit at most this many entries per cache (newest tweet ids first); {@code size} is always the full count
     */
    @GetMapping
    public Map<String, Object> show(@RequestParam(defaultValue = "50") int limit) {
        // The synchronous view skips entries still being loaded.
        Map<Long, CachedPost> tweets = tweetLocalCache.synchronous().asMap();
        Map<Long, CelebrityPostsSnapshot> celebrities = celebrityPostsLocalCache.synchronous().asMap();

        List<TweetEntry> tweetEntries = tweets.values().stream()
                .sorted(Comparator.comparingLong(CachedPost::id).reversed())
                .limit(limit)
                .map(p -> new TweetEntry(String.valueOf(p.id()), String.valueOf(p.userId()), p.content(),
                        p.createdAt()))
                .toList();

        List<CelebrityEntry> celebrityEntries = celebrities.entrySet().stream()
                .limit(limit)
                .map(e -> new CelebrityEntry(String.valueOf(e.getKey()), e.getValue().size(),
                        e.getValue().ids().stream().map(String::valueOf).toList()))
                .toList();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("instance", instance);
        result.put("tweets", Map.of("size", tweets.size(), "entries", tweetEntries));
        result.put("celebrityPosts", Map.of("size", celebrities.size(), "entries", celebrityEntries));
        return result;
    }

    record TweetEntry(String id, String userId, String content, Instant createdAt) {
    }

    /** @param redisListSize size of the Redis list when it was copied here (can be above tweetIds' count if trimmed) */
    record CelebrityEntry(String celebrityId, long redisListSize, List<String> tweetIds) {
    }
}
