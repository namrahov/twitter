package az.twitter.service;

import az.twitter.dto.cache.CachedPost;
import az.twitter.dto.cache.CelebrityPostsSnapshot;
import az.twitter.dto.response.CursorPage;
import az.twitter.dto.response.PostResponse;
import az.twitter.entity.PostEntity;
import az.twitter.exception.CacheUnavailableException;
import az.twitter.repository.CelebrityPostsCacheRepository;
import az.twitter.repository.FollowRepository;
import az.twitter.repository.PostRepository;
import az.twitter.repository.SortedIdCache;
import az.twitter.repository.SortedIdCache.Read;
import az.twitter.repository.SortedIdCache.Source;
import az.twitter.repository.SortedIdCache.SourcePage;
import az.twitter.repository.TimelineCacheRepository;
import az.twitter.util.RedisGuard;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Limit;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Home timeline = hybrid fan-out, merged at read time:
 * <ul>
 *   <li>push part: the user's precomputed Redis timeline (own tweets + followed normal accounts);</li>
 *   <li>pull part: the recent tweets of each followed celebrity, from their shared posts list;</li>
 *   <li>SQL: whatever the caches cannot cover (deep pages, empty or trimmed caches).</li>
 * </ul>
 * Ids are Snowflake ids, so "newest first" is simply "largest id first" across all sources.
 */
@Service
public class TimelineService {

    private final TimelineCacheRepository timelines;
    private final CelebrityPostsCacheRepository celebrityPosts;
    private final FollowRepository follows;
    private final PostRepository posts;
    private final PostService postService;
    private final AsyncCache<Long, CelebrityPostsSnapshot> celebrityLocalCache;
    private final CelebrityPostsLoader celebrityLoader;
    private final RedisGuard redis;
    private final Bulkhead sqlFallbackBulkhead;

    public TimelineService(TimelineCacheRepository timelines, CelebrityPostsCacheRepository celebrityPosts,
                           FollowRepository follows, PostRepository posts, PostService postService,
                           AsyncCache<Long, CelebrityPostsSnapshot> celebrityPostsLocalCache,
                           CelebrityPostsLoader celebrityLoader, RedisGuard redis,
                           @Value("${twitter.timeline.sql-fallback-max-concurrent}") int sqlFallbackMaxConcurrent) {
        this.timelines = timelines;
        this.celebrityPosts = celebrityPosts;
        this.follows = follows;
        this.posts = posts;
        this.postService = postService;
        this.celebrityLocalCache = celebrityPostsLocalCache;
        this.celebrityLoader = celebrityLoader;
        this.redis = redis;
        // maxWaitDuration 0: when all permits are taken, fail immediately instead of queueing web threads.
        this.sqlFallbackBulkhead = Bulkhead.of("timeline-sql-fallback", BulkheadConfig.custom()
                .maxConcurrentCalls(sqlFallbackMaxConcurrent)
                .maxWaitDuration(Duration.ZERO)
                .build());
    }

    /**
     * Normally built from the caches. If Redis fails (or its circuit breaker is open), the page is built with
     * SQL instead — but only {@code sql-fallback-max-concurrent} at a time, so a Redis outage cannot turn into
     * a PostgreSQL outage; the rest get 503 at once.
     */
    public CursorPage<PostResponse> home(long userId, long beforeId, int limit) {
        try {
            return fromCaches(userId, beforeId, limit);
        } catch (CacheUnavailableException e) {
            return fromSqlFallback(userId, beforeId, limit);
        } catch (CompletionException e) { // thrown through the L1 async cache
            if (e.getCause() instanceof CacheUnavailableException) {
                return fromSqlFallback(userId, beforeId, limit);
            }
            throw e;
        }
    }

    private CursorPage<PostResponse> fromSqlFallback(long userId, long beforeId, int limit) {
        try {
            return sqlFallbackBulkhead.executeSupplier(() -> {
                var rows = posts.findHomeTimeline(userId, beforeId, Limit.of(limit + 1));
                return CursorPage.of(rows, limit, PostEntity::getId, PostResponse::from);
            });
        } catch (BulkheadFullException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Timeline temporarily unavailable, please retry");
        }
    }

    private CursorPage<PostResponse> fromCaches(long userId, long beforeId, int limit) {
        int wanted = limit + 1; // one extra row tells us whether a next page exists

        // Ids from all cache sources, newest first, deduplicated (a tweet pushed before its author became a
        // celebrity can be in both the timeline and the celebrity list).
        TreeSet<Long> candidates = new TreeSet<>(Comparator.reverseOrder());
        // Ids at or below this are not reliably covered by every cache (one of them was trimmed above it).
        long floor = Long.MIN_VALUE;

        List<SourcePage> pages = new ArrayList<>();

        // Push part: the user's own timeline. Per-user, so no stampede risk: a missing key (inactive user) is
        // simply rebuilt from SQL and read again.
        Read own = new Read(new Source(timelines, userId), beforeId, wanted);
        SourcePage ownPage = redis.call(() -> SortedIdCache.readPages(List.of(own)).getFirst());
        if (!ownPage.exists()) {
            List<Long> pushedIds = posts.findPushedTimelineIds(userId, TimelineCacheRepository.MAX_ENTRIES);
            redis.run(() -> timelines.rebuild(userId, pushedIds));
            ownPage = redis.call(() -> SortedIdCache.readPages(List.of(own)).getFirst());
        }
        pages.add(ownPage);

        // Pull part: one shared list per followed celebrity, through L1. Lists missing in L1 are loaded together
        // (one Redis pipeline); concurrent requests missing the same celebrity wait for that one load.
        List<Long> celebrityIds = follows.findCelebrityFolloweeIds(userId);
        Map<Long, CelebrityPostsSnapshot> snapshots = celebrityLocalCache
                .getAll(celebrityIds, (missing, executor) ->
                        CompletableFuture.completedFuture(celebrityLoader.load(missing)))
                .join();
        snapshots.forEach((celebrityId, snapshot) ->
                pages.add(snapshot.page(new Source(celebrityPosts, celebrityId), beforeId, wanted)));

        for (SourcePage page : pages) {
            candidates.addAll(page.ids());
            floor = Math.max(floor, floorOf(page, beforeId, wanted));
        }

        long reliableAbove = floor;
        List<Long> ids = candidates.stream().filter(id -> id > reliableAbove).limit(wanted).toList();
        // Content: from the tweet:{id} cache, PostgreSQL only for misses.
        List<CachedPost> rows = new ArrayList<>(postService.getMany(ids));

        // SQL (the full JOIN over all followees) fills the rest: deep pages, trimmed or empty caches.
        if (ids.size() < wanted) {
            long sqlBefore = ids.isEmpty() ? beforeId : ids.getLast();
            posts.findHomeTimeline(userId, sqlBefore, Limit.of(wanted - ids.size()))
                    .forEach(p -> rows.add(CachedPost.from(p)));
        }
        return CursorPage.of(rows, limit, CachedPost::id, PostResponse::from);
    }

    /**
     * How far down one source's page can be trusted.
     *
     * <p>A source that returned a full page is fine: the merged page can never need anything older from it.
     * A source that returned less is fine too if it is complete (holds everything it should, i.e. was not trimmed).
     * Otherwise it was trimmed, and below its oldest returned id we cannot tell what is missing: merging older
     * ids from the other sources there could skip tweets. So report that id as a floor; below it, SQL takes over.
     *
     * @return the floor (ids at or below it are not covered), or {@code Long.MIN_VALUE} if there is none
     */
    private static long floorOf(SourcePage page, long beforeId, int wanted) {
        if (page.ids().size() == wanted || !page.trimmed()) {
            return Long.MIN_VALUE;
        }
        return page.ids().isEmpty() ? beforeId : page.ids().getLast() - 1;
    }
}
