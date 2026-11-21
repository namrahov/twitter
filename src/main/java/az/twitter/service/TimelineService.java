package az.twitter.service;

import az.twitter.dto.cache.CachedPost;
import az.twitter.dto.cache.CelebrityPostsSnapshot;
import az.twitter.dto.response.CursorPage;
import az.twitter.dto.response.PostResponse;
import az.twitter.repository.CelebrityPostsCacheRepository;
import az.twitter.repository.FollowRepository;
import az.twitter.repository.PostRepository;
import az.twitter.repository.SortedIdCache;
import az.twitter.repository.SortedIdCache.Read;
import az.twitter.repository.SortedIdCache.Source;
import az.twitter.repository.SortedIdCache.SourcePage;
import az.twitter.repository.TimelineCacheRepository;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.github.benmanes.caffeine.cache.Cache;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;

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
    private final Cache<Long, CelebrityPostsSnapshot> celebrityLocalCache;

    public TimelineService(TimelineCacheRepository timelines, CelebrityPostsCacheRepository celebrityPosts,
                           FollowRepository follows, PostRepository posts, PostService postService,
                           Cache<Long, CelebrityPostsSnapshot> celebrityPostsLocalCache) {
        this.timelines = timelines;
        this.celebrityPosts = celebrityPosts;
        this.follows = follows;
        this.posts = posts;
        this.postService = postService;
        this.celebrityLocalCache = celebrityPostsLocalCache;
    }

    public CursorPage<PostResponse> home(long userId, long beforeId, int limit) {
        int wanted = limit + 1; // one extra row tells us whether a next page exists

        // Ids from all cache sources, newest first, deduplicated (a tweet pushed before its author became a
        // celebrity can be in both the timeline and the celebrity list).
        TreeSet<Long> candidates = new TreeSet<>(Comparator.reverseOrder());
        // Ids at or below this are not reliably covered by every cache (one of them was trimmed above it).
        long floor = Long.MIN_VALUE;

        // Sources: the user's own timeline (push part) + one shared list per followed celebrity (pull part).
        // Celebrity lists found in L1 (this JVM) are paged locally; the rest is read from Redis.
        Map<Source, SourcePage> pages = new LinkedHashMap<>();
        List<Read> reads = new ArrayList<>();
        reads.add(new Read(new Source(timelines, userId), beforeId, wanted));
        for (long celebrityId : follows.findCelebrityFolloweeIds(userId)) {
            Source source = new Source(celebrityPosts, celebrityId);
            CelebrityPostsSnapshot snapshot = celebrityLocalCache.getIfPresent(celebrityId);
            if (snapshot != null) {
                pages.put(source, snapshot.page(source, beforeId, wanted));
            } else {
                // Whole list, not just this page: the snapshot then serves every page and every user here.
                reads.add(new Read(source, Long.MAX_VALUE, CelebrityPostsCacheRepository.MAX_ENTRIES));
            }
        }

        // One Redis round trip for all of them (TTL refresh + page + size per list), however many celebrities.
        List<SourcePage> fromRedis = new ArrayList<>(SortedIdCache.readPages(reads));

        // Expired lists (inactive user, celebrity nobody read for a day): rebuild from SQL, then read just those
        // again — a second round trip only when something was missing.
        List<Read> rereads = new ArrayList<>();
        for (int i = 0; i < fromRedis.size(); i++) {
            if (!fromRedis.get(i).exists()) {
                rebuild(fromRedis.get(i).source());
                rereads.add(reads.get(i));
            }
        }
        fromRedis.removeIf(p -> !p.exists());
        fromRedis.addAll(SortedIdCache.readPages(rereads));

        for (SourcePage page : fromRedis) {
            if (page.source().cache() == celebrityPosts) {
                // A celebrity without posts is cached as an empty snapshot too: no rebuild query for 2 s.
                var snapshot = new CelebrityPostsSnapshot(page.ids(), page.size());
                celebrityLocalCache.put(page.source().ownerId(), snapshot);
                pages.put(page.source(), snapshot.page(page.source(), beforeId, wanted));
            } else {
                pages.put(page.source(), page);
            }
        }

        for (SourcePage page : pages.values()) {
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

    private void rebuild(Source source) {
        if (source.cache() == timelines) {
            timelines.rebuild(source.ownerId(),
                    posts.findPushedTimelineIds(source.ownerId(), TimelineCacheRepository.MAX_ENTRIES));
        } else {
            celebrityPosts.rebuild(source.ownerId(),
                    posts.findLatestIds(source.ownerId(), Limit.of(CelebrityPostsCacheRepository.MAX_ENTRIES)));
        }
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
