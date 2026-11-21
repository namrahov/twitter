package az.twitter.service;

import az.twitter.dto.cache.CachedPost;
import az.twitter.dto.response.CursorPage;
import az.twitter.dto.response.PostResponse;
import az.twitter.repository.CelebrityPostsCacheRepository;
import az.twitter.repository.FollowRepository;
import az.twitter.repository.PostRepository;
import az.twitter.repository.SortedIdCache;
import az.twitter.repository.TimelineCacheRepository;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

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

    public TimelineService(TimelineCacheRepository timelines, CelebrityPostsCacheRepository celebrityPosts,
                           FollowRepository follows, PostRepository posts, PostService postService) {
        this.timelines = timelines;
        this.celebrityPosts = celebrityPosts;
        this.follows = follows;
        this.posts = posts;
        this.postService = postService;
    }

    public CursorPage<PostResponse> home(long userId, long beforeId, int limit) {
        int wanted = limit + 1; // one extra row tells us whether a next page exists

        // Ids from all cache sources, newest first, deduplicated (a tweet pushed before its author became a
        // celebrity can be in both the timeline and the celebrity list).
        TreeSet<Long> candidates = new TreeSet<>(Comparator.reverseOrder());
        // Ids at or below this are not reliably covered by every cache (one of them was trimmed above it).
        long floor = Long.MIN_VALUE;

        // Push part. Every read refreshes the TTL; a missing key (inactive user) is rebuilt from SQL first.
        if (!timelines.touch(userId)) {
            timelines.rebuild(userId, posts.findPushedTimelineIds(userId, TimelineCacheRepository.MAX_ENTRIES));
        }
        floor = Math.max(floor, collect(timelines, userId, beforeId, wanted, candidates));

        // Pull part: one shared list per followed celebrity (cache-aside: rebuilt from SQL when missing).
        for (long celebrityId : follows.findCelebrityFolloweeIds(userId)) {
            if (!celebrityPosts.touch(celebrityId)) {
                celebrityPosts.rebuild(celebrityId,
                        posts.findLatestIds(celebrityId, Limit.of(CelebrityPostsCacheRepository.MAX_ENTRIES)));
            }
            floor = Math.max(floor, collect(celebrityPosts, celebrityId, beforeId, wanted, candidates));
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
     * Adds one source's page to the candidates.
     *
     * <p>A source that returned a full page is fine: the merged page can never need anything older from it.
     * A source that returned less is fine too if it is complete (holds everything it should, i.e. was not trimmed).
     * Otherwise it was trimmed, and below its oldest returned id we cannot tell what is missing: merging older
     * ids from the other sources there could skip tweets. So report that id as a floor; below it, SQL takes over.
     *
     * @return the floor (ids at or below it are not covered), or {@code Long.MIN_VALUE} if there is none
     */
    private static long collect(SortedIdCache cache, long ownerId, long beforeId, int wanted, TreeSet<Long> into) {
        List<Long> page = cache.page(ownerId, beforeId, wanted);
        into.addAll(page);
        if (page.size() == wanted || cache.size(ownerId) < cache.maxEntries()) {
            return Long.MIN_VALUE;
        }
        return page.isEmpty() ? beforeId : page.getLast() - 1;
    }
}
