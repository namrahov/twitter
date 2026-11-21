package az.twitter.service;

import az.twitter.dto.cache.CelebrityPostsSnapshot;
import az.twitter.repository.CelebrityPostsCacheRepository;
import az.twitter.repository.PostRepository;
import az.twitter.repository.RebuildLockRepository;
import az.twitter.repository.SortedIdCache;
import az.twitter.repository.SortedIdCache.Read;
import az.twitter.repository.SortedIdCache.Source;
import az.twitter.repository.SortedIdCache.SourcePage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;

/**
 * Loads whole celebrity posts lists for the L1 cache, with cache stampede protection: when a list is missing
 * in Redis, only one request in the whole cluster rebuilds it from PostgreSQL (Redis lock + double-check);
 * the others wait briefly for that rebuild instead of all querying the DB at once.
 */
@Service
public class CelebrityPostsLoader {

    private static final Logger log = LoggerFactory.getLogger(CelebrityPostsLoader.class);

    private final CelebrityPostsCacheRepository celebrityPosts;
    private final PostRepository posts;
    private final RebuildLockRepository locks;

    public CelebrityPostsLoader(CelebrityPostsCacheRepository celebrityPosts, PostRepository posts,
                                RebuildLockRepository locks) {
        this.celebrityPosts = celebrityPosts;
        this.posts = posts;
        this.locks = locks;
    }

    /**
     * @return a snapshot per celebrity; a celebrity whose list another request is still rebuilding after the
     * short wait is left out (the caller shows the page without it this time, rather than hitting the DB)
     */
    public Map<Long, CelebrityPostsSnapshot> load(Set<? extends Long> celebrityIds) {
        Map<Long, CelebrityPostsSnapshot> result = new HashMap<>();
        List<Long> missing = readInto(celebrityIds, result); // one pipelined round trip for all

        List<Long> waiting = new ArrayList<>();
        for (Long celebrityId : missing) {
            Optional<String> token = locks.tryAcquire(lockName(celebrityId));
            if (token.isEmpty()) {
                waiting.add(celebrityId);
                continue;
            }
            try {
                // Double-check: another request may have rebuilt it between our read and our lock.
                if (readInto(List.of(celebrityId), result).isEmpty()) {
                    continue;
                }
                List<Long> ids = posts.findLatestIds(celebrityId, Limit.of(CelebrityPostsCacheRepository.MAX_ENTRIES));
                celebrityPosts.rebuild(celebrityId, ids);
                result.put(celebrityId, new CelebrityPostsSnapshot(ids, ids.size()));
            } finally {
                locks.release(lockName(celebrityId), token.get());
            }
        }

        for (int attempt = 0; attempt < StampedeWait.ATTEMPTS && !waiting.isEmpty(); attempt++) {
            StampedeWait.pause();
            waiting = readInto(waiting, result);
        }
        if (!waiting.isEmpty()) {
            log.info("Celebrity lists {} still being rebuilt elsewhere; served without them", waiting);
        }
        return result;
    }

    /** Reads the whole lists of {@code celebrityIds} into {@code result}; returns the ids whose key is missing. */
    private List<Long> readInto(Collection<? extends Long> celebrityIds, Map<Long, CelebrityPostsSnapshot> result) {
        List<Read> reads = celebrityIds.stream()
                .map(id -> new Read(new Source(celebrityPosts, id), Long.MAX_VALUE,
                        CelebrityPostsCacheRepository.MAX_ENTRIES))
                .toList();
        List<Long> missing = new ArrayList<>();
        for (SourcePage page : SortedIdCache.readPages(reads)) {
            long celebrityId = page.source().ownerId();
            if (page.exists()) {
                result.put(celebrityId, new CelebrityPostsSnapshot(page.ids(), page.size()));
            } else {
                missing.add(celebrityId);
            }
        }
        return missing;
    }

    private static String lockName(long celebrityId) {
        return "rebuild:posts:user:" + celebrityId;
    }
}
