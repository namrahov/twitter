package az.twitter.dto.cache;

import az.twitter.repository.SortedIdCache.Source;
import az.twitter.repository.SortedIdCache.SourcePage;

import java.util.List;

/**
 * A whole celebrity posts list (all ids Redis holds for it, newest first) kept in the local L1 cache.
 * Pages are cut from it in memory, so every timeline read in this instance can use the same snapshot.
 *
 * @param ids  every id in the Redis list (at most CelebrityPostsCacheRepository.MAX_ENTRIES)
 * @param size the Redis list size, needed to tell whether older ids were trimmed away
 */
public record CelebrityPostsSnapshot(List<Long> ids, long size) {

    public SourcePage page(Source source, long beforeId, int limit) {
        List<Long> page = ids.stream().filter(id -> id < beforeId).limit(limit).toList();
        return new SourcePage(source, true, page, size);
    }
}
