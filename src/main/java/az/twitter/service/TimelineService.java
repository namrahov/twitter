package az.twitter.service;

import az.twitter.dto.response.CursorPage;
import az.twitter.dto.response.PostResponse;
import az.twitter.entity.PostEntity;
import az.twitter.repository.PostRepository;
import az.twitter.repository.TimelineCacheRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;

@Service
public class TimelineService {

    private final TimelineCacheRepository timelines;
    private final PostRepository posts;

    public TimelineService(TimelineCacheRepository timelines, PostRepository posts) {
        this.timelines = timelines;
        this.posts = posts;
    }

    /**
     * Redis first (precomputed by FanoutWorker); whatever Redis cannot supply comes from the SQL join:
     * an empty timeline, or pages older than the newest {@link TimelineCacheRepository#MAX_ENTRIES}.
     *
     * <p>Every read refreshes the key's TTL. If the key is gone (user was inactive, so it expired and fan-out
     * stopped writing to it), it is rebuilt from SQL first: the first read after a break is slower, later ones fast.
     */
    public CursorPage<PostResponse> home(long userId, long beforeId, int limit) {
        if (!timelines.touch(userId)) {
            timelines.rebuild(userId,
                    posts.findHomeTimelineIds(userId, Limit.of(TimelineCacheRepository.MAX_ENTRIES)));
        }

        int wanted = limit + 1; // one extra row tells us whether a next page exists

        List<Long> cachedIds = timelines.page(userId, beforeId, wanted);
        List<PostEntity> rows = new ArrayList<>(loadInOrder(cachedIds));

        if (cachedIds.size() < wanted) {
            long sqlBefore = cachedIds.isEmpty() ? beforeId : cachedIds.getLast();
            rows.addAll(posts.findHomeTimeline(userId, sqlBefore, Limit.of(wanted - cachedIds.size())));
        }
        return CursorPage.of(rows, limit, PostEntity::getId, PostResponse::from);
    }

    /** One batched query for all ids, then back into the timeline's order. Deleted posts are skipped. */
    private List<PostEntity> loadInOrder(List<Long> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }
        Map<Long, PostEntity> byId = posts.findAllById(ids).stream()
                .collect(Collectors.toMap(PostEntity::getId, Function.identity()));
        return ids.stream().map(byId::get).filter(p -> p != null).toList();
    }
}
