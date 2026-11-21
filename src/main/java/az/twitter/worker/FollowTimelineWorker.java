package az.twitter.worker;

import az.twitter.config.KafkaConfig;
import az.twitter.dto.event.UserFollowedEvent;
import az.twitter.dto.event.UserUnfollowedEvent;
import az.twitter.entity.FollowId;
import az.twitter.repository.FollowRepository;
import az.twitter.repository.FollowingCacheRepository;
import az.twitter.repository.PostRepository;
import az.twitter.repository.TimelineCacheRepository;
import az.twitter.repository.UserRepository;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Keeps a follower's Redis timeline and following set in line with follow changes:
 * follow → add to the following set, backfill the followee's recent tweets;
 * unfollow → remove from the following set, remove the followee's tweets.
 *
 * <p>Fan-out vs unfollow race: a fan-out that read the follower list just before the unfollow can still write the
 * followee's tweet into the timeline after this cleanup. That tweet stays in Redis, but timeline reads drop tweets
 * whose author is not in the following set (TimelineService), so it is never shown.
 *
 * <p>Both event types come through one topic keyed by follower, so for a given follower they are handled
 * strictly in the order they happened. Every step is idempotent (ZADD/ZREM), so redelivery is harmless.
 */
@Component
public class FollowTimelineWorker {

    private static final Logger log = LoggerFactory.getLogger(FollowTimelineWorker.class);

    /** Enough for the first pages; older tweets still come from the SQL fallback when paging. */
    static final int BACKFILL_SIZE = 50;

    private final FollowRepository follows;
    private final UserRepository users;
    private final PostRepository posts;
    private final TimelineCacheRepository timelines;
    private final FollowingCacheRepository following;
    private final JsonMapper json;

    public FollowTimelineWorker(FollowRepository follows, UserRepository users, PostRepository posts,
                                TimelineCacheRepository timelines, FollowingCacheRepository following,
                                JsonMapper json) {
        this.follows = follows;
        this.users = users;
        this.posts = posts;
        this.timelines = timelines;
        this.following = following;
        this.json = json;
    }

    @KafkaListener(topics = KafkaConfig.FOLLOW_EVENTS_TOPIC, groupId = "follow-timeline")
    public void onFollowEvent(String payload, @Header(KafkaConfig.EVENT_TYPE_HEADER) String eventType) {
        switch (eventType) {
            case UserFollowedEvent.TYPE -> onFollowed(json.readValue(payload, UserFollowedEvent.class));
            case UserUnfollowedEvent.TYPE -> onUnfollowed(json.readValue(payload, UserUnfollowedEvent.class));
            default -> log.warn("Ignoring unknown follow event type {}", eventType);
        }
    }

    private void onFollowed(UserFollowedEvent event) {
        // Unfollowed again before this ran: its UserUnfollowed event comes next anyway, skip the useless work.
        if (!isFollowing(event.followerId(), event.followeeId())) {
            return;
        }
        following.addIfExists(event.followerId(), event.followeeId());
        // Celebrity tweets are pulled at read time (via the following set), nothing to push.
        if (users.isCelebrity(event.followeeId())) {
            return;
        }
        List<Long> tweetIds = posts.findLatestIds(event.followeeId(), Limit.of(BACKFILL_SIZE));
        timelines.addAll(event.followerId(), tweetIds);
        log.debug("Backfilled {} tweets of {} into timeline of {}",
                tweetIds.size(), event.followeeId(), event.followerId());
    }

    /**
     * Only the followee's newest MAX_ENTRIES tweets are candidates: a timeline keeps its newest MAX_ENTRIES ids,
     * and if an older followee tweet were still in it, every newer followee tweet would be too.
     */
    private void onUnfollowed(UserUnfollowedEvent event) {
        // Followed again before this ran: keep the tweets.
        if (isFollowing(event.followerId(), event.followeeId())) {
            return;
        }
        // First: from now on reads drop this followee's tweets, even ones a late fan-out still writes (see below).
        following.remove(event.followerId(), event.followeeId());
        List<Long> tweetIds = posts.findLatestIds(event.followeeId(), Limit.of(TimelineCacheRepository.MAX_ENTRIES));
        // Also for celebrities: tweets pushed before they became one may still be in the timeline.
        timelines.removeAll(event.followerId(), tweetIds);
        log.debug("Removed up to {} tweets of {} from timeline of {}",
                tweetIds.size(), event.followeeId(), event.followerId());
    }

    private boolean isFollowing(long followerId, long followeeId) {
        return follows.existsById(new FollowId(followerId, followeeId));
    }
}
