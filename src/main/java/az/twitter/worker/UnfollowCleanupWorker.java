package az.twitter.worker;

import az.twitter.config.KafkaConfig;
import az.twitter.dto.event.UserUnfollowedEvent;
import az.twitter.entity.FollowId;
import az.twitter.repository.FollowRepository;
import az.twitter.repository.PostRepository;
import az.twitter.repository.TimelineCacheRepository;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Removes an unfollowed user's tweets from the follower's Redis timeline.
 */
@Component
public class UnfollowCleanupWorker {

    private static final Logger log = LoggerFactory.getLogger(UnfollowCleanupWorker.class);

    private final FollowRepository follows;
    private final PostRepository posts;
    private final TimelineCacheRepository timelines;
    private final JsonMapper json;

    public UnfollowCleanupWorker(FollowRepository follows, PostRepository posts,
                                 TimelineCacheRepository timelines, JsonMapper json) {
        this.follows = follows;
        this.posts = posts;
        this.timelines = timelines;
        this.json = json;
    }

    /**
     * Only the followee's newest MAX_ENTRIES tweets are candidates: a timeline keeps its newest MAX_ENTRIES ids,
     * and if an older followee tweet were still in it, every newer followee tweet would be too, so the
     * followee's tweets in the timeline are always among the followee's newest MAX_ENTRIES.
     * Idempotent (ZREM of missing ids is a no-op), so redelivery is harmless.
     */
    @KafkaListener(topics = KafkaConfig.USER_UNFOLLOWED_TOPIC, groupId = "timeline-cleanup")
    public void onUserUnfollowed(String payload) {
        UserUnfollowedEvent event = json.readValue(payload, UserUnfollowedEvent.class);

        // Followed again before this event was processed: keep the tweets.
        if (follows.existsById(new FollowId(event.followerId(), event.followeeId()))) {
            log.debug("Skip cleanup: {} follows {} again", event.followerId(), event.followeeId());
            return;
        }

        List<Long> tweetIds = posts.findLatestIds(event.followeeId(), Limit.of(TimelineCacheRepository.MAX_ENTRIES));
        timelines.removeFromTimeline(event.followerId(), tweetIds);
        log.debug("Removed up to {} tweets of {} from timeline of {}",
                tweetIds.size(), event.followeeId(), event.followerId());
    }
}
