package az.twitter.worker;

import az.twitter.config.KafkaConfig;
import az.twitter.dto.event.TweetCreatedEvent;
import az.twitter.repository.FollowRepository;
import az.twitter.repository.TimelineCacheRepository;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Fan-out on write: pushes a new tweet id into the author's and every follower's Redis timeline.
 * Lives in this app for now; it only talks to Kafka, Postgres and Redis, so it can move to its own service later.
 */
@Component
public class FanoutWorker {

    private static final Logger log = LoggerFactory.getLogger(FanoutWorker.class);
    private static final int FOLLOWER_BATCH = 1000;

    private final FollowRepository follows;
    private final TimelineCacheRepository timelines;
    private final JsonMapper json;

    public FanoutWorker(FollowRepository follows, TimelineCacheRepository timelines, JsonMapper json) {
        this.follows = follows;
        this.timelines = timelines;
        this.json = json;
    }

    /**
     * Safe to run twice for the same event (redelivery, or a crash halfway through): every write is an idempotent ZADD,
     * so the retry simply redoes all batches. On an exception Kafka does not commit the offset and redelivers.
     */
    @KafkaListener(topics = KafkaConfig.TWEET_CREATED_TOPIC, groupId = "fanout")
    public void onTweetCreated(String payload) {
        TweetCreatedEvent event = json.readValue(payload, TweetCreatedEvent.class);

        timelines.addToTimelines(List.of(event.authorId()), event.tweetId());

        long afterId = 0;
        int total = 0;
        while (true) {
            List<Long> followerIds = follows.findFollowerIds(event.authorId(), afterId, Limit.of(FOLLOWER_BATCH));
            if (followerIds.isEmpty()) {
                break;
            }
            timelines.addToTimelines(followerIds, event.tweetId());
            total += followerIds.size();
            if (followerIds.size() < FOLLOWER_BATCH) {
                break;
            }
            afterId = followerIds.getLast();
        }
        log.debug("Fanned out tweet {} to {} followers", event.tweetId(), total);
    }
}
