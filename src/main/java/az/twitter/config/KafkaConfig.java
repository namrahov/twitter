package az.twitter.config;

import az.twitter.dto.event.TweetCreatedEvent;
import az.twitter.dto.event.TweetDeletedEvent;
import az.twitter.dto.event.TweetEditedEvent;
import az.twitter.dto.event.UserFollowedEvent;
import az.twitter.dto.event.UserUnfollowedEvent;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;

@Configuration
public class KafkaConfig {

    /**
     * Created, edited and deleted tweets share one topic, keyed by author, so for one tweet they are handled in
     * order (a delete never overtakes the fan-out of its create). The name is from when it carried creates only;
     * kept so existing consumer offsets stay valid.
     */
    public static final String TWEET_EVENTS_TOPIC = "tweet-created";

    /**
     * Follow and unfollow share one topic, keyed by follower: Kafka keeps order only within a partition,
     * so with two topics a quick follow → unfollow could be processed as unfollow → follow.
     */
    public static final String FOLLOW_EVENTS_TOPIC = "follow-events";

    /** Kafka header carrying the outbox event type, so one listener can tell event kinds apart. */
    public static final String EVENT_TYPE_HEADER = "event-type";

    /** Which topic an outbox row of a given event type goes to. */
    public static String topicFor(String eventType) {
        return switch (eventType) {
            case TweetCreatedEvent.TYPE, TweetEditedEvent.TYPE, TweetDeletedEvent.TYPE -> TWEET_EVENTS_TOPIC;
            case UserFollowedEvent.TYPE, UserUnfollowedEvent.TYPE -> FOLLOW_EVENTS_TOPIC;
            default -> throw new IllegalArgumentException("No topic for event type " + eventType);
        };
    }

    /** Created on startup by KafkaAdmin if missing. 3 partitions = up to 3 fan-out consumers in parallel. */
    @Bean
    public NewTopic tweetCreatedTopic() {
        return TopicBuilder.name(TWEET_EVENTS_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic followEventsTopic() {
        return TopicBuilder.name(FOLLOW_EVENTS_TOPIC).partitions(3).replicas(1).build();
    }

    /** Same partition count as the source topics: a dead letter keeps its original partition (and key order). */
    @Bean
    public NewTopic tweetCreatedDltTopic() {
        return TopicBuilder.name(dltOf(TWEET_EVENTS_TOPIC)).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic followEventsDltTopic() {
        return TopicBuilder.name(dltOf(FOLLOW_EVENTS_TOPIC)).partitions(3).replicas(1).build();
    }

    public static final String DLT_SUFFIX = ".DLT";

    public static String dltOf(String topic) {
        return topic + DLT_SUFFIX;
    }

    /**
     * Retry a failed record for ~10 s (covers a short Redis/DB blip), then move it to {@code <topic>.DLT}
     * instead of dropping it, so it can be replayed once the problem is fixed (see DeadLetterReplayer).
     * Unparseable payloads go to the DLT at once: retrying cannot fix them.
     * Records failing in the replayer itself go back to the same DLT, not to a {@code .DLT.DLT}.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaOperations<String, String> kafka) {
        var recoverer = new DeadLetterPublishingRecoverer(kafka, (record, exception) -> new TopicPartition(
                record.topic().endsWith(DLT_SUFFIX) ? record.topic() : dltOf(record.topic()),
                record.partition()));
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000, 9));
        handler.addNotRetryableExceptions(JacksonException.class);
        return handler;
    }
}
