package az.twitter.config;

import az.twitter.dto.event.TweetCreatedEvent;
import az.twitter.dto.event.UserUnfollowedEvent;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    public static final String TWEET_CREATED_TOPIC = "tweet-created";
    public static final String USER_UNFOLLOWED_TOPIC = "user-unfollowed";

    /** Which topic an outbox row of a given event type goes to. */
    public static String topicFor(String eventType) {
        return switch (eventType) {
            case TweetCreatedEvent.TYPE -> TWEET_CREATED_TOPIC;
            case UserUnfollowedEvent.TYPE -> USER_UNFOLLOWED_TOPIC;
            default -> throw new IllegalArgumentException("No topic for event type " + eventType);
        };
    }

    /** Created on startup by KafkaAdmin if missing. 3 partitions = up to 3 fan-out consumers in parallel. */
    @Bean
    public NewTopic tweetCreatedTopic() {
        return TopicBuilder.name(TWEET_CREATED_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic userUnfollowedTopic() {
        return TopicBuilder.name(USER_UNFOLLOWED_TOPIC).partitions(3).replicas(1).build();
    }

    /**
     * Without this, a failed record is retried with no delay and then skipped. Retry for ~10 s instead
     * (covers a short Redis/DB blip); after that the record is logged and skipped. A skipped fan-out is not
     * data loss: the timeline read falls back to SQL. A dead-letter topic would be the next step.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1000, 9));
    }
}
