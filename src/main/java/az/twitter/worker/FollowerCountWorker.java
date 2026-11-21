package az.twitter.worker;

import az.twitter.config.KafkaConfig;
import az.twitter.dto.event.UserFollowedEvent;
import az.twitter.dto.event.UserUnfollowedEvent;
import az.twitter.service.FollowerCountService;
import az.twitter.service.FollowerCountService.FollowerDelta;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Counts followers from the follow events, a batch at a time. Its own consumer group, so it reads every
 * follow event independently of the timeline worker.
 *
 * <p>Batch listener: one poll returns up to max.poll.records (500) events. When follows are rare a batch has
 * one or two events; under a follow storm on one account it has hundreds, and they all become one UPDATE.
 */
@Component
public class FollowerCountWorker {

    private final FollowerCountService followerCounts;
    private final JsonMapper json;

    public FollowerCountWorker(FollowerCountService followerCounts, JsonMapper json) {
        this.followerCounts = followerCounts;
        this.json = json;
    }

    /**
     * "latest" only matters the first time this group starts: events already in the topic were counted by the
     * old synchronous code, so reading them again would double count. Afterwards the committed offsets are used.
     */
    @KafkaListener(topics = KafkaConfig.FOLLOW_EVENTS_TOPIC, groupId = "follower-count", batch = "true",
            properties = "auto.offset.reset=latest")
    public void onFollowEvents(List<ConsumerRecord<String, String>> records) {
        List<FollowerDelta> deltas = new ArrayList<>(records.size());
        for (ConsumerRecord<String, String> record : records) {
            switch (eventType(record)) {
                case UserFollowedEvent.TYPE -> {
                    var e = json.readValue(record.value(), UserFollowedEvent.class);
                    deltas.add(new FollowerDelta(e.eventId(), e.followeeId(), +1));
                }
                case UserUnfollowedEvent.TYPE -> {
                    var e = json.readValue(record.value(), UserUnfollowedEvent.class);
                    deltas.add(new FollowerDelta(e.eventId(), e.followeeId(), -1));
                }
                default -> { /* not a follow change */ }
            }
        }
        if (!deltas.isEmpty()) {
            followerCounts.apply(deltas);
        }
    }

    private static String eventType(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(KafkaConfig.EVENT_TYPE_HEADER);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
