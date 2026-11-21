package az.twitter.worker;

import az.twitter.config.KafkaConfig;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Moves dead letters back to their original topic so the normal workers process them again.
 *
 * <p>Not started automatically: an operator starts it (see AdminDltController) after fixing whatever made the
 * events fail, e.g. Redis being down. Its consumer group remembers what was already replayed, so starting it again
 * only replays new dead letters. Re-processing is safe because every worker is idempotent; order relative to
 * newer events does not matter (fan-out is ZADD, the follow worker re-checks the current follow state).
 */
@Component
public class DeadLetterReplayer {

    public static final String LISTENER_ID = "dlt-replay";

    private static final Logger log = LoggerFactory.getLogger(DeadLetterReplayer.class);
    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, String> kafka;

    public DeadLetterReplayer(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    @KafkaListener(id = LISTENER_ID, autoStartup = "false", groupId = "dlt-replay",
            topics = {KafkaConfig.TWEET_EVENTS_TOPIC + KafkaConfig.DLT_SUFFIX,
                    KafkaConfig.FOLLOW_EVENTS_TOPIC + KafkaConfig.DLT_SUFFIX})
    public void replay(ConsumerRecord<String, String> deadLetter) throws Exception {
        String originalTopic = deadLetter.topic().substring(0, deadLetter.topic().length() - KafkaConfig.DLT_SUFFIX.length());

        var record = new ProducerRecord<>(originalTopic, deadLetter.key(), deadLetter.value());
        for (Header header : deadLetter.headers()) {
            // Keep business headers (event-type); drop the failure details the DLT publisher added.
            if (!header.key().startsWith("kafka_dlt")) {
                record.headers().add(header);
            }
        }
        // Synchronous: if it fails, the error handler retries, and the offset is not committed.
        kafka.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        log.info("Replayed dead letter {}-{}@{} to {}",
                deadLetter.topic(), deadLetter.partition(), deadLetter.offset(), originalTopic);
    }
}
