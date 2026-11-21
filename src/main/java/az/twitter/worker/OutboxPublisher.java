package az.twitter.worker;

import az.twitter.config.KafkaConfig;
import az.twitter.entity.OutboxEventEntity;
import az.twitter.repository.OutboxRepository;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Moves committed outbox rows to Kafka. Delivery is at-least-once: if the app dies after Kafka accepted
 * an event but before the PUBLISHED update commits, the event is sent again on the next run.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int BATCH_SIZE = 100;
    private static final long SEND_TIMEOUT_SECONDS = 5;

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;

    public OutboxPublisher(OutboxRepository outbox, KafkaTemplate<String, String> kafka) {
        this.outbox = outbox;
        this.kafka = kafka;
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publishPending() {
        List<OutboxEventEntity> batch = outbox.lockPendingBatch(BATCH_SIZE);
        for (OutboxEventEntity event : batch) {
            try {
                // Wait for the broker ack: only then is it safe to mark the row PUBLISHED.
                kafka.send(KafkaConfig.topicFor(event.getEventType()), String.valueOf(event.getUserId()),
                                event.getPayload())
                        .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                event.markPublished(Instant.now());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                event.recordFailedAttempt();
                log.warn("Outbox event {} not published (attempt {}): {}",
                        event.getId(), event.getAttempts(), e.getMessage());
                // Stop the batch: Kafka is likely down, and skipping ahead would reorder a user's events.
                return;
            }
        }
    }
}
