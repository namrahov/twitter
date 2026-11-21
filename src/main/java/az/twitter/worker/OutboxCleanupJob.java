package az.twitter.worker;

import az.twitter.repository.OutboxQueueRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Keeps the outbox table from growing forever: deletes PUBLISHED events older than {@link #RETENTION_DAYS}.
 * Deletes in small batches so no single transaction locks many rows or bloats the table at once.
 * (At much larger scale: partition the table by day and DROP old partitions instead.)
 */
@Component
public class OutboxCleanupJob {

    private static final Logger log = LoggerFactory.getLogger(OutboxCleanupJob.class);

    /** Published events are kept this long for debugging / replay. */
    static final int RETENTION_DAYS = 7;
    static final int BATCH_SIZE = 10_000;
    /** Caps one run; whatever is left is deleted on the next run. */
    static final int MAX_BATCHES_PER_RUN = 100;

    private final OutboxQueueRepository outbox;

    public OutboxCleanupJob(OutboxQueueRepository outbox) {
        this.outbox = outbox;
    }

    @Scheduled(initialDelay = 30_000, fixedDelay = 600_000)
    public void deleteOldPublished() {
        int total = 0;
        for (int i = 0; i < MAX_BATCHES_PER_RUN; i++) {
            int deleted = outbox.deletePublishedOlderThan(RETENTION_DAYS, BATCH_SIZE);
            total += deleted;
            if (deleted < BATCH_SIZE) {
                break;
            }
        }
        if (total > 0) {
            log.info("Deleted {} published outbox events older than {} days", total, RETENTION_DAYS);
        }
    }
}
