package az.twitter.worker;

import az.twitter.repository.ProcessedEventRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Deletes old ids from follower_count_events in small batches. An id only has to be kept while its event can
 * still arrive again: Kafka keeps events 7 days by default, and a DLT replay re-sends them later still, so ids
 * are kept for twice that.
 */
@Component
public class ProcessedEventCleanupJob {

    private static final Logger log = LoggerFactory.getLogger(ProcessedEventCleanupJob.class);

    static final int RETENTION_DAYS = 14;
    static final int BATCH_SIZE = 10_000;
    static final int MAX_BATCHES_PER_RUN = 100;

    private final ProcessedEventRepository processedEvents;

    public ProcessedEventCleanupJob(ProcessedEventRepository processedEvents) {
        this.processedEvents = processedEvents;
    }

    @Scheduled(initialDelay = 60_000, fixedDelay = 3_600_000)
    public void deleteOld() {
        int total = 0;
        for (int i = 0; i < MAX_BATCHES_PER_RUN; i++) {
            int deleted = processedEvents.deleteOlderThan(RETENTION_DAYS, BATCH_SIZE);
            total += deleted;
            if (deleted < BATCH_SIZE) {
                break;
            }
        }
        if (total > 0) {
            log.info("Deleted {} processed follow event ids older than {} days", total, RETENTION_DAYS);
        }
    }
}
