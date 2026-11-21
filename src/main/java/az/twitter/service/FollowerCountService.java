package az.twitter.service;

import az.twitter.repository.ProcessedEventRepository;
import az.twitter.repository.UserRepository;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Keeps users.follower_count (and the celebrity flag) up to date asynchronously, off the follow request path.
 *
 * <p>Why: updating the counter inside every follow transaction makes all follows of one popular account queue on
 * that single users row. Here a whole batch of follow events becomes one UPDATE per account ("+1500"), so the row
 * is locked once per batch instead of once per follow. The count lags real follows by about a second.
 */
@Service
public class FollowerCountService {

    /** One follow (+1) or unfollow (-1) of {@code followeeId}. */
    public record FollowerDelta(UUID eventId, long followeeId, int delta) {
    }

    private static final Logger log = LoggerFactory.getLogger(FollowerCountService.class);

    private final ProcessedEventRepository processedEvents;
    private final UserRepository users;
    private final long celebrityThreshold;

    public FollowerCountService(ProcessedEventRepository processedEvents, UserRepository users,
                                @Value("${twitter.timeline.celebrity-threshold}") long celebrityThreshold) {
        this.processedEvents = processedEvents;
        this.users = users;
        this.celebrityThreshold = celebrityThreshold;
    }

    /**
     * Applies a batch exactly once: event ids are recorded in the same transaction as the counter updates, so a
     * redelivered batch (crash after commit, before Kafka offsets were committed) finds its ids and changes nothing.
     */
    @Transactional
    public void apply(List<FollowerDelta> deltas) {
        List<UUID> fresh = processedEvents.recordNew(deltas.stream().map(FollowerDelta::eventId).toList());
        if (fresh.size() < deltas.size()) {
            log.info("Skipped {} already counted follow events", deltas.size() - fresh.size());
        }
        Set<UUID> freshIds = new HashSet<>(fresh);

        // TreeMap: accounts are updated in id order, so two instances updating overlapping accounts
        // lock rows in the same order and cannot deadlock.
        Map<Long, Long> perFollowee = new TreeMap<>();
        for (FollowerDelta d : deltas) {
            if (freshIds.contains(d.eventId())) {
                perFollowee.merge(d.followeeId(), (long) d.delta(), Long::sum);
            }
        }
        perFollowee.forEach((followeeId, delta) -> {
            if (delta != 0) {
                users.applyFollowerDelta(followeeId, delta, celebrityThreshold);
            }
        });
    }
}
