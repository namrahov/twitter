package az.twitter.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** Ids of follow events already counted into users.follower_count (see FollowerCountService). */
@Repository
public class ProcessedEventRepository {

    private final JdbcTemplate jdbc;

    public ProcessedEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Records the ids in one JDBC batch; ids that are already there are left alone.
     *
     * @return the ids that were new, i.e. not processed before
     */
    public List<UUID> recordNew(List<UUID> eventIds) {
        if (eventIds.isEmpty()) {
            return List.of();
        }
        int[] inserted = jdbc.batchUpdate(
                "INSERT INTO follower_count_events (event_id) VALUES (?) ON CONFLICT (event_id) DO NOTHING",
                eventIds.stream().map(id -> new Object[] {id}).toList());
        List<UUID> fresh = new ArrayList<>();
        for (int i = 0; i < inserted.length; i++) {
            if (inserted[i] == 1) {
                fresh.add(eventIds.get(i));
            }
        }
        return fresh;
    }

    /**
     * Deletes ids older than the Kafka retention: such events can no longer be redelivered.
     *
     * @return rows deleted
     */
    public int deleteOlderThan(int days, int limit) {
        return jdbc.update("""
                DELETE FROM follower_count_events
                WHERE event_id IN (
                    SELECT event_id FROM follower_count_events
                    WHERE processed_at < now() - make_interval(days => ?)
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED)
                """, days, limit);
    }
}
