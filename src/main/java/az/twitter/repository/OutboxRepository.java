package az.twitter.repository;

import az.twitter.entity.OutboxEventEntity;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxEventEntity, UUID> {

    /**
     * Oldest pending events, row-locked until the caller's transaction ends.
     * SKIP LOCKED: a second app instance takes the next rows instead of waiting or publishing the same ones.
     * Uses the partial index idx_outbox_pending.
     */
    @Query(value = """
            SELECT * FROM outbox
            WHERE status = 'PENDING'
            ORDER BY created_at, event_id
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEventEntity> lockPendingBatch(@Param("limit") int limit);
}
