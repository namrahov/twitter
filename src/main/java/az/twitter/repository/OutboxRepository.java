package az.twitter.repository;

import az.twitter.entity.OutboxEventEntity;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

/** Writes outbox rows (inside business transactions). Publishing and cleanup use {@link OutboxQueueRepository}. */
public interface OutboxRepository extends JpaRepository<OutboxEventEntity, UUID> {
}
