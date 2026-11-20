package az.twitter.entity;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.hibernate.annotations.Generated;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.springframework.data.domain.Persistable;

@Entity
@Table(name = "outbox")
public class OutboxEventEntity implements Persistable<UUID> {

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    /** The tweet id. */
    @Column(name = "aggregate_id", nullable = false)
    private Long aggregateId;

    /** The author; used as the Kafka key so one author's events stay ordered in one partition. */
    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    /** Already-serialised JSON, sent to Kafka as is. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private OutboxStatus status = OutboxStatus.PENDING;

    @Column(nullable = false)
    private int attempts;

    @Generated
    @Column(name = "created_at", insertable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Transient
    private boolean isNew = true;

    protected OutboxEventEntity() {
    }

    public OutboxEventEntity(UUID eventId, long aggregateId, long userId, String eventType, String payload) {
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.userId = userId;
        this.eventType = eventType;
        this.payload = payload;
    }

    public void markPublished(Instant at) {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = at;
    }

    public void recordFailedAttempt() {
        this.attempts++;
    }

    @PostPersist
    @PostLoad
    void markNotNew() {
        this.isNew = false;
    }

    @Override
    public UUID getId() {
        return eventId;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public Long getUserId() {
        return userId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public int getAttempts() {
        return attempts;
    }
}
