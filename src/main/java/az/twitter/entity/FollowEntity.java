package az.twitter.entity;

import az.twitter.repository.FollowRepository;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import org.hibernate.annotations.Generated;

/**
 * Mapped for queries; rows are written with a native INSERT ... ON CONFLICT DO NOTHING (see FollowRepository).
 */
@Entity
@Table(name = "follows")
public class FollowEntity {

    @EmbeddedId
    private FollowId id;

    @Generated
    @Column(name = "created_at", insertable = false, updatable = false)
    private Instant createdAt;

    protected FollowEntity() {
    }

    public FollowId getId() {
        return id;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
