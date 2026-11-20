package az.twitter.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.hibernate.annotations.Generated;
import org.springframework.data.domain.Persistable;

@Entity
@Table(name = "users")
public class UserEntity implements Persistable<Long> {

    /** Assigned by Snowflake, not by the DB. */
    @Id
    private Long id;

    @Column(nullable = false, length = 50)
    private String username;

    @Column(name = "display_name", nullable = false, length = 100)
    private String displayName;

    /** Filled by the DB default; Hibernate reads it back after INSERT. */
    @Generated
    @Column(name = "created_at", insertable = false, updatable = false)
    private Instant createdAt;

    /**
     * Because the id is set before save(), Spring Data would treat the entity as existing and call merge(),
     * which issues an extra SELECT. This flag makes save() call persist() for new entities.
     */
    @Transient
    private boolean isNew = true;

    protected UserEntity() {
    }

    public UserEntity(long id, String username, String displayName) {
        this.id = id;
        this.username = username;
        this.displayName = displayName;
    }

    @PostPersist
    @PostLoad
    void markNotNew() {
        this.isNew = false;
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public String getUsername() {
        return username;
    }

    public String getDisplayName() {
        return displayName;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
