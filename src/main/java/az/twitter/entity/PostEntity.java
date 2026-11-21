package az.twitter.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.springframework.data.domain.Persistable;

@Entity
@Table(name = "posts")
public class PostEntity implements Persistable<Long> {

    @Id
    private Long id;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(nullable = false, length = 280)
    private String content;

    /** Taken from the Snowflake id, so created_at and id order never disagree. */
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /** Null until the author edits the tweet. */
    @Column(name = "edited_at")
    private Instant editedAt;

    @Transient
    private boolean isNew = true;

    protected PostEntity() {
    }

    public PostEntity(long id, long userId, String content, Instant createdAt) {
        this.id = id;
        this.userId = userId;
        this.content = content;
        this.createdAt = createdAt;
    }

    @PostPersist
    @PostLoad
    void markNotNew() {
        this.isNew = false;
    }

    public void edit(String newContent, Instant at) {
        this.content = newContent;
        this.editedAt = at;
    }

    public Instant getEditedAt() {
        return editedAt;
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public Long getUserId() {
        return userId;
    }

    public String getContent() {
        return content;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
