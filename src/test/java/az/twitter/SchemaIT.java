package az.twitter;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
@Transactional
class SchemaIT {

    private final JdbcTemplate jdbc;

    @Autowired
    SchemaIT(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @BeforeEach
    void createUsers() {
        jdbc.update("INSERT INTO users (id, username, display_name) VALUES (1, 'alice', 'Alice'), (2, 'bob', 'Bob')");
    }

    @Test
    void migrationsAppliedExactlyOnce() {
        assertThat(jdbc.queryForObject("SELECT count(*) FROM databasechangelog", Integer.class)).isEqualTo(4);
    }

    @Test
    void timelineUsesFollowedAuthorsAndDeterministicOrder() {
        jdbc.update("INSERT INTO follows (follower_id, followee_id) VALUES (1, 2)");
        jdbc.update("INSERT INTO posts (id, user_id, content, created_at) VALUES (101, 2, 'First', '2026-01-01T10:00:00Z'), (102, 2, 'Second', '2026-01-01T10:00:00Z'), (103, 1, 'Own post', '2026-01-01T11:00:00Z')");

        var ids = jdbc.queryForList("""
                SELECT p.id FROM posts p
                JOIN follows f ON f.followee_id = p.user_id
                WHERE f.follower_id = ?
                ORDER BY p.created_at DESC, p.id DESC LIMIT 20
                """, Long.class, 1L);

        assertThat(ids).containsExactly(102L, 101L);
    }

    @Test
    void duplicateFollowIsRejected() {
        jdbc.update("INSERT INTO follows (follower_id, followee_id) VALUES (1, 2)");
        assertThatThrownBy(() -> jdbc.update("INSERT INTO follows (follower_id, followee_id) VALUES (1, 2)"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void selfFollowIsRejected() {
        assertThatThrownBy(() -> jdbc.update("INSERT INTO follows (follower_id, followee_id) VALUES (1, 1)"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void missingAuthorIsRejected() {
        assertThatThrownBy(() -> jdbc.update("INSERT INTO posts (id, user_id, content) VALUES (101, 999, 'Hello')"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void usernameUniquenessIsCaseInsensitive() {
        assertThatThrownBy(() -> jdbc.update("INSERT INTO users (id, username, display_name) VALUES (3, 'ALICE', 'Other Alice')"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void blankPostIsRejected() {
        assertThatThrownBy(() -> jdbc.update("INSERT INTO posts (id, user_id, content) VALUES (101, 1, '   ')"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void postAndOutboxParticipateInSameTransaction() {
        jdbc.execute("SAVEPOINT before_post");
        jdbc.update("INSERT INTO posts (id, user_id, content) VALUES (101, 1, 'Hello')");
        jdbc.update("INSERT INTO outbox (event_id, aggregate_id, user_id, event_type, payload) VALUES (?, 101, 1, 'TweetCreated', ?::jsonb)",
                UUID.randomUUID(), "{\"tweetId\":101,\"authorId\":1}");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE aggregate_id = 101 AND status = 'PENDING'", Integer.class)).isEqualTo(1);

        jdbc.execute("ROLLBACK TO SAVEPOINT before_post");

        assertThat(jdbc.queryForObject("SELECT count(*) FROM posts WHERE id = 101", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE aggregate_id = 101", Integer.class)).isZero();
    }
}
