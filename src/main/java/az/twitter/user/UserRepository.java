package az.twitter.user;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class UserRepository {

    private final JdbcClient jdbc;

    public UserRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** @throws org.springframework.dao.DuplicateKeyException if the username is taken (case-insensitive) */
    public User insert(long id, String username, String displayName) {
        return jdbc.sql("""
                        INSERT INTO users (id, username, display_name)
                        VALUES (:id, :username, :displayName)
                        RETURNING id, username, display_name, created_at
                        """)
                .param("id", id)
                .param("username", username)
                .param("displayName", displayName)
                .query(UserRepository::map)
                .single();
    }

    public Optional<User> findById(long id) {
        return jdbc.sql("SELECT id, username, display_name, created_at FROM users WHERE id = :id")
                .param("id", id)
                .query(UserRepository::map)
                .optional();
    }

    public boolean existsById(long id) {
        return jdbc.sql("SELECT EXISTS (SELECT 1 FROM users WHERE id = :id)")
                .param("id", id)
                .query(Boolean.class)
                .single();
    }

    public static User map(ResultSet rs, int rowNum) throws SQLException {
        return new User(
                rs.getLong("id"),
                rs.getString("username"),
                rs.getString("display_name"),
                rs.getObject("created_at", OffsetDateTime.class).toInstant());
    }
}
