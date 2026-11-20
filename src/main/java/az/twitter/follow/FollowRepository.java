package az.twitter.follow;

import java.util.List;

import az.twitter.user.User;
import az.twitter.user.UserRepository;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class FollowRepository {

    private final JdbcClient jdbc;

    public FollowRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Idempotent: an existing follow is left as is.
     *
     * @return true if a new follow row was created
     * @throws org.springframework.dao.DataIntegrityViolationException if either user does not exist (FK)
     */
    public boolean follow(long followerId, long followeeId) {
        return jdbc.sql("""
                        INSERT INTO follows (follower_id, followee_id)
                        VALUES (:followerId, :followeeId)
                        ON CONFLICT (follower_id, followee_id) DO NOTHING
                        """)
                .param("followerId", followerId)
                .param("followeeId", followeeId)
                .update() == 1;
    }

    /** @return true if a follow row was deleted */
    public boolean unfollow(long followerId, long followeeId) {
        return jdbc.sql("DELETE FROM follows WHERE follower_id = :followerId AND followee_id = :followeeId")
                .param("followerId", followerId)
                .param("followeeId", followeeId)
                .update() == 1;
    }

    /** Keyset page over idx_follows_followee (followee_id, follower_id). */
    public List<User> findFollowers(long userId, long afterId, int limit) {
        return jdbc.sql("""
                        SELECT u.id, u.username, u.display_name, u.created_at
                        FROM follows f
                        JOIN users u ON u.id = f.follower_id
                        WHERE f.followee_id = :userId AND f.follower_id > :afterId
                        ORDER BY f.follower_id
                        LIMIT :limit
                        """)
                .param("userId", userId)
                .param("afterId", afterId)
                .param("limit", limit)
                .query(UserRepository::map)
                .list();
    }

    /** Keyset page over the primary key (follower_id, followee_id). */
    public List<User> findFollowing(long userId, long afterId, int limit) {
        return jdbc.sql("""
                        SELECT u.id, u.username, u.display_name, u.created_at
                        FROM follows f
                        JOIN users u ON u.id = f.followee_id
                        WHERE f.follower_id = :userId AND f.followee_id > :afterId
                        ORDER BY f.followee_id
                        LIMIT :limit
                        """)
                .param("userId", userId)
                .param("afterId", afterId)
                .param("limit", limit)
                .query(UserRepository::map)
                .list();
    }
}
