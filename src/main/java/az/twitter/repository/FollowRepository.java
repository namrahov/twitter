package az.twitter.repository;

import az.twitter.entity.FollowEntity;
import az.twitter.entity.FollowId;
import az.twitter.entity.UserEntity;

import java.util.List;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface FollowRepository extends JpaRepository<FollowEntity, FollowId> {

    /**
     * Idempotent and race-free in one statement; JPA has no ON CONFLICT, so this one is native.
     *
     * @return 1 if a new follow was created, 0 if it already existed
     * @throws org.springframework.dao.DataIntegrityViolationException if either user does not exist (FK)
     */
    @Transactional
    @Modifying
    @Query(value = """
            INSERT INTO follows (follower_id, followee_id)
            VALUES (:followerId, :followeeId)
            ON CONFLICT (follower_id, followee_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("followerId") long followerId, @Param("followeeId") long followeeId);

    /** Bulk delete: unlike deleteById() it does not SELECT the row first. */
    @Transactional
    @Modifying
    @Query("DELETE FROM FollowEntity f WHERE f.id.followerId = :followerId AND f.id.followeeId = :followeeId")
    int delete(@Param("followerId") long followerId, @Param("followeeId") long followeeId);

    /** Keyset page over idx_follows_followee (followee_id, follower_id). */
    @Query("""
            SELECT u FROM FollowEntity f
            JOIN UserEntity u ON u.id = f.id.followerId
            WHERE f.id.followeeId = :userId AND f.id.followerId > :afterId
            ORDER BY f.id.followerId
            """)
    List<UserEntity> findFollowers(@Param("userId") long userId, @Param("afterId") long afterId, Limit limit);

    /** Keyset page over the primary key (follower_id, followee_id). */
    @Query("""
            SELECT u FROM FollowEntity f
            JOIN UserEntity u ON u.id = f.id.followeeId
            WHERE f.id.followerId = :userId AND f.id.followeeId > :afterId
            ORDER BY f.id.followeeId
            """)
    List<UserEntity> findFollowing(@Param("userId") long userId, @Param("afterId") long afterId, Limit limit);

    /** Ids only, for fan-out: index-only scan over idx_follows_followee, no join to users. */
    @Query("""
            SELECT f.id.followerId FROM FollowEntity f
            WHERE f.id.followeeId = :userId AND f.id.followerId > :afterId
            ORDER BY f.id.followerId
            """)
    List<Long> findFollowerIds(@Param("userId") long userId, @Param("afterId") long afterId, Limit limit);

    /** Everyone the user follows, to rebuild the following:user:{id} cache; index-only scan over the primary key. */
    @Query("SELECT f.id.followeeId FROM FollowEntity f WHERE f.id.followerId = :userId")
    List<Long> findFolloweeIds(@Param("userId") long userId);
}
