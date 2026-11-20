package az.twitter.repository;

import az.twitter.entity.PostEntity;

import java.util.List;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PostRepository extends JpaRepository<PostEntity, Long> {

    /** Profile timeline, keyset page over idx_posts_user_id (user_id, id DESC). */
    List<PostEntity> findByUserIdAndIdLessThanOrderByIdDesc(long userId, long beforeId, Limit limit);

    /**
     * Home timeline computed on read (the original JOIN from the design notes).
     * Used only when Redis has no (more) precomputed entries for the user.
     */
    @Query("""
            SELECT p FROM PostEntity p
            WHERE p.id < :beforeId
              AND (p.userId = :userId
                   OR p.userId IN (SELECT f.id.followeeId FROM FollowEntity f WHERE f.id.followerId = :userId))
            ORDER BY p.id DESC
            """)
    List<PostEntity> findHomeTimeline(@Param("userId") long userId, @Param("beforeId") long beforeId, Limit limit);
}
