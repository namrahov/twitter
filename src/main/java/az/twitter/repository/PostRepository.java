package az.twitter.repository;

import az.twitter.entity.PostEntity;

import java.util.Collection;
import java.util.List;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PostRepository extends JpaRepository<PostEntity, Long> {

    /**
     * One {@code SELECT ... WHERE id IN (...)} for all ids. (findAllById is not used for this: with Hibernate 7
     * it multi-loads, i.e. runs one SELECT per id.)
     */
    @Query("SELECT p FROM PostEntity p WHERE p.id IN :ids")
    List<PostEntity> findByIds(@Param("ids") Collection<Long> ids);

    /** Profile timeline, keyset page over idx_posts_user_id (user_id, id DESC). */
    List<PostEntity> findByUserIdAndIdLessThanOrderByIdDesc(long userId, long beforeId, Limit limit);

    /** Newest post ids of one author; index-only over idx_posts_user_id. */
    @Query("SELECT p.id FROM PostEntity p WHERE p.userId = :userId ORDER BY p.id DESC")
    List<Long> findLatestIds(@Param("userId") long userId, Limit limit);

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

    /**
     * Rebuilds the push part of an expired Redis timeline: own tweets and those of followed non-celebrities.
     * Celebrity tweets are left out on purpose, they are pulled at read time.
     */
    @Query(value = """
            SELECT p.id FROM posts p
            WHERE p.user_id = :userId
               OR p.user_id IN (SELECT f.followee_id FROM follows f
                                JOIN users u ON u.id = f.followee_id
                                WHERE f.follower_id = :userId AND NOT u.celebrity)
            ORDER BY p.id DESC
            LIMIT :limit
            """, nativeQuery = true)
    List<Long> findPushedTimelineIds(@Param("userId") long userId, @Param("limit") int limit);
}
