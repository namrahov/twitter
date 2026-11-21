package az.twitter.repository;

import az.twitter.entity.UserEntity;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface UserRepository extends JpaRepository<UserEntity, Long> {

    /**
     * Applies an aggregated follower change (e.g. +1500 for a whole batch of follows) and flips the sticky
     * celebrity flag when the threshold is reached. Called only by FollowerCountService, inside its transaction.
     */
    @Modifying
    @Query(value = """
            UPDATE users
            SET follower_count = GREATEST(follower_count + :delta, 0),
                celebrity = celebrity OR follower_count + :delta >= :threshold
            WHERE id = :id
            """, nativeQuery = true)
    int applyFollowerDelta(@Param("id") long id, @Param("delta") long delta, @Param("threshold") long threshold);

    @Query(value = "SELECT celebrity FROM users WHERE id = :id", nativeQuery = true)
    Boolean findCelebrityFlag(@Param("id") long id);

    default boolean isCelebrity(long id) {
        return Boolean.TRUE.equals(findCelebrityFlag(id));
    }
}
