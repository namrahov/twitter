package az.twitter.repository;

import az.twitter.entity.UserEntity;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface UserRepository extends JpaRepository<UserEntity, Long> {

    /**
     * +1 follower; flips the sticky celebrity flag when the threshold is reached.
     * Note: every follow of the same account updates the same row, so very popular accounts serialize here.
     */
    @Transactional
    @Modifying
    @Query(value = """
            UPDATE users
            SET follower_count = follower_count + 1,
                celebrity = celebrity OR follower_count + 1 >= :threshold
            WHERE id = :id
            """, nativeQuery = true)
    int incrementFollowerCount(@Param("id") long id, @Param("threshold") long threshold);

    @Transactional
    @Modifying
    @Query(value = "UPDATE users SET follower_count = GREATEST(follower_count - 1, 0) WHERE id = :id",
            nativeQuery = true)
    int decrementFollowerCount(@Param("id") long id);

    @Query(value = "SELECT celebrity FROM users WHERE id = :id", nativeQuery = true)
    Boolean findCelebrityFlag(@Param("id") long id);

    default boolean isCelebrity(long id) {
        return Boolean.TRUE.equals(findCelebrityFlag(id));
    }
}
