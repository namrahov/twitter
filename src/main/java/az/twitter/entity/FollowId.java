package az.twitter.entity;

import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

@Embeddable
public record FollowId(
        @Column(name = "follower_id") Long followerId,
        @Column(name = "followee_id") Long followeeId
) implements Serializable {
}
