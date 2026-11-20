package az.twitter.follow;

import java.util.List;

import az.twitter.common.CursorPage;
import az.twitter.user.User;
import az.twitter.user.UserResponse;
import az.twitter.user.UserService;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class FollowService {

    private final FollowRepository follows;
    private final UserService users;

    public FollowService(FollowRepository follows, UserService users) {
        this.follows = follows;
        this.users = users;
    }

    public void follow(long followerId, long followeeId) {
        if (followerId == followeeId) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Cannot follow yourself");
        }
        try {
            // Single statement, no "does the user exist?" round trip: the FK answers that.
            follows.follow(followerId, followeeId);
        } catch (DataIntegrityViolationException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found");
        }
    }

    public void unfollow(long followerId, long followeeId) {
        follows.unfollow(followerId, followeeId);
    }

    public CursorPage<UserResponse> followers(long userId, long afterId, int limit) {
        users.requireExists(userId);
        return page(follows.findFollowers(userId, afterId, limit + 1), limit);
    }

    public CursorPage<UserResponse> following(long userId, long afterId, int limit) {
        users.requireExists(userId);
        return page(follows.findFollowing(userId, afterId, limit + 1), limit);
    }

    private static CursorPage<UserResponse> page(List<User> rows, int limit) {
        return CursorPage.of(rows, limit, User::id, UserResponse::from);
    }
}
