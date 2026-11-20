package az.twitter.user;

import az.twitter.id.SnowflakeIdGenerator;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class UserService {

    private final UserRepository users;
    private final SnowflakeIdGenerator ids;

    public UserService(UserRepository users, SnowflakeIdGenerator ids) {
        this.users = users;
        this.ids = ids;
    }

    public User create(CreateUserRequest request) {
        try {
            // No "SELECT then INSERT" check: two concurrent requests could both pass it.
            // The unique index on lower(username) is the real guard.
            return users.insert(ids.nextId(), request.username(), request.displayName().strip());
        } catch (DuplicateKeyException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Username already taken");
        }
    }

    public User get(long id) {
        return users.findById(id).orElseThrow(() -> notFound());
    }

    public void requireExists(long id) {
        if (!users.existsById(id)) {
            throw notFound();
        }
    }

    private static ResponseStatusException notFound() {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found");
    }
}
