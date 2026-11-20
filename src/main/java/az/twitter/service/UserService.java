package az.twitter.service;

import az.twitter.dto.request.CreateUserRequest;
import az.twitter.entity.UserEntity;
import az.twitter.repository.UserRepository;
import az.twitter.util.SnowflakeIdGenerator;

import org.springframework.dao.DataIntegrityViolationException;
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

    public UserEntity create(CreateUserRequest request) {
        try {
            // No "SELECT then INSERT" check: two concurrent requests could both pass it.
            // The unique index on lower(username) is the real guard.
            // saveAndFlush: Hibernate only hits the DB on flush, so the violation must surface here, inside the try.
            return users.saveAndFlush(new UserEntity(ids.nextId(), request.username(), request.displayName().strip()));
        } catch (DataIntegrityViolationException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Username already taken");
        }
    }

    public UserEntity get(long id) {
        return users.findById(id).orElseThrow(UserService::notFound);
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
