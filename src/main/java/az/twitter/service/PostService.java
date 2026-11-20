package az.twitter.service;

import az.twitter.dto.event.TweetCreatedEvent;
import az.twitter.dto.response.CursorPage;
import az.twitter.dto.response.PostResponse;
import az.twitter.entity.OutboxEventEntity;
import az.twitter.entity.PostEntity;
import az.twitter.repository.OutboxRepository;
import az.twitter.repository.PostRepository;
import az.twitter.util.SnowflakeIdGenerator;

import java.time.Instant;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Limit;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.json.JsonMapper;

@Service
public class PostService {

    private final PostRepository posts;
    private final OutboxRepository outbox;
    private final UserService users;
    private final SnowflakeIdGenerator ids;
    private final JsonMapper json;

    public PostService(PostRepository posts, OutboxRepository outbox, UserService users,
                       SnowflakeIdGenerator ids, JsonMapper json) {
        this.posts = posts;
        this.outbox = outbox;
        this.users = users;
        this.ids = ids;
        this.json = json;
    }

    /**
     * Transactional outbox: the post and its TweetCreated event commit together or not at all.
     * Nothing is sent to Kafka here; OutboxPublisher does that after commit.
     */
    @Transactional
    public PostEntity create(long authorId, String content) {
        long id = ids.nextId();
        Instant createdAt = Instant.ofEpochMilli(SnowflakeIdGenerator.timestampOf(id));
        PostEntity post;
        try {
            post = posts.saveAndFlush(new PostEntity(id, authorId, content, createdAt));
        } catch (DataIntegrityViolationException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found");
        }

        var event = new TweetCreatedEvent(UUID.randomUUID(), id, authorId, createdAt);
        outbox.save(new OutboxEventEntity(event.eventId(), id, authorId, TweetCreatedEvent.TYPE,
                json.writeValueAsString(event)));
        return post;
    }

    public PostEntity get(long id) {
        return posts.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Post not found"));
    }

    public CursorPage<PostResponse> byUser(long userId, long beforeId, int limit) {
        users.requireExists(userId);
        var rows = posts.findByUserIdAndIdLessThanOrderByIdDesc(userId, beforeId, Limit.of(limit + 1));
        return CursorPage.of(rows, limit, PostEntity::getId, PostResponse::from);
    }
}
