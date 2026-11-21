package az.twitter.service;

import az.twitter.dto.cache.CachedPost;
import az.twitter.dto.event.TweetCreatedEvent;
import az.twitter.dto.response.CursorPage;
import az.twitter.dto.response.PostResponse;
import az.twitter.entity.PostEntity;
import az.twitter.repository.PostContentCacheRepository;
import az.twitter.repository.PostRepository;
import az.twitter.util.SnowflakeIdGenerator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Limit;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.web.server.ResponseStatusException;

@Service
public class PostService {

    private static final Logger log = LoggerFactory.getLogger(PostService.class);

    private final PostRepository posts;
    private final PostContentCacheRepository contentCache;
    private final OutboxService outbox;
    private final UserService users;
    private final SnowflakeIdGenerator ids;

    public PostService(PostRepository posts, PostContentCacheRepository contentCache, OutboxService outbox,
                       UserService users, SnowflakeIdGenerator ids) {
        this.posts = posts;
        this.contentCache = contentCache;
        this.outbox = outbox;
        this.users = users;
        this.ids = ids;
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
        outbox.append(event.eventId(), TweetCreatedEvent.TYPE, id, authorId, event);

        // Warm the content cache once the post is really committed: followers start reading it right after
        // fan-out, and for a celebrity that would otherwise be a burst of simultaneous cache misses.
        CachedPost cached = CachedPost.from(post);
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                cacheQuietly(List.of(cached));
            }
        });
        return post;
    }

    public CachedPost get(long id) {
        List<CachedPost> found = getMany(List.of(id));
        if (found.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Post not found");
        }
        return found.getFirst();
    }

    /**
     * Cache-aside, batched: one MGET for all ids, one SQL query for the misses, then the misses are cached.
     * Result keeps the order of {@code ids}; ids that do not exist are skipped.
     * If Redis is unavailable, reads go straight to PostgreSQL (slower, but the page still loads).
     */
    public List<CachedPost> getMany(List<Long> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }
        Map<Long, CachedPost> byId = new HashMap<>();
        try {
            byId.putAll(contentCache.getAll(ids));
        } catch (DataAccessException e) {
            log.warn("Tweet content cache unavailable, reading from DB: {}", e.getMessage());
        }

        List<Long> missing = ids.stream().filter(id -> !byId.containsKey(id)).toList();
        if (!missing.isEmpty()) {
            List<CachedPost> loaded = posts.findAllById(missing).stream().map(CachedPost::from).toList();
            loaded.forEach(p -> byId.put(p.id(), p));
            cacheQuietly(loaded);
        }

        List<CachedPost> result = new ArrayList<>(ids.size());
        for (Long id : ids) {
            CachedPost post = byId.get(id);
            if (post != null) {
                result.add(post);
            }
        }
        return result;
    }

    public CursorPage<PostResponse> byUser(long userId, long beforeId, int limit) {
        users.requireExists(userId);
        var rows = posts.findByUserIdAndIdLessThanOrderByIdDesc(userId, beforeId, Limit.of(limit + 1));
        return CursorPage.of(rows, limit, PostEntity::getId, PostResponse::from);
    }

    /** The cache is an optimisation: failing to fill it must not fail the request. */
    private void cacheQuietly(List<CachedPost> toCache) {
        try {
            contentCache.putAll(toCache);
        } catch (DataAccessException e) {
            log.warn("Could not cache {} tweets: {}", toCache.size(), e.getMessage());
        }
    }
}
