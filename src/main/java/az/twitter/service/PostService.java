package az.twitter.service;

import az.twitter.dto.cache.CacheInvalidation.LocalCache;
import az.twitter.dto.cache.CachedPost;
import az.twitter.dto.event.TweetCreatedEvent;
import az.twitter.dto.event.TweetDeletedEvent;
import az.twitter.dto.event.TweetEditedEvent;
import az.twitter.dto.response.CursorPage;
import az.twitter.dto.response.PostResponse;
import az.twitter.entity.PostEntity;
import az.twitter.exception.CacheUnavailableException;
import az.twitter.repository.PostContentCacheRepository;
import az.twitter.repository.PostRepository;
import az.twitter.repository.RebuildLockRepository;
import az.twitter.util.RedisGuard;
import az.twitter.util.SnowflakeIdGenerator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.github.benmanes.caffeine.cache.AsyncCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final AsyncCache<Long, CachedPost> localCache;
    private final RebuildLockRepository locks;
    private final RedisGuard redis;
    private final CacheInvalidationService invalidation;
    private final OutboxService outbox;
    private final UserService users;
    private final SnowflakeIdGenerator ids;

    public PostService(PostRepository posts, PostContentCacheRepository contentCache,
                       AsyncCache<Long, CachedPost> tweetLocalCache, RebuildLockRepository locks, RedisGuard redis,
                       CacheInvalidationService invalidation, OutboxService outbox, UserService users,
                       SnowflakeIdGenerator ids) {
        this.posts = posts;
        this.contentCache = contentCache;
        this.localCache = tweetLocalCache;
        this.locks = locks;
        this.redis = redis;
        this.invalidation = invalidation;
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

    /**
     * Author only. The row and a TweetEdited event commit together; then every cached copy is dropped, so the next
     * read loads the new text. Twice on purpose:
     * <ul>
     *   <li>right after commit (best effort), so the new text shows at once;</li>
     *   <li>again by the worker handling TweetEdited (FanoutWorker), retried until it succeeds. This also fixes the
     *       cache-aside race where a reader loaded the old row just before our commit and put it into Redis just
     *       after our first removal ("delayed double delete").</li>
     * </ul>
     */
    @Transactional
    public PostEntity edit(long userId, long postId, String content) {
        PostEntity post = requireOwnPost(userId, postId);
        post.edit(content, Instant.now());
        var event = new TweetEditedEvent(UUID.randomUUID(), postId, userId, post.getEditedAt());
        outbox.append(event.eventId(), TweetEditedEvent.TYPE, postId, userId, event);
        evictAfterCommit(postId);
        return post;
    }

    /**
     * Author only. The row is deleted and a TweetDeleted event queued; the content caches are cleared as in
     * {@link #edit}, and the worker also removes the id from the timelines and the celebrity posts list.
     * Until then, timeline reads drop the id themselves (it has no content any more).
     */
    @Transactional
    public void delete(long userId, long postId) {
        PostEntity post = requireOwnPost(userId, postId);
        posts.delete(post);
        var event = new TweetDeletedEvent(UUID.randomUUID(), postId, userId, Instant.now());
        outbox.append(event.eventId(), TweetDeletedEvent.TYPE, postId, userId, event);
        evictAfterCommit(postId);
    }

    private PostEntity requireOwnPost(long userId, long postId) {
        PostEntity post = posts.findById(postId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Post not found"));
        if (post.getUserId() != userId) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Only the author can change this post");
        }
        return post;
    }

    /** Drops the tweet from this instance's L1, from Redis, and (via Pub/Sub) from every other instance's L1. */
    private void evictAfterCommit(long postId) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                localCache.synchronous().invalidate(postId);
                redis.runQuietly(() -> contentCache.delete(postId));
                redis.runQuietly(() -> invalidation.publish(LocalCache.TWEET, postId));
            }
        });
    }

    public CachedPost get(long id) {
        List<CachedPost> found = getMany(List.of(id));
        if (found.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Post not found");
        }
        return found.getFirst();
    }

    /**
     * Three tiers, each asked only for what the previous one lacked:
     * L1 (this JVM, Caffeine) → L2 (Redis, one MGET) → PostgreSQL. Result keeps the order of {@code ids};
     * ids that do not exist are skipped.
     *
     * <p>Stampede protection: concurrent requests missing the same tweet in L1 share one load (async cache),
     * and across instances only the holder of a per-tweet Redis lock reads it from PostgreSQL.
     */
    public List<CachedPost> getMany(List<Long> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }
        // While edit/delete invalidations may be getting lost, L1 copies could be stale: skip L1 then.
        Map<Long, CachedPost> byId = invalidation.isLocalCacheTrusted()
                ? localCache.getAll(ids, (missing, executor) ->
                        CompletableFuture.completedFuture(loadFromRedisOrDb(missing))).join()
                : loadFromRedisOrDb(Set.copyOf(ids));

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

    /**
     * L1 miss: Redis, then PostgreSQL under a per-tweet lock. Returned entries are put into L1 by the caller.
     * If Redis fails at any point (or its circuit breaker is open), whatever is still missing comes from the DB.
     */
    private Map<Long, CachedPost> loadFromRedisOrDb(Set<? extends Long> requested) {
        List<Long> ids = requested.stream().map(Long::valueOf).toList();
        Map<Long, CachedPost> found = new HashMap<>();
        try {
            loadThroughRedis(ids, found);
        } catch (CacheUnavailableException e) {
            log.debug("Tweet content cache unavailable, reading from DB: {}", e.getMessage());
            loadFromDb(ids.stream().filter(id -> !found.containsKey(id)).toList(), found);
        }
        return found;
    }

    private void loadThroughRedis(List<Long> ids, Map<Long, CachedPost> found) {
        found.putAll(redis.call(() -> contentCache.getAll(ids)));

        List<Long> mine = new ArrayList<>();
        List<Long> waiting = new ArrayList<>();
        Map<Long, String> tokens = new HashMap<>();
        for (Long id : ids) {
            if (found.containsKey(id)) {
                continue;
            }
            Optional<String> token = redis.call(() -> locks.tryAcquire(lockName(id)));
            if (token.isPresent()) {
                tokens.put(id, token.get());
                mine.add(id);
            } else {
                waiting.add(id);
            }
        }

        try {
            if (!mine.isEmpty()) {
                // Double-check: another instance may have filled Redis between our MGET and our lock.
                found.putAll(redis.call(() -> contentCache.getAll(mine)));
                loadFromDb(mine.stream().filter(id -> !found.containsKey(id)).toList(), found);
            }
        } finally {
            // If Redis is gone, the release fails quietly and the lock simply expires (TTL).
            tokens.forEach((id, token) -> redis.runQuietly(() -> locks.release(lockName(id), token)));
        }

        // Someone else is loading these: wait briefly for them to appear in Redis instead of hitting the DB too.
        for (int attempt = 0; attempt < StampedeWait.ATTEMPTS && !waiting.isEmpty(); attempt++) {
            StampedeWait.pause();
            List<Long> stillWaiting = waiting;
            found.putAll(redis.call(() -> contentCache.getAll(stillWaiting)));
            waiting = waiting.stream().filter(id -> !found.containsKey(id)).toList();
        }
        // Still missing (lock holder slow or died): the tweet must show, so read it ourselves as a last resort.
        loadFromDb(waiting, found);
    }

    private void loadFromDb(List<Long> ids, Map<Long, CachedPost> into) {
        if (ids.isEmpty()) {
            return;
        }
        List<CachedPost> loaded = posts.findByIds(ids).stream().map(CachedPost::from).toList();
        loaded.forEach(p -> into.put(p.id(), p));
        redis.runQuietly(() -> contentCache.putAll(loaded));
    }

    private static String lockName(long tweetId) {
        return "rebuild:tweet:" + tweetId;
    }

    /** Fills L1 and L2. The caches are an optimisation: failing to fill Redis must not fail the request. */
    private void cacheQuietly(List<CachedPost> toCache) {
        toCache.forEach(p -> localCache.synchronous().put(p.id(), p));
        redis.runQuietly(() -> contentCache.putAll(toCache));
    }
}
