package az.twitter.worker;

import az.twitter.config.KafkaConfig;
import az.twitter.dto.cache.CacheInvalidation.LocalCache;
import az.twitter.dto.event.TweetCreatedEvent;
import az.twitter.dto.event.TweetDeletedEvent;
import az.twitter.dto.event.TweetEditedEvent;
import az.twitter.repository.CelebrityPostsCacheRepository;
import az.twitter.repository.FanoutProgressRepository;
import az.twitter.repository.FollowRepository;
import az.twitter.repository.PostContentCacheRepository;
import az.twitter.repository.TimelineCacheRepository;
import az.twitter.repository.UserRepository;
import az.twitter.service.CacheInvalidationService;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Hybrid fan-out:
 * <ul>
 *   <li>normal author → push: tweet id goes into the author's and every follower's Redis timeline;</li>
 *   <li>celebrity author → no push to followers (20M writes per tweet would take minutes); the id goes into the
 *       celebrity's own posts list once, and followers pull it when they read.</li>
 * </ul>
 * Also undoes it: a deleted tweet is removed from where the fan-out put it, an edited one from the content caches.
 * Lives in this app for now; it only talks to Kafka, Postgres and Redis, so it can move to its own service later.
 */
@Component
public class FanoutWorker {

    private static final Logger log = LoggerFactory.getLogger(FanoutWorker.class);
    private static final int FOLLOWER_BATCH = 1000;

    private final FollowRepository follows;
    private final UserRepository users;
    private final TimelineCacheRepository timelines;
    private final CelebrityPostsCacheRepository celebrityPosts;
    private final FanoutProgressRepository progress;
    private final CacheInvalidationService invalidation;
    private final PostContentCacheRepository contentCache;
    private final JsonMapper json;

    public FanoutWorker(FollowRepository follows, UserRepository users, TimelineCacheRepository timelines,
                        CelebrityPostsCacheRepository celebrityPosts, FanoutProgressRepository progress,
                        CacheInvalidationService invalidation, PostContentCacheRepository contentCache,
                        JsonMapper json) {
        this.contentCache = contentCache;
        this.follows = follows;
        this.users = users;
        this.timelines = timelines;
        this.celebrityPosts = celebrityPosts;
        this.progress = progress;
        this.invalidation = invalidation;
        this.json = json;
    }

    @KafkaListener(topics = KafkaConfig.TWEET_EVENTS_TOPIC, groupId = "fanout")
    public void onTweetEvent(String payload, @Header(KafkaConfig.EVENT_TYPE_HEADER) String eventType) {
        switch (eventType) {
            case TweetCreatedEvent.TYPE -> onCreated(json.readValue(payload, TweetCreatedEvent.class));
            case TweetEditedEvent.TYPE -> onEdited(json.readValue(payload, TweetEditedEvent.class));
            case TweetDeletedEvent.TYPE -> onDeleted(json.readValue(payload, TweetDeletedEvent.class));
            default -> log.warn("Ignoring unknown tweet event type {}", eventType);
        }
    }

    /**
     * Safe to run twice for the same event (redelivery, or a crash halfway through): every write is an idempotent
     * ZADD. On an exception Kafka does not commit the offset and redelivers; a redelivered event resumes from the
     * checkpoint after the last completed batch instead of rewriting every follower's timeline.
     */
    private void onCreated(TweetCreatedEvent event) {
        // The author always sees their own tweet in their own timeline.
        timelines.addToEach(List.of(event.authorId()), event.tweetId());

        if (users.isCelebrity(event.authorId())) {
            celebrityPosts.addToEach(List.of(event.authorId()), event.tweetId());
            // Every instance drops its L1 copy of this list now, so followers see the tweet on their next read.
            // If this fails, Kafka redelivers and both steps run again (the ZADD is idempotent).
            invalidation.publish(LocalCache.CELEBRITY_POSTS, event.authorId());
            log.debug("Tweet {} by celebrity {}: pull model, no fan-out", event.tweetId(), event.authorId());
            return;
        }

        long afterId = progress.lastFollowerId(event.eventId());
        if (afterId > 0) {
            log.info("Resuming fan-out of tweet {} after follower {}", event.tweetId(), afterId);
        }
        boolean checkpointed = afterId > 0;
        int total = 0;
        while (true) {
            List<Long> followerIds = follows.findFollowerIds(event.authorId(), afterId, Limit.of(FOLLOWER_BATCH));
            if (followerIds.isEmpty()) {
                break;
            }
            timelines.addToEach(followerIds, event.tweetId());
            total += followerIds.size();
            if (followerIds.size() < FOLLOWER_BATCH) {
                break;
            }
            afterId = followerIds.getLast();
            // Only multi-batch fan-outs are checkpointed: a single batch is cheap to redo.
            progress.save(event.eventId(), afterId);
            checkpointed = true;
        }
        if (checkpointed) {
            progress.clear(event.eventId());
        }
        log.debug("Fanned out tweet {} to {} followers", event.tweetId(), total);
    }

    /**
     * The guaranteed half of the edit: PostService already dropped the cached copies right after commit, but that
     * was best effort. This runs until it succeeds (Kafka retries, then the DLT), and being later it also removes
     * an old copy a concurrent reader may have put back in between.
     */
    private void onEdited(TweetEditedEvent event) {
        evictContent(event.tweetId());
        log.debug("Tweet {} edited: cached copies dropped", event.tweetId());
    }

    /**
     * Removes the id everywhere it was written: content caches, the author's timeline, and either the celebrity
     * posts list or every follower's timeline (the reverse of the fan-out). Idempotent ZREMs, so a redelivery
     * simply redoes it. Comes after the TweetCreated of the same tweet (same topic and key), so a fan-out never
     * re-adds the id after this cleanup.
     *
     * <p>A celebrity's tweets pushed before the promotion may still sit in old followers' timelines; they are not
     * swept here (that would be millions of writes) — reads skip them because their content is gone.
     */
    private void onDeleted(TweetDeletedEvent event) {
        long tweetId = event.tweetId();
        long authorId = event.authorId();
        evictContent(tweetId);
        timelines.removeAll(authorId, List.of(tweetId));

        if (users.isCelebrity(authorId)) {
            celebrityPosts.removeAll(authorId, List.of(tweetId));
            invalidation.publish(LocalCache.CELEBRITY_POSTS, authorId);
            log.debug("Celebrity tweet {} deleted", tweetId);
            return;
        }
        long afterId = 0;
        int total = 0;
        while (true) {
            List<Long> followerIds = follows.findFollowerIds(authorId, afterId, Limit.of(FOLLOWER_BATCH));
            if (followerIds.isEmpty()) {
                break;
            }
            timelines.removeFromEach(followerIds, tweetId);
            total += followerIds.size();
            if (followerIds.size() < FOLLOWER_BATCH) {
                break;
            }
            afterId = followerIds.getLast();
        }
        log.debug("Tweet {} deleted from {} follower timelines", tweetId, total);
    }

    /** Redis copy first, then every instance's L1 copy (they reload from Redis, which no longer has it). */
    private void evictContent(long tweetId) {
        contentCache.delete(tweetId);
        invalidation.publish(LocalCache.TWEET, tweetId);
    }
}
