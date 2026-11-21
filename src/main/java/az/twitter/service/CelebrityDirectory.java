package az.twitter.service;

import az.twitter.repository.UserRepository;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Which accounts are celebrities, held in memory by every instance. With the user's following set (Redis) this
 * answers "which celebrities does this user follow?" without a database query per timeline request.
 *
 * <p>Reloaded from PostgreSQL every 5 s in the background: one small index-only query per instance, whatever the
 * request rate. Readers never wait for it, and if a reload fails the previous set stays. A newly promoted celebrity
 * is therefore recognised within ~5 s; until then its new tweets (no longer pushed) are not pulled either, after
 * that they show up from its posts list.
 *
 * <p>Size: one entry per celebrity. Fine for thousands; with millions of celebrities this set would belong in
 * Redis instead.
 */
@Service
public class CelebrityDirectory {

    private static final Logger log = LoggerFactory.getLogger(CelebrityDirectory.class);

    private final UserRepository users;
    private volatile Set<Long> celebrityIds;

    public CelebrityDirectory(UserRepository users) {
        this.users = users;
    }

    public boolean isCelebrity(long userId) {
        Set<Long> ids = celebrityIds;
        if (ids == null) { // first request before the first scheduled load
            reload();
            ids = celebrityIds;
        }
        return ids != null && ids.contains(userId);
    }

    @Scheduled(fixedDelay = 5000)
    void reload() {
        try {
            celebrityIds = Set.copyOf(users.findCelebrityIds());
        } catch (RuntimeException e) {
            log.warn("Celebrity ids not reloaded, keeping the previous set: {}", e.getMessage());
        }
    }
}
