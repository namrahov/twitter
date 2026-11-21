package az.twitter.util;

import java.net.InetAddress;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Leases a Snowflake worker id from the {@code snowflake_workers} table, so two running instances can never
 * use the same worker id (which would produce duplicate ids).
 *
 * <ul>
 *   <li>Startup: take a free row (expired lease) atomically; wait until the clock is past its last_timestamp.</li>
 *   <li>Every {@link #HEARTBEAT_MS}: renew. Renewal fails (row taken over) → stop issuing ids, take another row.</li>
 *   <li>DB unreachable → no renewal → ids stop after {@link #LOCAL_VALIDITY_MS}, before the DB lease
 *       ({@link #LEASE_SECONDS}) expires and someone else could take the row.</li>
 *   <li>Shutdown: release the row immediately.</li>
 * </ul>
 * Lease expiry uses the DB clock ({@code now()}), so instances with skewed clocks still agree on it.
 */
@Component
public class WorkerIdLeaseService {

    private static final Logger log = LoggerFactory.getLogger(WorkerIdLeaseService.class);

    static final int LEASE_SECONDS = 30;
    static final long HEARTBEAT_MS = 10_000;
    /** Shorter than the lease: the 10 s margin covers a slow renewal round trip and clock rate differences. */
    static final long LOCAL_VALIDITY_MS = 20_000;
    /** If the previous owner's timestamps are further ahead of our clock than this, our clock is wrong: give up. */
    static final long MAX_CLOCK_WAIT_MS = 60_000;

    private final JdbcClient jdbc;
    private final SnowflakeIdGenerator generator;
    private final String owner;

    private Integer workerId; // null = no lease held

    public WorkerIdLeaseService(JdbcClient jdbc, SnowflakeIdGenerator generator) {
        this.jdbc = jdbc;
        this.generator = generator;
        this.owner = hostname() + "-" + UUID.randomUUID();
        // Fail startup if no id can be leased: an instance that cannot create posts or users is useless.
        if (!acquire()) {
            throw new IllegalStateException("Could not lease a Snowflake worker id");
        }
    }

    @Scheduled(fixedDelay = HEARTBEAT_MS, initialDelay = HEARTBEAT_MS)
    public synchronized void heartbeat() {
        try {
            if (workerId == null) {
                acquire();
            } else if (!renew()) {
                log.error("Lost Snowflake worker id {} lease (taken over); acquiring a new one", workerId);
                generator.deactivate();
                workerId = null;
                acquire();
            }
        } catch (RuntimeException e) {
            // DB down etc.: the generator stops by itself when the local validity runs out.
            log.warn("Snowflake worker id lease heartbeat failed: {}", e.getMessage());
        }
    }

    @PreDestroy
    public synchronized void release() {
        if (workerId == null) {
            return;
        }
        long lastUsed = generator.deactivate();
        // Record the real last timestamp (not the reservation) so a restart can reuse this id without waiting.
        jdbc.sql("""
                        UPDATE snowflake_workers SET expires_at = now(), last_timestamp = :lastUsed
                        WHERE worker_id = :id AND owner = :owner
                        """)
                .param("lastUsed", Math.max(lastUsed, 0))
                .param("id", workerId)
                .param("owner", owner)
                .update();
        log.info("Released Snowflake worker id {}", workerId);
        workerId = null;
    }

    private synchronized boolean acquire() {
        // Prefer the row with the oldest last_timestamp: least (usually no) waiting for the clock.
        Optional<long[]> row = jdbc.sql("""
                        UPDATE snowflake_workers w
                        SET owner = :owner, expires_at = now() + interval '%d seconds'
                        WHERE w.worker_id = (
                            SELECT worker_id FROM snowflake_workers
                            WHERE expires_at < now()
                            ORDER BY last_timestamp, worker_id
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED)
                        RETURNING w.worker_id, w.last_timestamp
                        """.formatted(LEASE_SECONDS))
                .param("owner", owner)
                .query((rs, i) -> new long[] {rs.getInt("worker_id"), rs.getLong("last_timestamp")})
                .optional();
        if (row.isEmpty()) {
            log.error("No free Snowflake worker id: all {} are leased", SnowflakeIdGenerator.MAX_WORKER_ID + 1);
            return false;
        }
        int id = (int) row.get()[0];
        long notBefore = row.get()[1];

        if (!waitUntilClockPast(notBefore)) {
            return false; // lease expires on its own; another instance with a sane clock can take it
        }

        workerId = id;
        long startNanos = System.nanoTime();
        long reservedUntil = reserve();
        generator.activate(id, notBefore, reservedUntil,
                startNanos + TimeUnit.MILLISECONDS.toNanos(LOCAL_VALIDITY_MS));
        log.info("Leased Snowflake worker id {} as {}", id, owner);
        return true;
    }

    /** Extends the lease and moves the reserved timestamp bound forward. */
    private boolean renew() {
        long startNanos = System.nanoTime();
        long reservedUntil = System.currentTimeMillis() + LOCAL_VALIDITY_MS;
        int updated = jdbc.sql("""
                        UPDATE snowflake_workers
                        SET expires_at = now() + interval '%d seconds',
                            last_timestamp = GREATEST(last_timestamp, :reservedUntil)
                        WHERE worker_id = :id AND owner = :owner
                        """.formatted(LEASE_SECONDS))
                .param("reservedUntil", reservedUntil)
                .param("id", workerId)
                .param("owner", owner)
                .update();
        if (updated == 1) {
            generator.extend(reservedUntil, startNanos + TimeUnit.MILLISECONDS.toNanos(LOCAL_VALIDITY_MS));
        }
        return updated == 1;
    }

    /**
     * Writes the upper bound of timestamps this instance may use before the next renewal. Stored before any id
     * is issued, so even after a crash the next owner knows how far to wait.
     */
    private long reserve() {
        long reservedUntil = System.currentTimeMillis() + LOCAL_VALIDITY_MS;
        jdbc.sql("""
                        UPDATE snowflake_workers SET last_timestamp = GREATEST(last_timestamp, :reservedUntil)
                        WHERE worker_id = :id AND owner = :owner
                        """)
                .param("reservedUntil", reservedUntil)
                .param("id", workerId)
                .param("owner", owner)
                .update();
        return reservedUntil;
    }

    private static boolean waitUntilClockPast(long timestampMs) {
        long waitMs = timestampMs - System.currentTimeMillis() + 1;
        if (waitMs <= 0) {
            return true;
        }
        if (waitMs > MAX_CLOCK_WAIT_MS) {
            log.error("Clock is {} ms behind the previous owner of this worker id; refusing it", waitMs);
            return false;
        }
        log.warn("Waiting {} ms for the clock to pass the previous owner's last timestamp", waitMs);
        try {
            Thread.sleep(waitMs);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-host";
        }
    }
}
