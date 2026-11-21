package az.twitter.util;

import az.twitter.exception.ClockMovedBackwardsException;
import az.twitter.exception.WorkerIdUnavailableException;

import java.time.Instant;

import org.springframework.stereotype.Component;

/**
 * 64-bit, roughly time-ordered unique ids without a central sequence.
 *
 * <pre>
 * | 1 bit sign (0) | 41 bits ms since EPOCH | 10 bits worker id | 12 bits sequence |
 * </pre>
 *
 * <p>The worker id is not configured: it is leased by {@link WorkerIdLeaseService}, which calls
 * {@link #activate}/{@link #extend}. Ids are issued only while the lease is valid, and only with timestamps up to
 * {@code reservedUntilMs} — the bound recorded in the DB — so the next owner of this worker id can wait until its
 * clock is past every timestamp this instance could have used.
 */
@Component
public class SnowflakeIdGenerator {

    /** 2026-01-01T00:00:00Z — 41 bits of ms last ~69 years from here. */
    public static final long EPOCH = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();

    static final int WORKER_ID_BITS = 10;
    static final int SEQUENCE_BITS = 12;

    public static final long MAX_WORKER_ID = (1L << WORKER_ID_BITS) - 1; // 1023
    static final long SEQUENCE_MASK = (1L << SEQUENCE_BITS) - 1;         // 4095

    static final int WORKER_ID_SHIFT = SEQUENCE_BITS;                    // 12
    static final int TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;   // 22

    /** Rollbacks up to this size (typical NTP corrections) are waited out instead of failing. */
    static final long MAX_TOLERATED_ROLLBACK_MS = 5;

    private long workerId = -1;
    /** Highest timestamp this instance may put into an id (wall clock, ms). */
    private long reservedUntilMs;
    /** Lease validity on the monotonic clock: immune to wall-clock jumps. */
    private long validUntilNanos;
    private boolean active;

    private long lastTimestamp = -1L;
    private long sequence = 0L;

    /**
     * Starts issuing ids with a newly leased worker id.
     *
     * @param notBeforeMs the previous owner's bound; ids from here on must have a later timestamp
     */
    public synchronized void activate(long workerId, long notBeforeMs, long reservedUntilMs, long validUntilNanos) {
        if (workerId < 0 || workerId > MAX_WORKER_ID) {
            throw new IllegalArgumentException("workerId must be in 0.." + MAX_WORKER_ID + ", got " + workerId);
        }
        this.workerId = workerId;
        this.lastTimestamp = Math.max(lastTimestamp, notBeforeMs);
        this.sequence = 0;
        this.reservedUntilMs = reservedUntilMs;
        this.validUntilNanos = validUntilNanos;
        this.active = true;
    }

    /** Lease renewed. */
    public synchronized void extend(long reservedUntilMs, long validUntilNanos) {
        this.reservedUntilMs = Math.max(this.reservedUntilMs, reservedUntilMs);
        this.validUntilNanos = validUntilNanos;
    }

    /**
     * Stops issuing ids (lease lost or released).
     *
     * @return the highest timestamp actually used, so a graceful release can let the next owner start sooner
     */
    public synchronized long deactivate() {
        this.active = false;
        return lastTimestamp;
    }

    public synchronized long nextId() {
        if (!active || System.nanoTime() - validUntilNanos > 0) {
            throw new WorkerIdUnavailableException("No valid Snowflake worker id lease");
        }

        long now = currentTimeMillis();

        if (now < lastTimestamp) {
            now = waitOutRollback(now);
        }

        if (now == lastTimestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                // 4096 ids already issued in this millisecond
                now = waitNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0;
        }

        if (now > reservedUntilMs) {
            // Wall clock jumped past what the DB says we may use; the next renewal moves the bound forward.
            throw new WorkerIdUnavailableException("Clock is past the reserved timestamp range");
        }

        lastTimestamp = now;

        return ((now - EPOCH) << TIMESTAMP_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    /** Epoch millis encoded in the id (used e.g. as the post's created_at). */
    public static long timestampOf(long id) {
        return (id >>> TIMESTAMP_SHIFT) + EPOCH;
    }

    public static long workerIdOf(long id) {
        return (id >>> WORKER_ID_SHIFT) & MAX_WORKER_ID;
    }

    public static long sequenceOf(long id) {
        return id & SEQUENCE_MASK;
    }

    private long waitOutRollback(long now) {
        long offset = lastTimestamp - now;
        if (offset > MAX_TOLERATED_ROLLBACK_MS) {
            throw new ClockMovedBackwardsException(lastTimestamp, now);
        }
        try {
            Thread.sleep(offset);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for clock to catch up", e);
        }
        now = currentTimeMillis();
        if (now < lastTimestamp) {
            throw new ClockMovedBackwardsException(lastTimestamp, now);
        }
        return now;
    }

    private long waitNextMillis(long last) {
        long now = currentTimeMillis();
        while (now <= last) {
            Thread.onSpinWait();
            now = currentTimeMillis();
        }
        return now;
    }

    long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
