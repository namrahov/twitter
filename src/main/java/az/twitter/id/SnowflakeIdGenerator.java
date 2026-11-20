package az.twitter.id;

import java.time.Instant;

import org.springframework.stereotype.Component;

/**
 * 64-bit, roughly time-ordered unique ids without a central sequence.
 *
 * <pre>
 * | 1 bit sign (0) | 41 bits ms since EPOCH | 10 bits worker id | 12 bits sequence |
 * </pre>
 *
 * Known limitation: {@code lastTimestamp} lives in memory, so a restart combined with a clock rollback
 * can repeat ids. A worker-id lease would fix that later.
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

    private final long workerId;

    private long lastTimestamp = -1L;
    private long sequence = 0L;

    public SnowflakeIdGenerator(SnowflakeProperties properties) {
        long id = properties.workerId();
        if (id < 0 || id > MAX_WORKER_ID) {
            throw new IllegalArgumentException("workerId must be in 0.." + MAX_WORKER_ID + ", got " + id);
        }
        this.workerId = id;
    }

    public synchronized long nextId() {
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

        lastTimestamp = now;

        return ((now - EPOCH) << TIMESTAMP_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    /** Epoch millis encoded in the id (used e.g. as the Redis sorted-set score). */
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
