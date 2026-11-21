package az.twitter.util;

import az.twitter.exception.CacheUnavailableException;

import java.time.Duration;
import java.util.function.Supplier;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

/**
 * Circuit breaker around request-path Redis calls.
 *
 * <ul>
 *   <li>CLOSED: calls go to Redis; failures are counted over the last 20 calls.</li>
 *   <li>OPEN (≥50% failed): for 10 s no call reaches Redis at all; callers get {@link CacheUnavailableException}
 *       at once and use their fallback. This is what keeps a dead Redis from costing a timeout per request.</li>
 *   <li>HALF_OPEN: 3 trial calls; if they succeed the breaker closes, otherwise it opens again.</li>
 * </ul>
 * Only wrap Redis calls in here, never DB calls: DB errors are DataAccessExceptions too and would trip it.
 */
@Component
public class RedisGuard {

    private static final Logger log = LoggerFactory.getLogger(RedisGuard.class);

    private final CircuitBreaker breaker = CircuitBreaker.of("redis", CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(20)
            .minimumNumberOfCalls(10)
            .failureRateThreshold(50)
            .waitDurationInOpenState(Duration.ofSeconds(10))
            .permittedNumberOfCallsInHalfOpenState(3)
            .recordExceptions(DataAccessException.class) // Spring wraps every Redis client error in one
            .build());

    public RedisGuard() {
        breaker.getEventPublisher().onStateTransition(event ->
                log.warn("Redis circuit breaker: {}", event.getStateTransition()));
    }

    /** @throws CacheUnavailableException if Redis failed or the breaker is open */
    public <T> T call(Supplier<T> redisCall) {
        try {
            return breaker.executeSupplier(redisCall);
        } catch (CallNotPermittedException e) {
            throw new CacheUnavailableException("Redis circuit breaker is open", e);
        } catch (DataAccessException e) {
            throw new CacheUnavailableException("Redis call failed: " + e.getMessage(), e);
        }
    }

    public void run(Runnable redisCall) {
        call(() -> {
            redisCall.run();
            return null;
        });
    }

    /** For best-effort writes (cache fills, lock releases): a failure is logged, never thrown. */
    public void runQuietly(Runnable redisCall) {
        try {
            run(redisCall);
        } catch (CacheUnavailableException e) {
            log.debug("Skipped best-effort Redis write: {}", e.getMessage());
        }
    }
}
