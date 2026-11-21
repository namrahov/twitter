package az.twitter.exception;

/** Redis failed or its circuit breaker is open: callers switch to their fallback (L1, PostgreSQL, or 503). */
public class CacheUnavailableException extends RuntimeException {

    public CacheUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
