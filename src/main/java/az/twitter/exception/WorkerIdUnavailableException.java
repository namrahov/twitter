package az.twitter.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * This instance holds no valid Snowflake worker id lease (not acquired yet, lost, or not renewed in time),
 * so it must not generate ids: another instance may already be using the same worker id.
 */
@ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
public class WorkerIdUnavailableException extends RuntimeException {

    public WorkerIdUnavailableException(String message) {
        super(message);
    }
}
