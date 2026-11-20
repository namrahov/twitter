package az.twitter.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * The system clock went back further than we are willing to wait for; generating an id now could duplicate one.
 */
@ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
public class ClockMovedBackwardsException extends RuntimeException {

    public ClockMovedBackwardsException(long lastTimestamp, long now) {
        super("Clock moved backwards by " + (lastTimestamp - now) + " ms; refusing to generate id");
    }
}
