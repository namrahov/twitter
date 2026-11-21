package az.twitter.service;

import java.util.concurrent.ThreadLocalRandom;

/**
 * How requests that did not get a rebuild lock wait for the request that did: a few short pauses with
 * random jitter, so thousands of waiters do not re-check Redis in the same millisecond. At most ~300 ms.
 */
final class StampedeWait {

    static final int ATTEMPTS = 3;

    private StampedeWait() {
    }

    static void pause() {
        try {
            Thread.sleep(50 + ThreadLocalRandom.current().nextInt(50));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
