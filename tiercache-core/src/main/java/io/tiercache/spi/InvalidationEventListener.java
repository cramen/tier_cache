package io.tiercache.spi;

import io.tiercache.InvalidationMessage;

/**
 * Application-facing observer of inbound invalidation events. Invoked after
 * an incoming event has been applied locally (L1 entry evicted or updated),
 * in arrival order per cache. Registered via
 * {@code TierCacheFactory.Builder.invalidationEventListener(...)}; one
 * transport subscription is shared regardless of what the listener does.
 *
 * <p>The callback runs on the invalidation receive path: implementations
 * must be fast and non-blocking, and must not throw.
 *
 * <p><b>Incubating:</b> 0.x API, may change before 1.0.
 */
@FunctionalInterface
public interface InvalidationEventListener {

    InvalidationEventListener NOOP = (cache, event) -> {
    };

    /**
     * Called after the incoming {@code event} has been applied to the local
     * cache named {@code cache}.
     */
    void onEvent(String cache, InvalidationMessage event);
}
