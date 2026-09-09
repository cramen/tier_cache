package io.tiercache.spi;

import io.tiercache.InvalidationMessage;
import io.tiercache.Version;

/**
 * Core-side hook between cache instances and the invalidation engine.
 * Caches report their local writes; the engine publishes them. The engine
 * registers caches as {@link InvalidationTarget}s for inbound events.
 *
 * <p><b>Incubating:</b> 0.x API, may change before the public API freeze.
 */
public interface InvalidationHandler extends AutoCloseable {

    /**
     * Called by a cache after each successful local write (L2 written).
     */
    void onLocalWrite(String cache, Object key, Version version, InvalidationMessage.Type type);

    /**
     * Publishes an UPDATE event carrying the new value (update-mode caches).
     */
    default void onLocalUpdate(String cache, Object key, Object value, Version version) {
        // Default: degrade to plain INVALIDATE.
        onLocalWrite(cache, key, version, InvalidationMessage.Type.INVALIDATE);
    }

    /**
     * Registers a cache instance as a receiver of invalidation events.
     */
    void registerTarget(String cache, InvalidationTarget target);

    /**
     * Registers the application-facing observer of inbound events, invoked
     * after each incoming event has been applied locally. Called once by the
     * factory right after the handler is created; the default ignores it
     * (no event observation).
     */
    default void setEventListener(InvalidationEventListener listener) {
    }

    /**
     * Called when L2 recovers after a circuit-breaker episode: the engine
     * replays the missed journal range for all registered caches. L1 is
     * never flushed here (only journal-window overflow flushes).
     */
    default void onL2Recovery() {
    }

    @Override
    void close();
}
