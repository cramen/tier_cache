package io.tiercache.spi;

import io.tiercache.InvalidationMessage;
import io.tiercache.Version;

/**
 * Core-side hook between cache instances and the invalidation engine.
 * Caches report their local writes; the engine publishes them. The engine
 * registers caches as {@link InvalidationTarget}s for inbound events.
 *
 * <p><b>Internal — not part of the supported API.</b> Implemented by the
 * invalidation module.
 *
 * @since 0.1.0
 */
public interface InvalidationHandler extends AutoCloseable {

    /**
     * Called by a cache after each successful local write (L2 written).
     *
     * @param cache   the cache that was written
     * @param key     the affected key, or {@code null} for EVICT_ALL
     * @param version the write version
     * @param type    the event type to publish
     * @since 0.1.0
     */
    void onLocalWrite(String cache, Object key, Version version, InvalidationMessage.Type type);

    /**
     * Publishes an UPDATE event carrying the new value (update-mode caches).
     *
     * @param cache   the cache that was written
     * @param key     the affected key
     * @param value   the new value
     * @param version the write version
     * @since 0.1.0
     */
    default void onLocalUpdate(String cache, Object key, Object value, Version version) {
        // Default: degrade to plain INVALIDATE.
        onLocalWrite(cache, key, version, InvalidationMessage.Type.INVALIDATE);
    }

    /**
     * Registers a cache instance as a receiver of invalidation events.
     *
     * @param cache  the cache name
     * @param target the cache's L1 side
     * @since 0.1.0
     */
    void registerTarget(String cache, InvalidationTarget target);

    /**
     * Registers the application-facing observer of inbound events, invoked
     * after each incoming event has been applied locally. Called once by the
     * factory right after the handler is created; the default ignores it
     * (no event observation).
     *
     * @param listener the observer to register
     * @since 0.1.0
     */
    default void setEventListener(InvalidationEventListener listener) {
    }

    /**
     * Called when L2 recovers after a circuit-breaker episode: the engine
     * triggers recovery for registered caches. Built-in recovery is asynchronous;
     * use recoverAsync for completion. Unconfirmable history may require a clear.
     *
     * @since 0.1.0
     */
    default void onL2Recovery() {
    }

    /** Installs the factory-owned recovery workers before any target is registered. */
    default void configureRecoveryExecutor(java.util.concurrent.ScheduledExecutorService executor) { }

    /**
     * Nonblocking recovery completion. The compatibility adapter runs the legacy
     * hook on the supplied workers; legacy hooks must throw on failed recovery.
     * Built-in engines compose cache passes without waiting on their own pool.
     */
    default java.util.concurrent.CompletionStage<Boolean> recoverAsync(java.util.concurrent.Executor executor) {
        return java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            onL2Recovery();
            return true;
        }, executor);
    }

    /** Requests a baseline-before-clear reset; unsupported legacy handlers fail explicitly. */
    default java.util.concurrent.CompletionStage<RecoveryResult> resetAsync(String cache) {
        return java.util.concurrent.CompletableFuture.completedFuture(RecoveryResult.failed());
    }

    /**
     * Shuts the engine down: subscriptions are cancelled and resources
     * released.
     *
     * @since 0.1.0
     */
    @Override
    void close();
}
