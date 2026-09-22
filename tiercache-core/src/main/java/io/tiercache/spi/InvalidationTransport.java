package io.tiercache.spi;

import io.tiercache.InvalidationMessage;

import java.util.function.Consumer;

/**
 * SPI for the invalidation event transport. Default profile: Redis Pub/Sub
 * (minimal latency); a durable Streams profile and third-party buses (e.g.
 * Kafka) plug in through this interface without core changes.
 *
 * <p><b>Internal — not part of the supported API.</b> Implemented by the
 * transport module.
 *
 * @since 0.1.0
 */
public interface InvalidationTransport extends AutoCloseable {

    /**
     * Publishes a message. Fire-and-forget from the caller's perspective.
     *
     * @param message the message to publish
     * @since 0.1.0
     */
    void publish(InvalidationMessage message);

    /**
     * Observes submission without waiting for delivery. A normal legacy void return
     * is unconfirmed; exceptional completion or cancellation is classified as failed.
     * Implementations must not block waiting for command completion.
     */
    default java.util.concurrent.CompletionStage<PublicationOutcome> publishAsync(InvalidationMessage message) {
        try {
            publish(message);
            return java.util.concurrent.CompletableFuture.completedFuture(PublicationOutcome.UNCONFIRMED);
        } catch (RuntimeException e) {
            return java.util.concurrent.CompletableFuture.failedFuture(e);
        }
    }


    /**
     * Subscribes to a cache's invalidation channel. The handler is invoked
     * asynchronously; per-channel ordering is preserved.
     *
     * @param cache   the cache whose channel is subscribed
     * @param handler receives each incoming message
     * @return handle to cancel the subscription
     * @since 0.1.0
     */
    AutoCloseable subscribe(String cache, Consumer<InvalidationMessage> handler);

    /**
     * Registers a callback invoked after the transport recovers from a
     * disconnect. The engine uses it to replay the journal. Transports that
     * cannot disconnect may leave the listener uncalled.
     *
     * @param listener the recovery callback
     * @since 0.1.0
     */
    default void setReconnectListener(Runnable listener) {
    }

    /** Installs optional recovery authorization; legacy transports ignore this hook. */
    default void setGapHandler(InvalidationGapHandler handler) { }

    /** Installs optional Streams diagnostics; callbacks must run outside state monitors. */
    default void setMetricsListener(CacheMetricsListener metrics) { }

    /** Side-effect-free capability: registration must establish an empty L1 before serving traffic. */
    default boolean requiresRegistrationReset() { return false; }

    /**
     * Shuts the transport down: subscriptions are cancelled and resources
     * released.
     *
     * @since 0.1.0
     */
    @Override
    void close();
}
