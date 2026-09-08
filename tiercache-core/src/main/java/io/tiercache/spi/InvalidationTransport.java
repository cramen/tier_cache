package io.tiercache.spi;

import io.tiercache.InvalidationMessage;

import java.util.function.Consumer;

/**
 * SPI for the invalidation event transport. Default profile: Redis Pub/Sub
 * (minimal latency); a durable Streams profile and third-party buses (e.g.
 * Kafka) plug in through this interface without core changes.
 *
 * <p><b>Incubating:</b> 0.x API, may change before the public API freeze.
 */
public interface InvalidationTransport extends AutoCloseable {

    /**
     * Publishes a message. Fire-and-forget from the caller's perspective.
     */
    void publish(InvalidationMessage message);

    /**
     * Subscribes to a cache's invalidation channel. The handler is invoked
     * asynchronously; per-channel ordering is preserved.
     *
     * @return handle to cancel the subscription
     */
    AutoCloseable subscribe(String cache, Consumer<InvalidationMessage> handler);

    /**
     * Registers a callback invoked after the transport recovers from a
     * disconnect. The engine uses it to replay the journal. Transports that
     * cannot disconnect may leave the listener uncalled.
     */
    default void setReconnectListener(Runnable listener) {
    }

    @Override
    void close();
}
