package io.tiercache.redis;

/** Aggregate receiver bounds, including executing messages. Internal transport configuration. */
public record PubSubDispatchOptions(int dispatchThreads, int maxPendingMessages, long maxPendingBytes) {
    public static final PubSubDispatchOptions DEFAULT = new PubSubDispatchOptions(2, 1024, 16_777_216);
    public PubSubDispatchOptions {
        positive(dispatchThreads, "dispatch-threads");
        positive(maxPendingMessages, "max-pending-messages");
        positive(maxPendingBytes, "max-pending-bytes");
    }
    private static void positive(long value, String property) {
        if (value <= 0) throw new IllegalArgumentException(
                "tiercache.invalidation.pubsub." + property + " must be positive");
    }
}
