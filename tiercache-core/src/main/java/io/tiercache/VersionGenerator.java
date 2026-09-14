package io.tiercache;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates monotonically increasing {@link Version}s for one instance. The
 * instance ID is a random UUID assigned at startup: a restarted instance is
 * a new writer with an empty L1, so no cross-restart continuity is needed.
 *
 * @since 0.1.0
 */
public final class VersionGenerator {

    private final UUID instanceId = UUID.randomUUID();
    private final AtomicLong sequence = new AtomicLong();

    /**
     * Creates a generator with a fresh random instance ID.
     *
     * @since 0.1.0
     */
    public VersionGenerator() {
    }

    /**
     * Returns the next version: the sequence incremented, stamped with this
     * instance's ID.
     *
     * @return the next version; never {@code null}
     * @since 0.1.0
     */
    public Version next() {
        return new Version(sequence.incrementAndGet(), instanceId);
    }

    /**
     * The instance ID shared by all versions from this generator.
     *
     * @return the instance ID; never {@code null}
     * @since 0.1.0
     */
    public UUID instanceId() {
        return instanceId;
    }
}
