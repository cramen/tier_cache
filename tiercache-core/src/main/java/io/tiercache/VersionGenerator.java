package io.tiercache;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates monotonically increasing {@link Version}s for one instance. The
 * instance ID is a random UUID assigned at startup: a restarted instance is
 * a new writer with an empty L1, so no cross-restart continuity is needed.
 */
public final class VersionGenerator {

    private final UUID instanceId = UUID.randomUUID();
    private final AtomicLong sequence = new AtomicLong();

    public Version next() {
        return new Version(sequence.incrementAndGet(), instanceId);
    }

    public UUID instanceId() {
        return instanceId;
    }
}
