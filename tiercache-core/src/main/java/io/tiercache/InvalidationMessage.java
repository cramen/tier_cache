package io.tiercache;

import java.util.Objects;
import java.util.UUID;

/**
 * A cross-instance invalidation message. Carries the write version for
 * last-write-wins application and the origin instance ID so publishers can
 * ignore their own events.
 *
 * <p><b>Incubating:</b> 0.x API, may change before the public API freeze.
 */
public record InvalidationMessage(
        String cache,
        Object key,
        Version version,
        UUID originInstanceId,
        Type type) {

    public enum Type {
        INVALIDATE, EVICT_ALL
    }

    public InvalidationMessage {
        Objects.requireNonNull(cache, "cache");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(originInstanceId, "originInstanceId");
        Objects.requireNonNull(type, "type");
        if (type == Type.INVALIDATE && key == null) {
            throw new IllegalArgumentException("INVALIDATE requires a key");
        }
    }
}
