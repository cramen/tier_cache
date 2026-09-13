package io.tiercache;

import java.util.Objects;
import java.util.UUID;

/**
 * A cross-instance invalidation message. Carries the write version for
 * last-write-wins application and the origin instance ID so publishers can
 * ignore their own events. In UPDATE mode the message also carries the new
 * value payload, letting receivers warm L1 without an L2 read.
 */
public record InvalidationMessage(
        String cache,
        Object key,
        Version version,
        UUID originInstanceId,
        Type type,
        Object payload) {

    public enum Type {
        INVALIDATE, EVICT_ALL, UPDATE
    }

    public InvalidationMessage(String cache, Object key, Version version,
            UUID originInstanceId, Type type) {
        this(cache, key, version, originInstanceId, type, null);
    }

    public InvalidationMessage {
        Objects.requireNonNull(cache, "cache");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(originInstanceId, "originInstanceId");
        Objects.requireNonNull(type, "type");
        if (type != Type.EVICT_ALL && key == null) {
            throw new IllegalArgumentException(type + " requires a key");
        }
        if (type == Type.UPDATE && payload == null) {
            throw new IllegalArgumentException("UPDATE requires a payload");
        }
    }
}
