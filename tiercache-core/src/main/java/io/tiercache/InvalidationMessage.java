package io.tiercache;

import java.util.Objects;
import java.util.UUID;

/**
 * A cross-instance invalidation message. Carries the write version for
 * last-write-wins application and the origin instance ID so publishers can
 * ignore their own events. In UPDATE mode the message also carries the new
 * value payload, letting receivers warm L1 without an L2 read.
 *
 * @param cache            the cache the event applies to
 * @param key              the affected key, or {@code null} for EVICT_ALL
 * @param version          the write version, for last-write-wins ordering
 * @param originInstanceId the instance that produced the event
 * @param type             the event type
 * @param payload          the new value for UPDATE events, otherwise
 *                         {@code null}
 * @since 0.1.0
 */
public record InvalidationMessage(
        String cache,
        Object key,
        Version version,
        UUID originInstanceId,
        Type type,
        Object payload) {

    /**
     * The kind of invalidation event.
     *
     * @since 0.1.0
     */
    public enum Type {
        /** Remove the key everywhere. */
        INVALIDATE,
        /** Remove all entries of the cache everywhere. */
        EVICT_ALL,
        /** A new value was written; the payload carries it. */
        UPDATE
    }

    /**
     * Creates a message without a payload (INVALIDATE / EVICT_ALL).
     *
     * @param cache            the cache the event applies to
     * @param key              the affected key, or {@code null} for EVICT_ALL
     * @param version          the write version
     * @param originInstanceId the instance that produced the event
     * @param type             the event type
     * @since 0.1.0
     */
    public InvalidationMessage(String cache, Object key, Version version,
            UUID originInstanceId, Type type) {
        this(cache, key, version, originInstanceId, type, null);
    }

    /**
     * Validates the message fields.
     *
     * @param cache            the cache the event applies to
     * @param key              the affected key, or {@code null} for
     *                         EVICT_ALL
     * @param version          the write version
     * @param originInstanceId the instance that produced the event
     * @param type             the event type
     * @param payload          the new value for UPDATE events, otherwise
     *                         {@code null}
     * @throws NullPointerException     if a mandatory component is
     *                                  {@code null}
     * @throws IllegalArgumentException if the key/payload combination does
     *                                  not match the event type
     * @since 0.1.0
     */
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
