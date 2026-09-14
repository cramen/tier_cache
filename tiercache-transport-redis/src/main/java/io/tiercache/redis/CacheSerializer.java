package io.tiercache.redis;

/**
 * Serializes cache keys and values to bytes for storage in Redis/Valkey.
 *
 * <p>Implementations must be thread-safe and deterministic: the same value
 * must always serialize to the same bytes.
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @param <T> the type being serialized
 * @since 0.1.0
 */
public interface CacheSerializer<T> {

    /**
     * Serializes {@code value} to bytes.
     *
     * @param value the value to serialize
     * @return the serialized bytes
     * @since 0.1.0
     */
    byte[] toBytes(T value);

    /**
     * Deserializes bytes previously produced by {@link #toBytes}.
     *
     * @param bytes the serialized bytes
     * @return the deserialized value
     * @since 0.1.0
     */
    T fromBytes(byte[] bytes);
}
