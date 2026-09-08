package io.tiercache.redis;

/**
 * Serializes cache keys and values to bytes for storage in Redis/Valkey.
 *
 * <p><b>Incubating:</b> 0.x API, may change before the public API freeze.
 */
public interface CacheSerializer<T> {

    byte[] toBytes(T value);

    T fromBytes(byte[] bytes);
}
