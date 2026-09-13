package io.tiercache.redis;

/**
 * Serializes cache keys and values to bytes for storage in Redis/Valkey.
 */
public interface CacheSerializer<T> {

    byte[] toBytes(T value);

    T fromBytes(byte[] bytes);
}
