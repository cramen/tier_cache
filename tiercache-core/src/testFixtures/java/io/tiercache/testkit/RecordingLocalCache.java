package io.tiercache.testkit;

import io.tiercache.spi.LocalCache;
import io.tiercache.spi.StoredEntry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * {@link LocalCache} wrapper that records the effective TTL of every write —
 * used by the avalanche TCK test (T-02) to inspect the expiry distribution.
 */
public final class RecordingLocalCache<K, V> implements LocalCache<K, V> {

    private final Map<K, StoredEntry<V>> store = new ConcurrentHashMap<>();
    private final List<Duration> recordedTtls = new CopyOnWriteArrayList<>();

    @Override
    public StoredEntry<V> get(K key) {
        return store.get(key);
    }

    @Override
    public void put(K key, StoredEntry<V> entry, Duration ttl) {
        recordedTtls.add(ttl);
        store.put(key, entry);
    }

    @Override
    public void evict(K key) {
        store.remove(key);
    }

    /**
     * Effective TTL of every put, in write order.
     */
    public List<Duration> recordedTtls() {
        return recordedTtls;
    }
}
