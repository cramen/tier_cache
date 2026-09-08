package io.tiercache.testkit;

import io.tiercache.spi.LocalCache;
import io.tiercache.spi.StoredEntry;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Counting in-memory {@link LocalCache} for observing core behavior in
 * tests (e.g. "no L2 access on L1 hit").
 */
public final class CountingLocalCache<K, V> implements LocalCache<K, V> {

    private final Map<K, StoredEntry<V>> store = new ConcurrentHashMap<>();
    public final AtomicInteger gets = new AtomicInteger();
    public final AtomicInteger puts = new AtomicInteger();
    public final AtomicInteger evicts = new AtomicInteger();

    @Override
    public StoredEntry<V> get(K key) {
        gets.incrementAndGet();
        return store.get(key);
    }

    @Override
    public void put(K key, StoredEntry<V> entry, Duration ttl) {
        puts.incrementAndGet();
        store.put(key, entry);
    }

    @Override
    public void evict(K key) {
        evicts.incrementAndGet();
        store.remove(key);
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public boolean setIfAbsent(K key, StoredEntry<V> entry, Duration ttl) {
        return store.putIfAbsent(key, entry) == null;
    }
}
