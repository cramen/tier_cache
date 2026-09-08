package io.tiercache.testkit;

import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link RemoteCache} for tests and TCK harnesses (design D4).
 * Stands in for a real Redis/Valkey-backed transport until
 * {@code tiercache-transport-redis} exists.
 */
public final class InMemoryRemoteCache<K, V> implements RemoteCache<K, V> {

    private final Map<K, Entry<V>> store = new ConcurrentHashMap<>();

    @Override
    public StoredEntry<V> get(K key) {
        Entry<V> entry = store.get(key);
        if (entry == null) {
            return null;
        }
        if (System.nanoTime() >= entry.expiresAtNanos) {
            store.remove(key, entry);
            return null;
        }
        return entry.value;
    }

    @Override
    public void put(K key, StoredEntry<V> value, Duration ttl) {
        store.put(key, new Entry<>(value, System.nanoTime() + ttl.toNanos()));
    }

    @Override
    public void evict(K key) {
        store.remove(key);
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public boolean setIfAbsent(K key, V value, Duration ttl) {
        long now = System.nanoTime();
        Entry<V> candidate = new Entry<>(StoredEntry.ofValue(value), now + ttl.toNanos());
        Entry<V> result = store.merge(key, candidate,
                (existing, candidateEntry) -> now >= existing.expiresAtNanos ? candidateEntry : existing);
        return result == candidate;
    }

    public int size() {
        return store.size();
    }

    private record Entry<V>(StoredEntry<V> value, long expiresAtNanos) {
    }
}
