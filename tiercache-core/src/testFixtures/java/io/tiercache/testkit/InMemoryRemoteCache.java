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

    /**
     * Stale-window write, mirroring the Redis transport: the physical
     * lifetime becomes {@code ttl + staleTtl} and the entry is stamped with
     * the current write time, so reads can classify freshness by age.
     */
    @Override
    public void put(K key, StoredEntry<V> entry, Duration ttl, Duration staleTtl) {
        if (staleTtl == null || staleTtl.isZero() || staleTtl.isNegative()) {
            put(key, entry, ttl);
            return;
        }
        store.put(key, new Entry<>(stampedWithWriteTime(entry),
                System.nanoTime() + ttl.toNanos() + staleTtl.toNanos()));
    }

    private static <V> StoredEntry<V> stampedWithWriteTime(StoredEntry<V> entry) {
        if (entry.hasWriteTimestamp()) {
            return entry;
        }
        long now = System.currentTimeMillis();
        return entry.isNullMarker()
                ? StoredEntry.nullMarker(entry.version(), now)
                : StoredEntry.ofValue(entry.value(), entry.version(), now);
    }

    @Override
    public void evict(K key) {
        store.remove(key);
        java.util.Set<String> tags = keyTags.remove(key);
        if (tags != null) {
            for (String tag : tags) {
                java.util.Set<K> members = tagIndex.get(tag);
                if (members != null) {
                    members.remove(key);
                }
            }
        }
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public boolean setIfAbsent(K key, StoredEntry<V> entry, Duration ttl) {
        long now = System.nanoTime();
        Entry<V> candidate = new Entry<>(entry, now + ttl.toNanos());
        Entry<V> result = store.merge(key, candidate,
                (existing, candidateEntry) -> now >= existing.expiresAtNanos ? candidateEntry : existing);
        return result == candidate;
    }

    private final Map<String, java.util.Set<K>> tagIndex = new ConcurrentHashMap<>();
    private final Map<K, java.util.Set<String>> keyTags = new ConcurrentHashMap<>();

    @Override
    public void putTagged(K key, StoredEntry<V> entry, Duration ttl, String[] tags) {
        put(key, entry, ttl);
        for (String tag : tags) {
            tagIndex.computeIfAbsent(tag, t -> ConcurrentHashMap.newKeySet()).add(key);
            keyTags.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(tag);
        }
    }

    @Override
    public java.util.List<K> keysByTag(String tag) {
        return java.util.List.copyOf(tagIndex.getOrDefault(tag, java.util.Set.of()));
    }

    public int size() {
        return store.size();
    }

    private record Entry<V>(StoredEntry<V> value, long expiresAtNanos) {
    }
}
