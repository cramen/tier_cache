package io.tiercache.testkit;

import io.tiercache.spi.RemoteCache;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link InMemoryRemoteCache} that counts operations, for asserting cascade
 * behavior (e.g. "second lookup must not touch L2").
 */
public final class CountingRemoteCache<K, V> implements RemoteCache<K, V> {

    private final InMemoryRemoteCache<K, V> delegate = new InMemoryRemoteCache<>();
    public final AtomicInteger gets = new AtomicInteger();
    public final AtomicInteger puts = new AtomicInteger();
    public final AtomicInteger evicts = new AtomicInteger();

    @Override
    public V get(K key) {
        gets.incrementAndGet();
        return delegate.get(key);
    }

    @Override
    public void put(K key, V value, Duration ttl) {
        puts.incrementAndGet();
        delegate.put(key, value, ttl);
    }

    @Override
    public void evict(K key) {
        evicts.incrementAndGet();
        delegate.evict(key);
    }
}
