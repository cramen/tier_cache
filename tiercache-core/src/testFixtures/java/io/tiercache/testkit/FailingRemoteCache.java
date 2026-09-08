package io.tiercache.testkit;

import io.tiercache.Version;
import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link RemoteCache} that fails every operation while {@link #fail()} is
 * active — models a dead Redis for degradation tests.
 */
public final class FailingRemoteCache<K, V> implements RemoteCache<K, V> {

    private final InMemoryRemoteCache<K, V> delegate = new InMemoryRemoteCache<>();
    private final AtomicBoolean failing = new AtomicBoolean();

    public void fail() {
        failing.set(true);
    }

    public void heal() {
        failing.set(false);
    }

    private void maybeFail() {
        if (failing.get()) {
            throw new IllegalStateException("simulated L2 outage");
        }
    }

    @Override
    public StoredEntry<V> get(K key) {
        maybeFail();
        return delegate.get(key);
    }

    @Override
    public void put(K key, StoredEntry<V> entry, Duration ttl) {
        maybeFail();
        delegate.put(key, entry, ttl);
    }

    @Override
    public boolean putIfNewer(K key, StoredEntry<V> entry, Duration ttl) {
        maybeFail();
        return delegate.putIfNewer(key, entry, ttl);
    }

    @Override
    public void evict(K key) {
        maybeFail();
        delegate.evict(key);
    }

    @Override
    public void evict(K key, Version version) {
        maybeFail();
        delegate.evict(key, version);
    }

    @Override
    public void clear() {
        maybeFail();
        delegate.clear();
    }

    @Override
    public boolean setIfAbsent(K key, StoredEntry<V> entry, Duration ttl) {
        maybeFail();
        return delegate.setIfAbsent(key, entry, ttl);
    }
}
