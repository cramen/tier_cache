package io.tiercache.testkit;

import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/** Benchmark attribution: counters execute only on L2 work, never on a successful L1 read. */
public final class FreshHitRemoteCache<K, V> implements RemoteCache<K, V> {
    private final ConcurrentHashMap<K, StoredEntry<V>> entries = new ConcurrentHashMap<>();
    private final LongAdder calls = new LongAdder();
    private final LongAdder loads = new LongAdder();
    public StoredEntry<V> get(K key) { calls.increment(); return entries.get(key); }
    public void put(K key, StoredEntry<V> value, Duration ttl) { calls.increment(); entries.put(key, value); }
    public void evict(K key) { calls.increment(); entries.remove(key); }
    public void clear() { calls.increment(); entries.clear(); }
    public boolean setIfAbsent(K key, StoredEntry<V> value, Duration ttl) {
        calls.increment(); return entries.putIfAbsent(key, value) == null;
    }
    /** Call only after setup, with no concurrent operations. */
    public void resetAttribution() { calls.reset(); loads.reset(); }
    public void recordLoad() { loads.increment(); }
    public long loads() { return loads.sum(); }
    public long calls() { return calls.sum(); }
    /** Call after worker quiescence; an invalid pure-hit measurement must fail loudly. */
    public void assertNoCalls() {
        long count = calls.sum();
        if (count != 0 || loads.sum() != 0) throw new AssertionError(
                "Invalid fresh-hit benchmark: " + count + " unexpected L2 calls, " + loads.sum() + " loader calls");
    }
}
