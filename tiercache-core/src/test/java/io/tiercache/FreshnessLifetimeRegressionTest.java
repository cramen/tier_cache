package io.tiercache;

import io.tiercache.internal.CaffeineLocalCache;
import io.tiercache.internal.CircuitBreaker;
import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.LocalCache;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** Reproductions for independent metadata expiry and L1 eviction during access refresh. */
class FreshnessLifetimeRegressionTest {
    private static CacheSettings settings(Duration access) {
        return new CacheSettings(100, Duration.ofMinutes(30), access, Duration.ofHours(1), 0,
                NullPolicy.deny(), InvalidationMode.INVALIDATE, 65536,
                Duration.ZERO, false, Duration.ofSeconds(1), Duration.ofMinutes(30));
    }

    @Test
    void expiryOfIndependentFenceCannotInvalidateRetainedFreshValue() throws Exception {
        Duration previous = DefaultTierCache.L1_META_EXPIRY;
        DefaultTierCache.L1_META_EXPIRY = Duration.ofMillis(20);
        try {
            var settings = settings(null);
            var l1 = new CaffeineLocalCache<String, String>(settings);
            var breaker = new CircuitBreaker(new CircuitBreaker.Config(1, 1, 1, Duration.ofHours(1), 1),
                    new CircuitBreaker.Listener() {
                        public void onOpen() { }
                        public void onClose() { }
                    });
            var cache = new DefaultTierCache<>("c", l1, new InMemoryRemoteCache<String, String>(),
                    settings, true, null, null, null, null, breaker, io.tiercache.spi.CacheMetricsListener.NOOP);
            cache.put("hot", "retained");
            Thread.sleep(80); // Accelerate only the independent fence's expiry, not value TTL.
            assertNotNull(l1.get("hot"));
            breaker.onFailure();
            assertEquals("retained", cache.getOrCompute("hot", key -> {
                fail("fresh retained value must not call the source after fence expiry");
                return null;
            }));
        } finally {
            DefaultTierCache.L1_META_EXPIRY = previous;
        }
    }

    @Test
    void accessRefreshCannotReinsertAnEntryRemovedAfterItsRead() {
        var settings = settings(Duration.ofMinutes(20));
        var delegate = new CaffeineLocalCache<String, String>(settings);
        var reads = new AtomicInteger();
        LocalCache<String, String> evicting = new LocalCache<>() {
            public StoredEntry<String> get(String key) {
                var result = delegate.get(key);
                // Evict after the engine's second (stripe-protected) read. This models
                // removal by the L1 itself, which does not acquire the engine stripe.
                if (reads.incrementAndGet() == 2) delegate.evict(key);
                return result;
            }
            public boolean supportsAtomicReplace() { return true; }
            public boolean replaceIfSame(String key, StoredEntry<String> expected, StoredEntry<String> replacement, Duration ttl) {
                return delegate.replaceIfSame(key, expected, replacement, ttl);
            }
            public void put(String key, StoredEntry<String> value, Duration ttl) { delegate.put(key, value, ttl); }
            public boolean setIfAbsent(String key, StoredEntry<String> value, Duration ttl) {
                return delegate.setIfAbsent(key, value, ttl);
            }
            public void evict(String key) { delegate.evict(key); }
            public void clear() { delegate.clear(); }
        };
        var breaker = new CircuitBreaker(new CircuitBreaker.Config(1, 1, 1, Duration.ofHours(1), 1),
                new CircuitBreaker.Listener() { public void onOpen() { } public void onClose() { } });
        var cache = new DefaultTierCache<>("c", evicting, new InMemoryRemoteCache<String, String>(),
                settings, true, null, null, null, null, breaker, io.tiercache.spi.CacheMetricsListener.NOOP);
        cache.put("hot", "old");
        breaker.onFailure(); // Prevent a legitimate L2 re-warm from obscuring access refresh. 
        reads.set(0);
        cache.get("hot");
        assertNull(delegate.get("hot"), "access refresh must not resurrect a removed L1 entry");
    }
}
