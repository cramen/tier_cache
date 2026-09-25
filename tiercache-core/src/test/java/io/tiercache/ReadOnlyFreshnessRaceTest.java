package io.tiercache;

import io.tiercache.spi.LocalCache;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Completed mutations must win over the caller's first, paused L1 snapshot. */
class ReadOnlyFreshnessRaceTest {
    @ParameterizedTest
    @CsvSource({"get,put", "get,null", "get,evict", "get,clear",
            "lookup,put", "lookup,null", "lookup,evict", "lookup,clear",
            "compute,put", "compute,null", "compute,evict", "compute,clear"})
    void rereadsCurrentLifetime(String operation, String mutation) throws Exception {
        GatedLocal local = new GatedLocal();
        CacheSettings d = CacheSettings.defaults();
        CacheSettings settings = new CacheSettings(100, Duration.ofHours(1), null,
                Duration.ofHours(3), 0, NullPolicy.allow(Duration.ofMinutes(30)),
                d.invalidationMode(), d.payloadCapBytes(), Duration.ZERO, false,
                d.xfetchBeta(), Duration.ofMinutes(10));
        try (TierCacheFactory factory = TierCacheFactory.builder().defaults(settings)
                .localCacheFactory((name, ignored) -> local)
                .remoteCache(new InMemoryRemoteCache<String, String>()).build()) {
            TierCache<String, String> cache = factory.getCache("race");
            cache.put("key", "old");
            local.armed.set(true);
            AtomicReference<Object> result = new AtomicReference<>();
            AtomicReference<Throwable> error = new AtomicReference<>();
            AtomicInteger loads = new AtomicInteger();
            Thread reader = new Thread(() -> {
                try {
                    result.set(switch (operation) {
                        case "get" -> cache.get("key");
                        case "lookup" -> cache.lookup("key");
                        default -> cache.getOrCompute("key", k -> {
                            loads.incrementAndGet();
                            return "loaded";
                        });
                    });
                } catch (Throwable failure) {
                    error.set(failure);
                }
            }, "freshness-race-reader");
            reader.start();
            try {
                assertTrue(local.observed.await(5, TimeUnit.SECONDS));
                switch (mutation) {
                    case "put" -> cache.put("key", "new");
                    case "null" -> cache.putNull("key");
                    case "evict" -> cache.evict("key");
                    case "clear" -> cache.evictAll();
                    default -> fail("Unknown mutation");
                }
            } finally {
                local.resume.countDown();
                reader.join(5000);
                if (reader.isAlive()) reader.interrupt();
            }
            assertFalse(reader.isAlive(), "reader must terminate");
            assertNull(error.get(), "reader failed");
            boolean absent = mutation.equals("evict") || mutation.equals("clear");
            Object expected = operation.equals("lookup")
                    ? absent ? LookupResult.miss()
                    : mutation.equals("null") ? LookupResult.cachedNull() : LookupResult.hit("new")
                    : absent ? operation.equals("compute") ? "loaded" : null
                    : mutation.equals("null") ? null : "new";
            assertEquals(expected, result.get());
            assertEquals(absent && operation.equals("compute") ? 1 : 0, loads.get());
            assertFalse(local.supportsAtomicReplace(), "opaque no-access provider needs no replacement SPI");
        }
    }

    /** Opaque thread-safe provider deliberately does not implement atomic replacement. */
    private static final class GatedLocal implements LocalCache<String, String> {
        final ConcurrentHashMap<String, StoredEntry<String>> entries = new ConcurrentHashMap<>();
        final AtomicBoolean armed = new AtomicBoolean();
        final CountDownLatch observed = new CountDownLatch(1);
        final CountDownLatch resume = new CountDownLatch(1);
        public StoredEntry<String> get(String key) {
            StoredEntry<String> entry = entries.get(key);
            if (armed.compareAndSet(true, false)) {
                observed.countDown();
                try {
                    if (!resume.await(5, TimeUnit.SECONDS)) throw new AssertionError("race gate timeout");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
            return entry;
        }
        public void put(String key, StoredEntry<String> value, Duration ttl) { entries.put(key, value); }
        public void evict(String key) { entries.remove(key); }
        public void clear() { entries.clear(); }
        public boolean setIfAbsent(String key, StoredEntry<String> value, Duration ttl) {
            return entries.putIfAbsent(key, value) == null;
        }
    }
}
