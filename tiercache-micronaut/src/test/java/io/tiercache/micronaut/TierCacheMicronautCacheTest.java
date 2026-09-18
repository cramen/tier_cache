package io.tiercache.micronaut;

import io.micronaut.core.type.Argument;
import io.tiercache.CacheSettings;
import io.tiercache.InvalidationMode;
import io.tiercache.NullPolicy;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.CountingRemoteCache;
import io.tiercache.testkit.InMemoryLockProvider;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spec: micronaut-integration — SyncCache adapter behavior (in-memory L2).
 */
class TierCacheMicronautCacheTest {

    private TierCacheMicronautCache newCache(String name, NullPolicy nullPolicy) {
        TierCacheFactory factory = newFactory(new InMemoryRemoteCache<>(), nullPolicy);
        return new TierCacheMicronautCache(name, factory.getCache(name), factory.asyncCache(name));
    }

    private TierCacheFactory newFactory(io.tiercache.spi.RemoteCache<Object, Object> l2,
            NullPolicy nullPolicy) {
        return TierCacheFactory.builder()
                .defaults(new CacheSettings(10_000, Duration.ofMinutes(5), null,
                        Duration.ofHours(1), 0.0, nullPolicy, InvalidationMode.INVALIDATE, 64 * 1024))
                .remoteCache(l2)
                .lockProvider(new InMemoryLockProvider())
                .build();
    }

    @Test
    void getPutInvalidateRoundTrip() {
        TierCacheMicronautCache cache = newCache("c", NullPolicy.deny());
        assertThat(cache.get("k", Argument.of(String.class))).isEmpty();
        cache.put("k", "v");
        assertThat(cache.get("k", Argument.of(String.class))).contains("v");
        cache.invalidate("k");
        assertThat(cache.get("k", Argument.of(String.class))).isEmpty();
    }

    @Test
    void invalidateAllClearsBothLevels() {
        TierCacheMicronautCache cache = newCache("c", NullPolicy.deny());
        cache.put("k", "v");
        cache.invalidateAll();
        assertThat(cache.get("k", Argument.of(String.class))).isEmpty();
        assertThat(((io.tiercache.TierCache<Object, Object>) cache.getNativeCache()).lookup("k"))
                .isInstanceOf(io.tiercache.LookupResult.Miss.class);
    }

    @Test
    void concurrentLoaderGetsCoalesceOntoOneSupplierExecution() throws Exception {
        TierCacheMicronautCache cache = newCache("c", NullPolicy.deny());
        AtomicInteger calls = new AtomicInteger();
        int threads = 16;
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        var start = new java.util.concurrent.CountDownLatch(1);
        var futures = new java.util.ArrayList<java.util.concurrent.Future<String>>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return cache.get("hot", Argument.of(String.class), () -> {
                    calls.incrementAndGet();
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "v";
                });
            }));
        }
        start.countDown();
        for (var f : futures) {
            assertThat(f.get(5, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("v");
        }
        pool.shutdown();
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void cachedNullMapsToEmptyOptionalButSuppressesLoader() {
        TierCacheMicronautCache cache = newCache("c", NullPolicy.allow(Duration.ofMinutes(1)));
        AtomicInteger calls = new AtomicInteger();
        // First loader-get caches the null marker.
        assertThat(cache.get("k", Argument.of(String.class), () -> {
            calls.incrementAndGet();
            return null;
        })).isNull();
        // Miss and cached-null both surface as Optional.empty() (design D3)...
        assertThat(cache.get("k", Argument.of(String.class))).isEmpty();
        // ...and the marker suppresses the supplier.
        assertThat(cache.get("k", Argument.of(String.class), () -> {
            calls.incrementAndGet();
            return "v";
        })).isNull();
        assertThat(calls.get()).isEqualTo(1);
        // ...while the tri-state stays available through the native cache.
        @SuppressWarnings("unchecked")
        io.tiercache.TierCache<Object, Object> core =
                (io.tiercache.TierCache<Object, Object>) cache.getNativeCache();
        assertThat(core.lookup("k")).isInstanceOf(io.tiercache.LookupResult.CachedNull.class);
    }

    @Test
    void nullPutIsSkippedUnderDeny() {
        TierCacheMicronautCache cache = newCache("c", NullPolicy.deny());
        cache.put("k", null);
        assertThat(cache.get("k", Argument.of(String.class))).isEmpty();
    }

    @Test
    void putIfAbsentReturnsExistingForLoser() {
        TierCacheMicronautCache cache = newCache("c", NullPolicy.deny());
        assertThat(cache.putIfAbsent("k", "v1")).isEmpty();
        assertThat(cache.putIfAbsent("k", "v2")).contains("v1");
        assertThat(cache.get("k", Argument.of(String.class))).contains("v1");
    }

    @Test
    void lookupIsMultilevelAndWarmsL1() {
        CountingRemoteCache<Object, Object> l2 = new CountingRemoteCache<>();
        TierCacheFactory factory = newFactory(l2, NullPolicy.deny());
        // Populate L2 (and L1 of the "writer" cache).
        TierCacheMicronautCache writer = new TierCacheMicronautCache("writer",
                factory.getCache("writer"), factory.asyncCache("writer"));
        writer.put("k", "v");
        // The factory memoizes one core cache per name, so a different name
        // yields a genuinely cold L1 over the same L2 (reusing "writer" would
        // serve the read from its already-warm L1 and prove nothing).
        TierCacheMicronautCache reader = new TierCacheMicronautCache("reader",
                factory.getCache("reader"), factory.asyncCache("reader"));

        int l2GetsBefore = l2.gets.get();
        assertThat(reader.get("k", Argument.of(String.class))).contains("v");
        assertThat(l2.gets.get()).isGreaterThan(l2GetsBefore)
                .as("cold L1 must be served from L2");

        // The L2 hit must have warmed L1: a second read stays on L1.
        l2GetsBefore = l2.gets.get();
        assertThat(reader.get("k", Argument.of(String.class))).contains("v");
        assertThat(l2.gets.get()).isEqualTo(l2GetsBefore)
                .as("L2 hit must warm L1; second read must not touch L2");
    }

    @Test
    void asyncViewIsBackedByTheEngine() {
        TierCacheMicronautCache cache = newCache("c", NullPolicy.deny());
        assertThat(cache.async()).isInstanceOf(TierCacheMicronautAsyncCache.class);
        cache.put("k", "v");
        assertThat(cache.async().get("k", String.class).join()).contains("v");
    }
}
