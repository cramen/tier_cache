package io.tiercache.spring;

import io.tiercache.InvalidationMode;

import io.tiercache.CacheSettings;
import io.tiercache.NullPolicy;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.CountingRemoteCache;
import io.tiercache.testkit.InMemoryLockProvider;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;
import org.springframework.cache.Cache;
import org.springframework.cache.support.NullValue;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Spec: spring-cache-integration — adapter behavior (in-memory L2).
 */
class TierCacheSpringCacheTest {

    private TierCacheSpringCache newCache(String name, NullPolicy nullPolicy) {
        TierCacheFactory factory = newFactory(new InMemoryRemoteCache<>(), nullPolicy);
        return new TierCacheSpringCache(name, factory.getCache(name));
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
    void getPutEvictRoundTrip() {
        TierCacheSpringCache cache = newCache("c", NullPolicy.deny());
        assertThat(cache.get("k")).isNull();
        cache.put("k", "v");
        assertThat(cache.get("k").get()).isEqualTo("v");
        assertThat(cache.get("k", String.class)).isEqualTo("v");
        cache.evict("k");
        assertThat(cache.get("k")).isNull();
    }

    @Test
    void getWithWrongTypeFails() {
        TierCacheSpringCache cache = newCache("c", NullPolicy.deny());
        cache.put("k", "v");
        assertThatThrownBy(() -> cache.get("k", Integer.class))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void concurrentValueLoadersCoalesce() throws Exception {
        TierCacheSpringCache cache = newCache("c", NullPolicy.deny());
        AtomicInteger calls = new AtomicInteger();
        int threads = 16;
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        var start = new java.util.concurrent.CountDownLatch(1);
        var futures = new java.util.ArrayList<java.util.concurrent.Future<String>>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return cache.get("hot", () -> {
                    calls.incrementAndGet();
                    Thread.sleep(50);
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
    void retrieveIsMultilevel() throws Exception {
        CountingRemoteCache<Object, Object> l2 = new CountingRemoteCache<>();
        TierCacheFactory factory = newFactory(l2, NullPolicy.deny());
        // Populate L2 (and L1 of the "writer" cache).
        TierCacheSpringCache writer = new TierCacheSpringCache("writer", factory.getCache("writer"));
        writer.put("k", "v");
        // The factory memoizes one core cache per name, so a different name
        // yields a genuinely cold L1 over the same L2 (reusing "writer" would
        // serve the read from its already-warm L1 and prove nothing).
        TierCacheSpringCache reader = new TierCacheSpringCache("reader", factory.getCache("reader"));

        int l2GetsBefore = l2.gets.get();
        Cache.ValueWrapper wrapper = reader.retrieve("k").get();
        assertThat(wrapper.get()).isEqualTo("v");
        assertThat(l2.gets.get()).isGreaterThan(l2GetsBefore)
                .as("cold L1 must be served from L2");

        // The L2 hit must have warmed L1: a second retrieve stays on L1.
        l2GetsBefore = l2.gets.get();
        assertThat(reader.retrieve("k").get().get()).isEqualTo("v");
        assertThat(l2.gets.get()).isEqualTo(l2GetsBefore)
                .as("L2 hit must warm L1; second retrieve must not touch L2");

        assertThat(reader.retrieve("absent").get()).isNull();
    }

    @Test
    void nullResultMapsToMarkerUnderAllow() {
        TierCacheSpringCache cache = newCache("c", NullPolicy.allow(Duration.ofMinutes(1)));
        cache.put("k", null);
        // Spring convention: callers get an unwrapped null...
        assertThat(cache.get("k").get()).isNull();
        // ...while the marker stays distinguishable at the core level.
        @SuppressWarnings("unchecked")
        io.tiercache.TierCache<Object, Object> core =
                (io.tiercache.TierCache<Object, Object>) cache.getNativeCache();
        assertThat(core.lookup("k")).isInstanceOf(io.tiercache.LookupResult.CachedNull.class);
    }

    @Test
    void nullResultIsSkippedUnderDeny() {
        TierCacheSpringCache cache = newCache("c", NullPolicy.deny());
        cache.put("k", null);
        assertThat(cache.get("k")).isNull();
    }

    @Test
    void evictIfPresentReportsPresence() {
        TierCacheSpringCache cache = newCache("c", NullPolicy.deny());
        cache.put("k", "v");
        assertThat(cache.evictIfPresent("k")).isTrue();
        assertThat(cache.evictIfPresent("k")).isFalse();
    }

    @Test
    void putIfAbsentReturnsExistingForLoser() {
        TierCacheSpringCache cache = newCache("c", NullPolicy.deny());
        assertThat(cache.putIfAbsent("k", "v1")).isNull();
        assertThat(cache.putIfAbsent("k", "v2").get()).isEqualTo("v1");
        assertThat(cache.get("k").get()).isEqualTo("v1");
    }
}
