package io.tiercache;

import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.CountingRemoteCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: null-caching + cache-configuration null policy validation.
 */
class NullCachingTest {

    private TierCacheFactory factoryWith(NullPolicy policy) {
        return TierCacheFactory.builder()
                .defaults(new CacheSettings(10_000, Duration.ofMinutes(5), null,
                        Duration.ofHours(1), 0.0, policy, InvalidationMode.INVALIDATE, 64 * 1024))
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
    }

    // --- cache-configuration: policy in config ---

    @Test
    void nullPolicyDefaultsToDeny() {
        assertNull(CacheSettings.defaults().nullPolicy().markerTtl());
        assertEquals(NullPolicy.deny().getClass(),
                CacheSettings.defaults().nullPolicy().getClass());
    }

    @Test
    void overrideReplacesNullPolicy() {
        CacheSettings resolved = new CacheOverride()
                .nullPolicy(NullPolicy.allow(Duration.ofSeconds(30)))
                .resolve(CacheSettings.defaults());
        assertEquals(Duration.ofSeconds(30), resolved.nullPolicy().markerTtl());
    }

    @Test
    void markerTtlExceedingL2TtlIsRejected() {
        CacheConfigurationException e = assertThrows(CacheConfigurationException.class, () ->
                TierCacheFactory.builder()
                        .remoteCache(new InMemoryRemoteCache<>())
                        .cache("bad-null", new CacheOverride()
                                .l1ExpireAfterWrite(Duration.ofSeconds(30))
                                .l2Ttl(Duration.ofMinutes(1))
                                .nullPolicy(NullPolicy.allow(Duration.ofHours(2))))
                        .build());
        assertTrue(e.getMessage().contains("bad-null"), () -> e.getMessage());
        assertTrue(e.getMessage().contains("markerTtl"), () -> e.getMessage());
    }

    // --- deny: current behavior preserved ---

    @Test
    void denyPolicyLeavesMissesUncached() {
        TierCache<String, String> cache = factoryWith(NullPolicy.deny()).getCache("c");
        AtomicInteger loaderCalls = new AtomicInteger();
        cache.getOrCompute("k", key -> {
            loaderCalls.incrementAndGet();
            return null;
        });
        cache.getOrCompute("k", key -> {
            loaderCalls.incrementAndGet();
            return null;
        });
        assertEquals(2, loaderCalls.get(), "deny: every miss reaches the loader");
        assertInstanceOf(LookupResult.Miss.class, cache.lookup("k"));
    }

    // --- allow: marker absorbs the loader ---

    @Test
    void markerSuppressesLoaderUntilExpiry() {
        TierCache<String, String> cache =
                factoryWith(NullPolicy.allow(Duration.ofMillis(200))).getCache("c");
        AtomicInteger loaderCalls = new AtomicInteger();
        var loader = new java.util.function.Function<String, String>() {
            @Override
            public String apply(String key) {
                loaderCalls.incrementAndGet();
                return null;
            }
        };

        assertNull(cache.getOrCompute("k", loader));
        assertNull(cache.getOrCompute("k", loader));
        assertEquals(1, loaderCalls.get(), "marker must suppress the loader");
        assertInstanceOf(LookupResult.CachedNull.class, cache.lookup("k"));
    }

    @Test
    void getReturnsNullForMarkerAndMissAlike() {
        TierCache<String, String> cache =
                factoryWith(NullPolicy.allow(Duration.ofMinutes(1))).getCache("c");
        cache.getOrCompute("k", key -> null); // stores marker
        assertNull(cache.get("k"));
        assertNull(cache.get("never-touched"));
    }

    @Test
    void putOverwritesMarker() {
        TierCache<String, String> cache =
                factoryWith(NullPolicy.allow(Duration.ofMinutes(1))).getCache("c");
        cache.getOrCompute("k", key -> null);
        cache.put("k", "now-exists");
        assertEquals("now-exists", cache.get("k"));
        assertInstanceOf(LookupResult.Hit.class, cache.lookup("k"));
    }

    @Test
    void lookupTriState() {
        TierCache<String, String> cache =
                factoryWith(NullPolicy.allow(Duration.ofMinutes(1))).getCache("c");
        assertInstanceOf(LookupResult.Miss.class, cache.lookup("nothing"));
        cache.getOrCompute("marked", key -> null);
        assertInstanceOf(LookupResult.CachedNull.class, cache.lookup("marked"));
        cache.put("real", "v");
        LookupResult<String> hit = cache.lookup("real");
        assertInstanceOf(LookupResult.Hit.class, hit);
        assertEquals("v", ((LookupResult.Hit<String>) hit).value());
    }

    @Test
    void l2MarkerWarmsL1WithMarker() {
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        CacheSettings settings = new CacheSettings(10_000, Duration.ofMinutes(5), null,
                Duration.ofHours(1), 0.0, NullPolicy.allow(Duration.ofMinutes(1)), InvalidationMode.INVALIDATE, 64 * 1024);
        TierCache<String, String> cache =
                new io.tiercache.internal.DefaultTierCache<>(l1, l2, settings, true);

        // Seed a marker directly in L2.
        l2.put("k", io.tiercache.spi.StoredEntry.nullMarker(), Duration.ofMinutes(1));

        AtomicInteger loaderCalls = new AtomicInteger();
        assertNull(cache.getOrCompute("k", key -> {
            loaderCalls.incrementAndGet();
            return "x";
        }));
        assertEquals(0, loaderCalls.get(), "L2 marker must suppress the loader");
        assertEquals(1, l1.puts.get(), "L2 marker must warm L1 with a marker");

        int l2GetsBefore = l2.gets.get();
        assertNull(cache.getOrCompute("k", key -> "x"));
        assertEquals(l2GetsBefore, l2.gets.get(), "subsequent lookup resolves from L1");
        assertInstanceOf(LookupResult.CachedNull.class, cache.lookup("k"));
    }
}
