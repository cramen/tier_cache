package io.tiercache;

import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: cache-configuration — defaults/overrides and fail-fast validation.
 */
class CacheConfigurationTest {

    @Test
    void unspecifiedCacheInheritsDefaults() {
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
        TierCache<String, String> cache = factory.getCache("unconfigured");
        // Operates with global defaults: usable without any explicit config.
        cache.put("k", "v");
        assertEquals("v", cache.get("k"));
    }

    @Test
    void overrideReplacesOnlyNamedSetting() {
        Duration customL2 = Duration.ofMinutes(30);
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .cache("custom", new CacheOverride().l2Ttl(customL2))
                .build();
        // L2 TTL overridden; everything else inherited — init succeeds and works.
        TierCache<String, String> cache = factory.getCache("custom");
        cache.put("k", "v");
        assertEquals("v", cache.get("k"));
    }

    @Test
    void invalidTtlOrderingAbortsInitialization() {
        CacheConfigurationException e = assertThrows(CacheConfigurationException.class, () ->
                TierCacheFactory.builder()
                        .remoteCache(new InMemoryRemoteCache<>())
                        .cache("bad", new CacheOverride()
                                .l1ExpireAfterWrite(Duration.ofHours(2))
                                .l2Ttl(Duration.ofHours(1)))
                        .build());
        // Error names the cache, the invariant, and the concrete values.
        assertTrue(e.getMessage().contains("bad"), () -> e.getMessage());
        assertTrue(e.getMessage().contains("PT2H"), () -> e.getMessage());
        assertTrue(e.getMessage().contains("PT1H"), () -> e.getMessage());
        assertTrue(e.getMessage().contains("stale"), () -> e.getMessage());
    }

    @Test
    void invalidExpireAfterAccessOrderingAbortsInitialization() {
        assertThrows(CacheConfigurationException.class, () ->
                TierCacheFactory.builder()
                        .remoteCache(new InMemoryRemoteCache<>())
                        .cache("bad-access", new CacheOverride()
                                .l1ExpireAfterAccess(Duration.ofHours(3))
                                .l2Ttl(Duration.ofHours(1)))
                        .build());
    }

    @Test
    void validConfigurationInitializes() {
        assertDoesNotThrow(() -> TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .cache("ok", new CacheOverride()
                        .l1ExpireAfterWrite(Duration.ofMinutes(10))
                        .l2Ttl(Duration.ofHours(1))
                        .jitterAmplitude(0.05))
                .build());
    }

    @Test
    void invalidGlobalDefaultsAbortInitialization() {
        assertThrows(CacheConfigurationException.class, () ->
                TierCacheFactory.builder()
                        .defaults(new CacheSettings(1000, Duration.ofHours(2), null,
                                Duration.ofHours(1), 0.10, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
                        .remoteCache(new InMemoryRemoteCache<>())
                        .build());
    }

    @Test
    void missingRemoteCacheIsRejected() {
        assertThrows(NullPointerException.class, () -> TierCacheFactory.builder().build());
    }

    @Test
    void negativeStaleWindowAbortsInitialization() {
        CacheConfigurationException e = assertThrows(CacheConfigurationException.class, () ->
                TierCacheFactory.builder()
                        .remoteCache(new InMemoryRemoteCache<>())
                        .cache("stale-bad", new CacheOverride()
                                .staleTtl(Duration.ofSeconds(-1)))
                        .build());
        // Error names the cache, the setting, and the concrete value.
        assertTrue(e.getMessage().contains("stale-bad"), () -> e.getMessage());
        assertTrue(e.getMessage().contains("staleTtl"), () -> e.getMessage());
        assertTrue(e.getMessage().contains("PT-1S"), () -> e.getMessage());
    }

    @Test
    void nonPositiveXfetchBetaAbortsInitialization() {
        CacheConfigurationException zero = assertThrows(CacheConfigurationException.class, () ->
                TierCacheFactory.builder()
                        .remoteCache(new InMemoryRemoteCache<>())
                        .cache("xfetch-zero-beta", new CacheOverride()
                                .xfetchEnabled(true)
                                .xfetchBeta(Duration.ZERO))
                        .build());
        assertTrue(zero.getMessage().contains("xfetch-zero-beta"), () -> zero.getMessage());
        assertTrue(zero.getMessage().contains("xfetchBeta"), () -> zero.getMessage());
        assertTrue(zero.getMessage().contains("PT0S"), () -> zero.getMessage());

        assertThrows(CacheConfigurationException.class, () ->
                TierCacheFactory.builder()
                        .remoteCache(new InMemoryRemoteCache<>())
                        .cache("xfetch-negative-beta", new CacheOverride()
                                .xfetchEnabled(true)
                                .xfetchBeta(Duration.ofMillis(-5)))
                        .build());
    }

    @Test
    void negativeStaleWindowInGlobalDefaultsAbortsInitialization() {
        assertThrows(CacheConfigurationException.class, () ->
                TierCacheFactory.builder()
                        .defaults(new CacheSettings(1000, Duration.ofMinutes(5), null,
                                Duration.ofHours(1), 0.10, NullPolicy.deny(), InvalidationMode.INVALIDATE,
                                64 * 1024, Duration.ofSeconds(-1), false, Duration.ofSeconds(1)))
                        .remoteCache(new InMemoryRemoteCache<>())
                        .build());
    }

    @Test
    void validStaleServingConfigurationInitializes() {
        assertDoesNotThrow(() -> TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .cache("stale-ok", new CacheOverride()
                        .staleTtl(Duration.ofSeconds(30))
                        .xfetchEnabled(true)
                        .xfetchBeta(Duration.ofSeconds(2)))
                .build());
    }
}
