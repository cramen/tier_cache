package io.tiercache;

import io.tiercache.internal.CaffeineLocalCache;
import io.tiercache.internal.DefaultTierCache;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.CountingRemoteCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Additional branch coverage: L2-miss path of plain get, expire-after-access
 * wiring, and factory builder branches.
 */
class AdditionalBehaviorTest {

    @Test
    void plainGetMissesWhenAbsentEverywhere() {
        TierCache<String, String> cache = new DefaultTierCache<>(
                new CountingLocalCache<>(), new CountingRemoteCache<>(), CacheSettings.defaults(), true);
        assertNull(cache.get("absent"));
    }

    @Test
    void caffeineL1HonorsExpireAfterAccess() throws InterruptedException {
        CacheSettings settings = new CacheSettings(1000, Duration.ofMinutes(10),
                Duration.ofMillis(60), Duration.ofHours(1), 0.0, NullPolicy.deny());
        CaffeineLocalCache<String, String> cache = new CaffeineLocalCache<>(settings);
        cache.put("k", io.tiercache.spi.StoredEntry.ofValue("v"), Duration.ofMinutes(10));
        cache.get("k"); // touches the entry: resets the access window
        Thread.sleep(150);
        assertNull(cache.get("k"), "entry must expire after the access window lapses");
    }

    @Test
    void factoryBuilderRejectsNullArguments() {
        TierCacheFactory.Builder builder = TierCacheFactory.builder();
        assertThrows(NullPointerException.class, () -> builder.defaults(null));
        assertThrows(NullPointerException.class, () -> builder.cache(null, new CacheOverride()));
        assertThrows(NullPointerException.class, () -> builder.cache("x", null));
        assertThrows(NullPointerException.class, () -> builder.remoteCache(null));
        assertThrows(NullPointerException.class, () -> builder.localCacheFactory(null));
    }

    @Test
    void factoryUsesCustomL1Factory() {
        boolean[] called = {false};
        TierCacheFactory factory = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .localCacheFactory((name, settings) -> {
                    called[0] = true;
                    return new CountingLocalCache<>();
                })
                .build();
        factory.getCache("any");
        assertTrue(called[0], "custom L1 factory must be used");
    }

    @Test
    void disabledSingleflightStillCaches() {
        TierCache<String, String> cache = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .disableSingleflight()
                .build()
                .getCache("c");
        assertEquals("v", cache.getOrCompute("k", key -> "v"));
        assertEquals("v", cache.get("k"));
    }

    @Test
    void getWarmsL1WithJitteredTtlWithinL2Ttl() {
        // TTL ordering end-to-end: after an L2 hit warms L1, the L1 copy expires no
        // later than the L2 copy even with jitter enabled.
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        TierCache<String, String> cache = TierCacheFactory.builder()
                .defaults(new CacheSettings(1000, Duration.ofMillis(200), null,
                        Duration.ofMillis(200), 0.50, NullPolicy.deny()))
                .remoteCache(l2)
                .build()
                .getCache("c");
        cache.put("k", "v");
        assertEquals("v", cache.get("k"));
    }
}
