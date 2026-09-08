package io.tiercache;

import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.CountingRemoteCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: core-read-path — atomic put-if-absent write.
 */
class PutIfAbsentTest {

    @Test
    void winnerStoresAndWarmsL1() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        TierCache<String, String> cache = new io.tiercache.internal.DefaultTierCache<>(
                l1, l2, CacheSettings.defaults(), true);

        assertTrue(cache.putIfAbsent("k", "v"));
        assertEquals("v", l2.get("k").value(), "winner stores in L2");
        assertEquals(1, l1.puts.get(), "winner warms L1");
        // Subsequent local lookup served from L1.
        int l2GetsBefore = l2.gets.get();
        assertEquals("v", cache.get("k"));
        assertEquals(l2GetsBefore, l2.gets.get());
    }

    @Test
    void loserLeavesStateUntouched() {
        TierCache<String, String> cache = TierCacheFactory.builder()
                .remoteCache(new InMemoryRemoteCache<>())
                .build()
                .getCache("c");
        cache.put("k", "original");
        cache.evict("k");
        cache.put("k", "original");

        assertFalse(cache.putIfAbsent("k", "intruder"));
        assertEquals("original", cache.get("k"));
    }

    @Test
    void winnerAfterExpirySucceeds() throws InterruptedException {
        TierCacheFactory factory = TierCacheFactory.builder()
                .defaults(new CacheSettings(10_000, Duration.ofMillis(100), null,
                        Duration.ofMillis(100), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
                .remoteCache(new InMemoryRemoteCache<>())
                .build();
        TierCache<String, String> cache = factory.getCache("c");
        assertTrue(cache.putIfAbsent("k", "v1"));
        Thread.sleep(250); // let the L2 entry expire
        assertTrue(cache.putIfAbsent("k", "v2"));
        assertEquals("v2", cache.get("k"));
    }
}
