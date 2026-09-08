package io.tiercache;

import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Versioned write paths: losing a version race converges L1 to L2;
 * InvalidationTarget applies events last-write-wins.
 */
class VersionedWriteTest {

    /** RemoteCache whose putIfNewer rejects (simulating a lost race). */
    private static final class RejectingRemoteCache implements RemoteCache<String, String> {
        StoredEntry<String> current;

        @Override
        public boolean putIfNewer(String key, StoredEntry<String> entry, Duration ttl) {
            return false;
        }

        @Override
        public StoredEntry<String> get(String key) {
            return current;
        }

        @Override
        public void put(String key, StoredEntry<String> entry, Duration ttl) {
        }

        @Override
        public void evict(String key) {
        }

        @Override
        public void clear() {
        }

        @Override
        public boolean setIfAbsent(String key, StoredEntry<String> entry, Duration ttl) {
            return true;
        }
    }

    @Test
    void losingPutConvergesL1ToL2WithoutPublishing() {
        RejectingRemoteCache l2 = new RejectingRemoteCache();
        l2.current = StoredEntry.ofValue("winner", new Version(99, java.util.UUID.randomUUID()));

        java.util.List<InvalidationMessage> published = new java.util.ArrayList<>();
        InvalidationMessage.Type[] types = new InvalidationMessage.Type[1];
        TierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, CacheSettings.defaults(), true,
                null, null, new VersionGenerator(),
                new io.tiercache.spi.InvalidationHandler() {
                    @Override
                    public void onLocalWrite(String c, Object k, Version v, InvalidationMessage.Type t) {
                        published.add(new InvalidationMessage(c, k, v, v.instanceId(), t));
                        types[0] = t;
                    }

                    @Override
                    public void registerTarget(String c, io.tiercache.spi.InvalidationTarget t) {
                    }

                    @Override
                    public void close() {
                    }
                });

        cache.put("k", "loser");
        assertEquals(0, published.size(), "a lost version race must not publish");
        assertEquals("winner", cache.get("k"), "L1 converged to the L2 winner");
    }

    @Test
    void invalidationTargetAppliesLww() {
        InMemoryRemoteCache<String, String> l2 = new InMemoryRemoteCache<>();
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c", l1, l2,
                CacheSettings.defaults(), true, null, null, new VersionGenerator(), null);

        cache.put("k", "v"); // version 1
        Version v1 = cache.versionOfL1Entry("k");
        assertNotNull(v1);

        // Older event: entry survives. Newer event: evicted.
        cache.evictL1IfNewer("k", new Version(0, java.util.UUID.randomUUID()));
        assertEquals("v", cache.get("k"));
        cache.evictL1IfNewer("k", new Version(Integer.MAX_VALUE, java.util.UUID.randomUUID()));
        assertNull(cache.versionOfL1Entry("k"));

        // EVICT_ALL clears L1.
        cache.put("a", "1");
        cache.evictAllL1();
        assertNull(cache.versionOfL1Entry("a"));
    }
}
