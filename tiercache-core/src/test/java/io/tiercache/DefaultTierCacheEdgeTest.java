package io.tiercache;

import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.CountingRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Remaining LWW/degraded edge branches in DefaultTierCache. */
class DefaultTierCacheEdgeTest {

    @Test
    void equalVersionEventDoesNotEvict() {
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), new CountingRemoteCache<>(),
                CacheSettings.defaults(), true, null, null, new VersionGenerator(), null);
        cache.put("k", "v");
        Version current = cache.versionOfL1Entry("k");
        assertNotNull(current);
        cache.evictL1IfNewer("k", current); // equal, not newer
        assertEquals("v", cache.get("k"));
    }

    @Test
    void publishSkipsWhenNoHandlerOrNoVersion() {
        // Handler present but versioning off: nothing published, nothing breaks.
        java.util.List<InvalidationMessage> published = new java.util.ArrayList<>();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), new CountingRemoteCache<>(),
                CacheSettings.defaults(), true, null, null, null,
                new io.tiercache.spi.InvalidationHandler() {
                    @Override
                    public void onLocalWrite(String c, Object k, Version v, InvalidationMessage.Type t) {
                        published.add(new InvalidationMessage(c, k, v, v.instanceId(), t));
                    }

                    @Override
                    public void registerTarget(String c, io.tiercache.spi.InvalidationTarget t) {
                    }

                    @Override
                    public void close() {
                    }
                });
        cache.put("k", "v");
        cache.evict("k");
        cache.evictAll();
        assertEquals(0, published.size(), "no versions -> no events");
    }

    @Test
    void degradedLoaderMarkerGoesToL1Only() {
        io.tiercache.internal.CircuitBreaker breaker = new io.tiercache.internal.CircuitBreaker(
                io.tiercache.internal.CircuitBreaker.Config.defaults(),
                new io.tiercache.internal.CircuitBreaker.Listener() {
                    @Override
                    public void onOpen() {
                    }

                    @Override
                    public void onClose() {
                    }
                });
        breaker.onFailure();
        breaker.onFailure();
        breaker.onFailure();
        breaker.onFailure();
        breaker.onFailure(); // defaults: min 5 calls, 100% failures -> open
        assertTrue(breaker.isOpen());

        CacheSettings allow = new CacheSettings(10_000, Duration.ofMinutes(5), null,
                Duration.ofHours(1), 0.0, NullPolicy.allow(Duration.ofMinutes(1)), InvalidationMode.INVALIDATE, 64 * 1024);
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, allow, true, null, null,
                new VersionGenerator(), null, breaker, io.tiercache.spi.CacheMetricsListener.NOOP);
        assertNull(cache.getOrCompute("k", key -> null));
        assertEquals(0, l2.puts.get(), "degraded marker must not touch L2");
        assertEquals(LookupResult.CachedNull.instance().getClass(), cache.lookup("k").getClass());
    }

    @Test
    void putNullUnderDenyIsNoop() {
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), new CountingRemoteCache<>(),
                CacheSettings.defaults(), true, null, null, new VersionGenerator(), null);
        cache.putNull("k"); // deny: nothing happens
        assertEquals(LookupResult.Miss.instance().getClass(), cache.lookup("k").getClass());
    }

    @Test
    void lostRaceWithEmptyL2EvictsL1() {
        // putIfNewer loses and L2 has nothing (evicted meanwhile): L1 drops too.
        io.tiercache.spi.RemoteCache<String, String> rejecting = new io.tiercache.spi.RemoteCache<>() {
            @Override
            public StoredEntry<String> get(String key) {
                return null; // L2 empty after the race
            }

            @Override
            public void put(String key, StoredEntry<String> entry, Duration ttl) {
            }

            @Override
            public boolean putIfNewer(String key, StoredEntry<String> entry, Duration ttl) {
                return false;
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
        };
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), rejecting, CacheSettings.defaults(), true,
                null, null, new VersionGenerator(), null);
        cache.put("k", "v"); // loses race; L2 empty -> L1 evicted
        assertEquals(LookupResult.Miss.instance().getClass(), cache.lookup("k").getClass());
    }

    @Test
    void degradedEvictAndEvictAllSkipPublishAndTouchL1() {
        var breaker = new io.tiercache.internal.CircuitBreaker(
                new io.tiercache.internal.CircuitBreaker.Config(10, 0.5, 2, Duration.ofMinutes(1), 1),
                new io.tiercache.internal.CircuitBreaker.Listener() {
                    @Override public void onOpen() { }
                    @Override public void onClose() { }
                });
        breaker.onFailure();
        breaker.onFailure();
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, CacheSettings.defaults(), true,
                null, null, new VersionGenerator(), null, breaker, io.tiercache.spi.CacheMetricsListener.NOOP);
        cache.evict("k");
        cache.evictAll();
        assertEquals(0, l2.evicts.get(), "degraded evict must not touch L2");
    }

    @Test
    void markerFromL2ThroughAllReadPaths() {
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        l2.put("k", StoredEntry.nullMarker(new Version(1, UUID.randomUUID())), Duration.ofMinutes(1));
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, CacheSettings.defaults(), true,
                null, null, new VersionGenerator(), null);
        assertNull(cache.get("k"));
        assertEquals(LookupResult.CachedNull.instance().getClass(),
                new DefaultTierCache<>("c", new CountingLocalCache<>(), l2,
                        CacheSettings.defaults(), true, null, null, new VersionGenerator(), null)
                        .lookup("k").getClass());
    }

    @Test
    void degradedReadSkipsL2Entirely() {
        io.tiercache.internal.CircuitBreaker breaker = new io.tiercache.internal.CircuitBreaker(
                new io.tiercache.internal.CircuitBreaker.Config(10, 0.5, 2, Duration.ofMinutes(1), 1),
                new io.tiercache.internal.CircuitBreaker.Listener() {
                    @Override
                    public void onOpen() {
                    }

                    @Override
                    public void onClose() {
                    }
                });
        breaker.onFailure();
        breaker.onFailure();
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        l2.put("k", StoredEntry.ofValue("v"), Duration.ofMinutes(1));
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, CacheSettings.defaults(), true,
                null, null, new VersionGenerator(), null, breaker, io.tiercache.spi.CacheMetricsListener.NOOP);
        assertEquals(LookupResult.Miss.instance().getClass(), cache.lookup("k").getClass(),
                "miss: L2 skipped while degraded");
        assertEquals(0, l2.gets.get(), "no L2 access while open");
    }
}
