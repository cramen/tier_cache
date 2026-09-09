package io.tiercache;

import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.CacheMetricsListener.Outcome;
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

    @Test
    void getOrComputeServesL1HitWithoutCallingTheLoader() {
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), new CountingRemoteCache<>(),
                CacheSettings.defaults(), true, null, null, new VersionGenerator(), null);
        cache.put("k", "v");
        String value = cache.getOrCompute("k", key -> {
            throw new AssertionError("loader must not run on an L1 hit");
        });
        assertEquals("v", value);
    }

    @Test
    void putIfAbsentWinsOnceThenLoses() {
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                l1, new CountingRemoteCache<>(), CacheSettings.defaults(), true,
                null, null, new VersionGenerator(), null);
        assertTrue(cache.putIfAbsent("k", "first"));
        org.junit.jupiter.api.Assertions.assertFalse(cache.putIfAbsent("k", "second"),
                "the key already exists in L2");
        assertEquals("first", cache.get("k"), "the losing write must not overwrite");
    }

    @Test
    void evictAllClearsBothLevels() {
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, CacheSettings.defaults(), true,
                null, null, new VersionGenerator(), null);
        cache.put("k", "v"); // warms L1 and L2
        cache.evictAll();
        assertNull(cache.get("k"), "L1 must be cleared");
        assertEquals(1, l2.gets.get(), "get after evictAll misses L1 and probes L2");
    }

    @Test
    void lookupL2HitWarmsL1AndReportsOutcomes() {
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        l2.put("k", StoredEntry.ofValue("v"), Duration.ofMinutes(1));
        java.util.List<String> outcomes = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.concurrent.atomic.AtomicInteger l2Spans = new java.util.concurrent.atomic.AtomicInteger();
        io.tiercache.spi.CacheMetricsListener metrics = new io.tiercache.spi.CacheMetricsListener() {
            @Override
            public void onRequest(String cache, Outcome outcome) {
                outcomes.add(outcome.name());
            }

            @Override
            public void onL2OperationEnd(String cache, String operation, boolean hit, Object handle) {
                l2Spans.incrementAndGet();
            }
        };
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, CacheSettings.defaults(), true,
                null, null, new VersionGenerator(), null, null, metrics);

        assertEquals(LookupResult.hit("v"), cache.lookup("k"));
        assertEquals(LookupResult.hit("v"), cache.lookup("k"));
        assertEquals(java.util.List.of("L2_HIT", "L1_HIT"), outcomes,
                "the second lookup must be served from the warmed L1");
        assertEquals(1, l2.gets.get(), "L2 read once");
        assertEquals(1, l2Spans.get(), "the L2 read span is closed");

        assertEquals(LookupResult.Miss.instance().getClass(), cache.lookup("absent").getClass());
        assertEquals("MISS", outcomes.get(2));
    }

    @Test
    void putNullStoresTheMarkerInL1WithTheMarkerTtl() {
        CacheSettings allow = new CacheSettings(10_000, Duration.ofMinutes(5), null,
                Duration.ofHours(1), 0.0, NullPolicy.allow(Duration.ofMinutes(1)),
                InvalidationMode.INVALIDATE, 64 * 1024);
        io.tiercache.testkit.RecordingLocalCache<String, String> l1 =
                new io.tiercache.testkit.RecordingLocalCache<>();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                l1, new CountingRemoteCache<>(), allow, true,
                null, null, new VersionGenerator(), null);
        cache.putNull("k");
        assertEquals(LookupResult.CachedNull.instance().getClass(), cache.lookup("k").getClass(),
                "the marker must be in L1");
        assertEquals(1, l1.recordedTtls().size());
        assertTrue(l1.recordedTtls().get(0).compareTo(Duration.ofMinutes(1)) <= 0,
                "the marker TTL override applies, not the L1 write TTL (5 min)");
    }

    @Test
    void degradedEvictAndEvictAllDoNotPublish() {
        var breaker = new io.tiercache.internal.CircuitBreaker(
                new io.tiercache.internal.CircuitBreaker.Config(10, 0.5, 2, Duration.ofMinutes(1), 1),
                new io.tiercache.internal.CircuitBreaker.Listener() {
                    @Override public void onOpen() { }
                    @Override public void onClose() { }
                });
        breaker.onFailure();
        breaker.onFailure();
        java.util.List<InvalidationMessage> published = new java.util.ArrayList<>();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), new CountingRemoteCache<>(),
                CacheSettings.defaults(), true, null, null, new VersionGenerator(),
                recordingHandler(published), breaker, io.tiercache.spi.CacheMetricsListener.NOOP);
        cache.evict("k");
        cache.evictAll();
        assertEquals(0, published.size(), "degraded writes never publish (no journal row either)");
    }

    @Test
    void degradedTaggedPutKeepsL2Untouched() {
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
        cache.put("k", "v", "tag");
        assertEquals(0, l2.puts.get(), "tagged put degrades to L1 only");
        assertEquals("v", cache.get("k"));
    }

    @Test
    void degradedEvictByTagIsANoOp() {
        var breaker = new io.tiercache.internal.CircuitBreaker(
                new io.tiercache.internal.CircuitBreaker.Config(10, 0.5, 2, Duration.ofMinutes(1), 1),
                new io.tiercache.internal.CircuitBreaker.Listener() {
                    @Override public void onOpen() { }
                    @Override public void onClose() { }
                });
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        l2.putTagged("k", StoredEntry.ofValue("v"), Duration.ofMinutes(1), new String[]{"tag"});
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                new CountingLocalCache<>(), l2, CacheSettings.defaults(), true,
                null, null, new VersionGenerator(), null, breaker, io.tiercache.spi.CacheMetricsListener.NOOP);
        breaker.onFailure();
        breaker.onFailure();
        cache.evictByTag("tag");
        assertEquals(0, l2.gets.get() + l2.evicts.get(), "no L2 access while open");
        assertEquals("v", l2.get("k").value(), "tagged keys survive a degraded evictByTag");
    }

    @Test
    void entryPastTheStaleWindowIsAMissAndDoesNotWarmL1() {
        CacheSettings stale = new CacheSettings(10_000, Duration.ofMinutes(5), null,
                Duration.ofSeconds(30), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE,
                64 * 1024, Duration.ofSeconds(30), false, Duration.ofSeconds(1));
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        UUID id = UUID.randomUUID();
        // Written long past TTL + stale window.
        l2.put("k", StoredEntry.ofValue("v", new Version(1, id),
                System.currentTimeMillis() - Duration.ofMinutes(10).toMillis()), Duration.ofHours(1));
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        java.util.List<String> outcomes = new java.util.concurrent.CopyOnWriteArrayList<>();
        io.tiercache.spi.CacheMetricsListener metrics = new io.tiercache.spi.CacheMetricsListener() {
            @Override
            public void onRequest(String cache, Outcome outcome) {
                outcomes.add(outcome.name());
            }
        };
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c",
                l1, l2, stale, true, null, null, new VersionGenerator(), null, null, metrics);
        assertNull(cache.get("k"), "past the stale window: hard miss");
        assertEquals(java.util.List.of("MISS"), outcomes);
        assertEquals(0, l1.puts.get(), "an expired entry must not warm L1");
    }

    private static io.tiercache.spi.InvalidationHandler recordingHandler(
            java.util.List<InvalidationMessage> published) {
        return new io.tiercache.spi.InvalidationHandler() {
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
        };
    }
}
