package io.tiercache;

import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.CountingRemoteCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Versioned marker and LWW edge branches. */
class VersionedMarkerTest {

    private static final CacheSettings ALLOW = new CacheSettings(10_000, Duration.ofMinutes(5),
            null, Duration.ofHours(1), 0.0, NullPolicy.allow(Duration.ofMinutes(1)));

    private record Harness(DefaultTierCache<String, String> cache, List<InvalidationMessage> published,
            CountingLocalCache<String, String> l1, CountingRemoteCache<String, String> l2) {
    }

    private Harness harness(CacheSettings settings) {
        List<InvalidationMessage> published = new ArrayList<>();
        CountingLocalCache<String, String> l1 = new CountingLocalCache<>();
        CountingRemoteCache<String, String> l2 = new CountingRemoteCache<>();
        DefaultTierCache<String, String> cache = new DefaultTierCache<>("c", l1, l2,
                settings, true, null, null, new VersionGenerator(),
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
        return new Harness(cache, published, l1, l2);
    }

    @Test
    void versionedMarkerIsStoredAndPublished() {
        Harness h = harness(ALLOW);
        h.cache().putNull("k");
        assertEquals(1, h.published().size());
        assertTrue(h.l2().get("k").isNullMarker());
        assertTrue(h.l2().get("k").version() != null);
    }

    @Test
    void versionedLoaderNullStoresMarkerAndPublishes() {
        Harness h = harness(ALLOW);
        assertNull(h.cache().getOrCompute("k", key -> null));
        assertEquals(1, h.published().size());
        assertTrue(h.l2().get("k").isNullMarker());
    }

    @Test
    void unversionedEntryLosesToAnyEvent() {
        Harness h = harness(CacheSettings.defaults());
        h.l1().put("k", StoredEntry.ofValue("legacy"), Duration.ofMinutes(1)); // no version
        h.cache().evictL1IfNewer("k", new Version(1, UUID.randomUUID()));
        assertNull(h.cache().versionOfL1Entry("k"), "unversioned L1 entry is evicted by any event");
    }

    @Test
    void versionedPutWinnerStaysConsistent() {
        Harness h = harness(CacheSettings.defaults());
        h.cache().put("k", "v");
        assertEquals(1, h.published().size());
        assertEquals("v", h.cache().get("k"));
    }
}
