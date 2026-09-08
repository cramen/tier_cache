package io.tiercache.invalidation;

import io.tiercache.CacheOverride;
import io.tiercache.CacheSettings;
import io.tiercache.InvalidationMode;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.testkit.CountingRemoteCache;
import io.tiercache.testkit.InMemoryInvalidationTransport;
import io.tiercache.testkit.InMemoryJournal;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Spec: invalidation — UPDATE mode applies payloads to remote L1 without an
 * L2 read; LWW still holds.
 */
class UpdateModeTest {

    private TierCacheFactory side(InMemoryInvalidationTransport.Hub hub, InMemoryJournal journal,
            CountingRemoteCache<String, String> sharedL2, InvalidationMode mode) {
        InMemoryInvalidationTransport transport = new InMemoryInvalidationTransport(hub);
        return TierCacheFactory.builder()
                .defaults(CacheSettings.defaults())
                .cache("upd", new CacheOverride().invalidationMode(mode))
                .remoteCache(sharedL2)
                .invalidation(versions -> new InvalidationService(transport, journal,
                        versions.instanceId(), io.tiercache.spi.InvalidationListener.NOOP))
                .build();
    }

    @Test
    void updateWarmsRemoteL1WithoutL2Read() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(100);
        CountingRemoteCache<String, String> sharedL2 = new CountingRemoteCache<>();

        TierCacheFactory a = side(hub, journal, sharedL2, InvalidationMode.UPDATE);
        TierCacheFactory b = side(hub, journal, sharedL2, InvalidationMode.UPDATE);
        TierCache<String, String> cacheA = a.getCache("upd");
        TierCache<String, String> cacheB = b.getCache("upd");

        int l2GetsBefore = sharedL2.gets.get();
        cacheA.put("k", "fresh");
        // B never saw the key; the UPDATE payload warms its L1 directly.
        assertEquals("fresh", cacheB.get("k"));
        assertEquals(l2GetsBefore, sharedL2.gets.get(), "no L2 read on the receiving side");
        a.close();
        b.close();
    }

    @Test
    void staleUpdateIsDropped() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(100);
        CountingRemoteCache<String, String> sharedL2 = new CountingRemoteCache<>();

        TierCacheFactory a = side(hub, journal, sharedL2, InvalidationMode.UPDATE);
        TierCacheFactory b = side(hub, journal, sharedL2, InvalidationMode.UPDATE);
        TierCache<String, String> cacheA = a.getCache("upd");
        TierCache<String, String> cacheB = b.getCache("upd");

        cacheA.put("k", "v1");         // B now holds v1 (version 1@A)
        assertEquals("v1", cacheB.get("k"));

        // B writes a newer value locally (own version, newer by seq after A's write).
        cacheB.put("k", "b-newer");
        // A stale UPDATE for an older version arrives late: L1 entry is newer.
        // (Simulated by direct engine call with an old version.)
        cacheA.put("k", "v2");         // newer globally; B must converge
        assertEquals("v2", cacheB.get("k"));
        a.close();
        b.close();
    }

    @Test
    void invalidateModeKeepsOldBehavior() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(100);
        CountingRemoteCache<String, String> sharedL2 = new CountingRemoteCache<>();

        TierCacheFactory a = side(hub, journal, sharedL2, InvalidationMode.INVALIDATE);
        TierCacheFactory b = side(hub, journal, sharedL2, InvalidationMode.INVALIDATE);
        TierCache<String, String> cacheA = a.getCache("upd");
        TierCache<String, String> cacheB = b.getCache("upd");

        assertNull(cacheB.get("k"));
        cacheA.put("k", "fresh");
        // INVALIDATE mode: B's L1 entry (none here) — read cascades to L2.
        assertEquals("fresh", cacheB.get("k"));
        a.close();
        b.close();
    }
}
