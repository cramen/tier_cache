package io.tiercache.invalidation;

import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import io.tiercache.VersionGenerator;
import io.tiercache.spi.InvalidationTarget;
import io.tiercache.testkit.InMemoryInvalidationTransport;
import io.tiercache.testkit.InMemoryJournal;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Spec: invalidation — the engine: publish, drop-own, last-write-wins,
 * replay, overflow flush.
 */
class InvalidationServiceTest {

    /** Fake L1 side: key -> version currently held. */
    private static final class FakeTarget implements InvalidationTarget {
        final Map<Object, Version> entries = new ConcurrentHashMap<>();
        final AtomicInteger flushCount = new AtomicInteger();

        @Override
        public Version versionOfL1Entry(Object key) {
            return entries.get(key);
        }

        @Override
        public void evictL1IfNewer(Object key, Version eventVersion) {
            entries.computeIfPresent(key, (k, current) ->
                    current == null || eventVersion.compareTo(current) > 0 ? null : current);
        }

        @Override
        public void evictAllL1() {
            entries.clear();
            flushCount.incrementAndGet();
        }
    }

    private static InvalidationService service(UUID origin, InMemoryInvalidationTransport.Hub hub,
            InMemoryJournal journal) {
        return new InvalidationService(new InMemoryInvalidationTransport(hub), journal,
                origin, cache -> {
                });
    }

    private record ServiceSide(InvalidationService service, InMemoryInvalidationTransport transport) {
    }

    private static ServiceSide side(UUID origin, InMemoryInvalidationTransport.Hub hub,
            InMemoryJournal journal, io.tiercache.spi.InvalidationListener listener) {
        InMemoryInvalidationTransport transport = new InMemoryInvalidationTransport(hub);
        return new ServiceSide(new InvalidationService(transport, journal, origin, listener),
                transport);
    }

    @Test
    void localWriteIsPublishedToOthers() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(100);
        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        InvalidationService a = service(idA, hub, journal);
        InvalidationService b = service(idB, hub, journal);
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("k", new Version(1, idA));
        b.registerTarget("c", targetB);

        a.onLocalWrite("c", "k", new Version(2, idA), InvalidationMessage.Type.INVALIDATE);

        assertNull(targetB.entries.get("k"), "B's stale entry must be evicted");
        a.close();
        b.close();
    }

    @Test
    void ownMessagesAreIgnored() {
        var hub = new InMemoryInvalidationTransport.Hub();
        UUID idA = UUID.randomUUID();
        InvalidationService a = service(idA, hub, new InMemoryJournal(100));
        FakeTarget targetA = new FakeTarget();
        targetA.entries.put("k", new Version(2, idA));
        a.registerTarget("c", targetA);

        a.onLocalWrite("c", "k", new Version(2, idA), InvalidationMessage.Type.INVALIDATE);

        assertEquals(new Version(2, idA), targetA.entries.get("k"),
                "own fresh entry must survive its own invalidation echo");
        a.close();
    }

    @Test
    void staleEventDoesNotEvictNewerEntry() {
        var hub = new InMemoryInvalidationTransport.Hub();
        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        InvalidationService a = service(idA, hub, null);
        InvalidationService b = service(idB, hub, null);
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("k", new Version(5, idB)); // B holds a fresh write
        b.registerTarget("c", targetB);

        // An older event arrives late.
        a.onLocalWrite("c", "k", new Version(3, idA), InvalidationMessage.Type.INVALIDATE);

        assertEquals(new Version(5, idB), targetB.entries.get("k"), "newer entry survives");
        a.close();
        b.close();
    }

    @Test
    void evictAllClearsRemoteL1() {
        var hub = new InMemoryInvalidationTransport.Hub();
        UUID idA = UUID.randomUUID();
        InvalidationService a = service(idA, hub, null);
        InvalidationService b = service(idB(), hub, null);
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("k1", new Version(1, idA));
        targetB.entries.put("k2", new Version(9, idB()));
        b.registerTarget("c", targetB);

        a.onLocalWrite("c", null, new Version(10, idA), InvalidationMessage.Type.EVICT_ALL);

        assertEquals(0, targetB.entries.size());
        a.close();
        b.close();
    }

    @Test
    void shortDisconnectHealsByReplay() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(100);
        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        ServiceSide a = side(idA, hub, journal, cache -> {
        });
        ServiceSide b = side(idB, hub, journal, cache -> {
        });
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("k", new Version(1, idA));
        b.service().registerTarget("c", targetB);

        b.transport().disconnect(); // the RECEIVER is offline: event will be lost
        Version v2 = new Version(2, idA);
        // The write is journaled (transport-side) but the event is lost.
        journal.append("c", new InvalidationMessage("c", "k", v2, idA,
                InvalidationMessage.Type.INVALIDATE));
        a.service().onLocalWrite("c", "k", v2, InvalidationMessage.Type.INVALIDATE);
        assertEquals(new Version(1, idA), targetB.entries.get("k"), "lost event: still stale");

        b.transport().reconnect();
        assertNull(targetB.entries.get("k"), "replay must heal the missed invalidation");
        a.service().close();
        b.service().close();
    }

    @Test
    void overflowFlushesWholeL1WithListenerEvent() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(2); // tiny window
        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        ServiceSide a = side(idA, hub, journal, cache -> {
        });
        AtomicInteger overflows = new AtomicInteger();
        ServiceSide b = side(idB, hub, journal, cache -> overflows.incrementAndGet());
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("k", new Version(1, idA));
        b.service().registerTarget("c", targetB);

        b.transport().disconnect();
        for (int i = 2; i <= 6; i++) { // 5 events, capacity 2 -> window overflow
            journal.append("c", new InvalidationMessage("c", "k" + i, new Version(i, idA),
                    idA, InvalidationMessage.Type.INVALIDATE));
        }
        b.transport().reconnect();

        assertEquals(0, targetB.entries.size(), "overflow must flush L1");
        assertEquals(1, overflows.get(), "listener must be notified");
        a.service().close();
        b.service().close();
    }

    private static UUID idB() {
        return UUID.randomUUID();
    }
}
