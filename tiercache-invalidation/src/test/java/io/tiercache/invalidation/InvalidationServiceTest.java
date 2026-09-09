package io.tiercache.invalidation;

import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import io.tiercache.spi.InvalidationTarget;
import io.tiercache.spi.CacheMetricsListener.Direction;
import io.tiercache.testkit.InMemoryInvalidationTransport;
import io.tiercache.testkit.InMemoryJournal;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void appliedEventsReachEventListenerInOrder() {
        var hub = new InMemoryInvalidationTransport.Hub();
        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        InvalidationService a = service(idA, hub, null);
        InvalidationService b = service(idB, hub, null);
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("k1", new Version(1, idB));
        targetB.entries.put("k2", new Version(1, idB));
        b.registerTarget("c", targetB);
        List<InvalidationMessage> received = new CopyOnWriteArrayList<>();
        List<Boolean> k1EvictedAtCallback = new CopyOnWriteArrayList<>();
        b.setEventListener((cache, event) -> {
            received.add(event);
            k1EvictedAtCallback.add(!targetB.entries.containsKey("k1"));
        });

        a.onLocalWrite("c", "k1", new Version(2, idA), InvalidationMessage.Type.INVALIDATE);
        a.onLocalWrite("c", "k2", new Version(3, idA), InvalidationMessage.Type.INVALIDATE);
        a.onLocalWrite("c", null, new Version(4, idA), InvalidationMessage.Type.EVICT_ALL);

        assertEquals(List.of(InvalidationMessage.Type.INVALIDATE,
                        InvalidationMessage.Type.INVALIDATE, InvalidationMessage.Type.EVICT_ALL),
                received.stream().map(InvalidationMessage::type).toList(),
                "listener receives applied events in arrival order");
        assertEquals("k1", received.get(0).key());
        assertTrue(k1EvictedAtCallback.get(0),
                "the event must already be applied when the listener runs");
        a.close();
        b.close();
    }

    @Test
    void ownWritesDoNotNotifyEventListener() {
        var hub = new InMemoryInvalidationTransport.Hub();
        UUID idA = UUID.randomUUID();
        InvalidationService a = service(idA, hub, null);
        FakeTarget targetA = new FakeTarget();
        targetA.entries.put("k", new Version(1, idA));
        a.registerTarget("c", targetA);
        List<InvalidationMessage> received = new CopyOnWriteArrayList<>();
        a.setEventListener((cache, event) -> received.add(event));

        a.onLocalWrite("c", "k", new Version(2, idA), InvalidationMessage.Type.INVALIDATE);

        assertTrue(received.isEmpty(), "own writes are not inbound events");
        a.close();
    }

    /** Transport double that records lifecycle calls and delivers on demand. */
    private static final class RecordingTransport
            implements io.tiercache.spi.InvalidationTransport {
        final AtomicInteger subscribes = new AtomicInteger();
        final AtomicInteger subscriptionCloses = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        private java.util.function.Consumer<InvalidationMessage> handler;

        @Override
        public void publish(InvalidationMessage message) {
        }

        @Override
        public AutoCloseable subscribe(String cache,
                java.util.function.Consumer<InvalidationMessage> h) {
            subscribes.incrementAndGet();
            this.handler = h;
            return subscriptionCloses::incrementAndGet;
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }

        void deliver(InvalidationMessage message) {
            handler.accept(message);
        }
    }

    private static final class RecordingMetrics implements io.tiercache.spi.CacheMetricsListener {
        final Map<Direction, AtomicInteger> directions = new ConcurrentHashMap<>();
        final AtomicInteger spansEnded = new AtomicInteger();

        @Override
        public void onInvalidation(String cache, Direction direction) {
            directions.computeIfAbsent(direction, d -> new AtomicInteger()).incrementAndGet();
        }

        @Override
        public void onInvalidationEnd(String cache, Object handle) {
            spansEnded.incrementAndGet();
        }

        int count(Direction direction) {
            return directions.getOrDefault(direction, new AtomicInteger()).get();
        }
    }

    @Test
    void closeReleasesSubscriptionsTransportAndTargets() throws Exception {
        RecordingTransport transport = new RecordingTransport();
        InvalidationService service = new InvalidationService(transport, null,
                UUID.randomUUID(), cache -> {
                });
        FakeTarget target = new FakeTarget();
        Version held = new Version(1, UUID.randomUUID());
        target.entries.put("k", held);
        service.registerTarget("c", target);

        service.close();
        assertEquals(1, transport.subscriptionCloses.get(), "the subscription is cancelled");
        assertEquals(1, transport.closes.get(), "the transport is closed");

        service.close();
        assertEquals(1, transport.subscriptionCloses.get(),
                "subscriptions are cleared: a second close does not re-cancel");

        transport.deliver(new InvalidationMessage("c", "k", new Version(9, UUID.randomUUID()),
                UUID.randomUUID(), InvalidationMessage.Type.INVALIDATE));
        assertEquals(held, target.entries.get("k"), "targets are cleared: late events apply nowhere");
    }

    @Test
    void registerTargetSubscribesOncePerCache() {
        RecordingTransport transport = new RecordingTransport();
        InvalidationService service = new InvalidationService(transport, null,
                UUID.randomUUID(), cache -> {
                });
        service.registerTarget("c", new FakeTarget());
        service.registerTarget("c", new FakeTarget());
        assertEquals(1, transport.subscribes.get(), "one subscription per cache");
        service.close();
    }

    @Test
    void sentReceivedAndSpanMetricsAreEmitted() {
        RecordingTransport transport = new RecordingTransport();
        RecordingMetrics metrics = new RecordingMetrics();
        UUID idB = UUID.randomUUID();
        InvalidationService b = new InvalidationService(transport, null, idB, cache -> {
        }, metrics);
        FakeTarget target = new FakeTarget();
        target.entries.put("k", new Version(1, idB));
        b.registerTarget("c", target);

        b.onLocalWrite("c", "k", new Version(2, idB), InvalidationMessage.Type.INVALIDATE);
        b.onLocalUpdate("c", "k", "v", new Version(3, idB));
        assertEquals(2, metrics.count(Direction.SENT), "both write kinds are counted");

        transport.deliver(new InvalidationMessage("c", "k", new Version(4, UUID.randomUUID()),
                UUID.randomUUID(), InvalidationMessage.Type.INVALIDATE));
        assertEquals(1, metrics.count(Direction.RECEIVED));
        assertEquals(1, metrics.spansEnded.get(), "the processing span is closed");
        b.close();
    }

    @Test
    void replayMetricsAreEmittedAndTheCursorAdvances() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(100);
        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        InvalidationService a = service(idA, hub, journal);
        InMemoryInvalidationTransport transportB = new InMemoryInvalidationTransport(hub);
        RecordingMetrics metrics = new RecordingMetrics();
        InvalidationService b = new InvalidationService(transportB, journal, idB, cache -> {
        }, metrics);
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("k", new Version(1, idA));
        b.registerTarget("c", targetB);

        transportB.disconnect();
        Version v2 = new Version(2, idA);
        journal.append("c", new InvalidationMessage("c", "k", v2, idA,
                InvalidationMessage.Type.INVALIDATE));
        a.onLocalWrite("c", "k", v2, InvalidationMessage.Type.INVALIDATE);

        transportB.reconnect();
        assertEquals(1, metrics.count(Direction.REPLAYED), "the missed event is replayed");

        transportB.disconnect();
        transportB.reconnect();
        assertEquals(1, metrics.count(Direction.REPLAYED),
                "the cursor advanced: nothing is replayed twice");
        a.close();
        b.close();
    }

    @Test
    void journalOverflowIsCountedAsDropped() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(1); // window of one
        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        InMemoryInvalidationTransport transportB = new InMemoryInvalidationTransport(hub);
        RecordingMetrics metrics = new RecordingMetrics();
        InvalidationService b = new InvalidationService(transportB, journal, idB, cache -> {
        }, metrics);
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("k", new Version(1, idA));
        b.registerTarget("c", targetB);

        transportB.disconnect();
        for (int i = 2; i <= 4; i++) { // three events, capacity one -> overflow
            journal.append("c", new InvalidationMessage("c", "k" + i, new Version(i, idA),
                    idA, InvalidationMessage.Type.INVALIDATE));
        }
        transportB.reconnect();

        assertEquals(1, metrics.count(Direction.DROPPED), "the overflow is surfaced");
        assertEquals(0, targetB.entries.size(), "overflow still flushes L1");
        b.close();
    }

    @Test
    void l2RecoveryWithoutJournalFlushesL1() {
        var hub = new InMemoryInvalidationTransport.Hub();
        UUID idA = UUID.randomUUID();
        InvalidationService a = service(idA, hub, null);
        FakeTarget targetA = new FakeTarget();
        targetA.entries.put("k", new Version(1, idA));
        a.registerTarget("c", targetA);

        a.onL2Recovery();

        assertEquals(1, targetA.flushCount.get(), "no journal: the honest fallback is a full flush");
        assertTrue(targetA.entries.isEmpty());
        a.close();
    }
}
