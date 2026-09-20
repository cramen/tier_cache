package io.tiercache.invalidation;

import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import io.tiercache.spi.InvalidationJournal;
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
    private static class FakeTarget implements InvalidationTarget {
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

    /**
     * Two writers, delayed publish (reviewer-reported): writer A journals a
     * row but its publish is delayed while the next rows are journaled AND
     * delivered. The cursor must never advance past A's unconsumed row —
     * otherwise the next reconnect skips it and stale L1 survives.
     */
    @Test
    void cursorNeverAdvancesPastAnUnconsumedRow() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(1000);
        UUID idA = UUID.randomUUID();
        UUID idW = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        ServiceSide a = side(idA, hub, journal, cache -> {
        });
        ServiceSide b = side(idB, hub, journal, cache -> {
        });
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("k", new Version(1, idA));
        b.service().registerTarget("c", targetB); // cursor: beginning

        // Row 1 is journaled but its live publish is delayed (never arrives).
        journal.append("c", new InvalidationMessage("c", "k", new Version(2, idA), idA,
                InvalidationMessage.Type.INVALIDATE));
        // The next 64 rows are journaled AND delivered — the 64th delivery
        // fires the cursor tick.
        for (int i = 3; i <= 66; i++) {
            Version v = new Version(i, idW);
            journal.append("c", new InvalidationMessage("c", "x" + i, v, idW,
                    InvalidationMessage.Type.INVALIDATE));
            a.service().onLocalWrite("c", "x" + i, v, InvalidationMessage.Type.INVALIDATE);
        }
        assertEquals(new Version(1, idA), targetB.entries.get("k"),
                "the delayed row must not be applied from thin air");

        b.transport().disconnect();
        b.transport().reconnect();
        assertNull(targetB.entries.get("k"),
                "the cursor must not have advanced past the unconsumed row: replay applies it");
        assertEquals(0, targetB.flushCount.get(), "nothing was trimmed: no flush expected");
        a.service().close();
        b.service().close();
    }

    /**
     * Write during replay (reviewer-reported): a row journaled between the
     * replay's last read and the cursor store must be picked up — the
     * cursor records the last row actually read, never the stream end.
     */
    @Test
    void writeDuringReplayIsNotSkipped() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(1000);
        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        ServiceSide b = side(idB, hub, journal, cache -> {
        });
        FakeTarget targetB = new FakeTarget() {
            @Override
            public void evictL1IfNewer(Object key, Version eventVersion) {
                super.evictL1IfNewer(key, eventVersion);
                if ("k3".equals(key)) {
                    // A new row lands mid-replay: after the replay's read,
                    // before its cursor store.
                    journal.append("c", new InvalidationMessage("c", "k4",
                            new Version(4, idA), idA, InvalidationMessage.Type.INVALIDATE));
                }
            }
        };
        targetB.entries.put("k2", new Version(1, idA));
        targetB.entries.put("k3", new Version(1, idA));
        targetB.entries.put("k4", new Version(1, idA));
        b.service().registerTarget("c", targetB);

        b.transport().disconnect();
        journal.append("c", new InvalidationMessage("c", "k2", new Version(2, idA), idA,
                InvalidationMessage.Type.INVALIDATE));
        journal.append("c", new InvalidationMessage("c", "k3", new Version(3, idA), idA,
                InvalidationMessage.Type.INVALIDATE));
        b.transport().reconnect();

        assertNull(targetB.entries.get("k2"));
        assertNull(targetB.entries.get("k3"));
        assertNull(targetB.entries.get("k4"),
                "a row journaled mid-replay is picked up, not skipped past the stored cursor");
        b.service().close();
    }

    /**
     * The cursor's own row trimmed since the last confirmation: prefix
     * integrity is unconfirmable — the flush path fires (with the signal)
     * instead of mistaking surviving rows for a contiguous prefix.
     */
    @Test
    void trimmedCursorRowForcesTheFlushPath() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(2); // tiny window
        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        ServiceSide a = side(idA, hub, journal, cache -> {
        });
        AtomicInteger overflows = new AtomicInteger();
        InMemoryInvalidationTransport transportB = new InMemoryInvalidationTransport(hub);
        RecordingMetrics metrics = new RecordingMetrics();
        InvalidationService b = new InvalidationService(transportB, journal, idB,
                cache -> overflows.incrementAndGet(), metrics);
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("k", new Version(1, idA));
        // Rows 1..3 (row 1 already trimmed); the cursor baselines at row 3.
        for (int i = 1; i <= 3; i++) {
            journal.append("c", new InvalidationMessage("c", "x" + i, new Version(i, idA), idA,
                    InvalidationMessage.Type.INVALIDATE));
        }
        b.registerTarget("c", targetB);
        // One live event past the baseline, then the receiver falls behind.
        Version v4 = new Version(4, idA);
        journal.append("c", new InvalidationMessage("c", "x4", v4, idA,
                InvalidationMessage.Type.INVALIDATE));
        a.service().onLocalWrite("c", "x4", v4, InvalidationMessage.Type.INVALIDATE);

        transportB.disconnect();
        for (int i = 5; i <= 8; i++) { // rows 5..8 trim the cursor row 3
            journal.append("c", new InvalidationMessage("c", "x" + i, new Version(i, idA), idA,
                    InvalidationMessage.Type.INVALIDATE));
        }
        transportB.reconnect();

        assertEquals(1, targetB.flushCount.get(),
                "unconfirmable cursor integrity must take the flush path");
        assertEquals(1, overflows.get(), "the flush is signaled to the listener");
        assertEquals(1, metrics.count(Direction.DROPPED), "the flush is signaled in metrics");
        a.service().close();
        b.close();
    }

    /**
     * Window overflow behind a delayed row: the service catches up directly
     * from the journal (the source of truth) — the delayed row is applied
     * without any reconnect, memory stays bounded, nothing is skipped.
     */
    @Test
    void windowOverflowFallsBackToJournalCatchUp() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(1000);
        UUID idA = UUID.randomUUID();
        UUID idW = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        ServiceSide a = side(idA, hub, journal, cache -> {
        });
        ServiceSide b = side(idB, hub, journal, cache -> {
        });
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("kd", new Version(1, idA));
        b.service().registerTarget("c", targetB);

        // The delayed row: journaled, never delivered.
        journal.append("c", new InvalidationMessage("c", "kd", new Version(2, idA), idA,
                InvalidationMessage.Type.INVALIDATE));
        // 129 subsequent events overflow the 128-entry applied window.
        for (int i = 3; i <= 131; i++) {
            Version v = new Version(i, idW);
            journal.append("c", new InvalidationMessage("c", "x" + i, v, idW,
                    InvalidationMessage.Type.INVALIDATE));
            a.service().onLocalWrite("c", "x" + i, v, InvalidationMessage.Type.INVALIDATE);
        }

        assertNull(targetB.entries.get("kd"),
                "the delayed row must be applied by journal catch-up, without a reconnect");
        assertEquals(0, targetB.flushCount.get(), "catch-up is not a flush: nothing was trimmed");
        a.service().close();
        b.service().close();
    }

    /**
     * First tick with a beginning cursor (registered against an empty
     * journal): there is no cursor row, so a literal row-existence check
     * would report a false loss. The counter-checked variant must advance
     * the cursor without any flush.
     */
    @Test
    void firstTickWithBeginningCursorIsNotAFalseLoss() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(1000);
        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        ServiceSide a = side(idA, hub, journal, cache -> {
        });
        InMemoryInvalidationTransport transportB = new InMemoryInvalidationTransport(hub);
        RecordingMetrics metrics = new RecordingMetrics();
        InvalidationService b = new InvalidationService(transportB, journal, idB, cache -> {
        }, metrics);
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("k", new Version(1, idA));
        b.registerTarget("c", targetB); // beginning cursor: empty journal

        for (int i = 2; i <= 65; i++) { // 64 deliveries: the tick fires
            Version v = new Version(i, idA);
            journal.append("c", new InvalidationMessage("c", "x" + i, v, idA,
                    InvalidationMessage.Type.INVALIDATE));
            a.service().onLocalWrite("c", "x" + i, v, InvalidationMessage.Type.INVALIDATE);
        }
        assertEquals(0, targetB.flushCount.get(), "a beginning cursor is not a loss");
        assertEquals(0, metrics.count(Direction.DROPPED));

        // And the advanced cursor replays only what is genuinely missed.
        transportB.disconnect();
        Version v66 = new Version(66, idA);
        journal.append("c", new InvalidationMessage("c", "k", v66, idA,
                InvalidationMessage.Type.INVALIDATE));
        transportB.reconnect();
        assertNull(targetB.entries.get("k"), "the missed row is replayed");
        assertEquals(0, targetB.flushCount.get(), "still no flush");
        a.service().close();
        b.close();
    }

    /**
     * A journal read failure during the reconnect check (e.g. the trim
     * counter is unreadable) is a possible loss: the flush path fires with
     * a warning — never "no loss".
     */
    @Test
    void journalReadFailureOnReconnectTakesTheFlushPath() {
        var hub = new InMemoryInvalidationTransport.Hub();
        InMemoryJournal delegate = new InMemoryJournal(100);
        InvalidationJournal failing = new InvalidationJournal() {
            @Override
            public String append(String cache, InvalidationMessage message) {
                return delegate.append(cache, message);
            }

            @Override
            public List<io.tiercache.spi.JournalRow> readRange(String cache, String cursorExclusive) {
                return delegate.readRange(cache, cursorExclusive);
            }

            @Override
            public io.tiercache.spi.CheckedRange checkedRead(String cache, String cursor, int maxRows) {
                throw new RuntimeException("trim counter unreadable");
            }

            @Override
            public String endCursor(String cache) {
                return delegate.endCursor(cache);
            }

            @Override
            public boolean isTrimmed(String cache, String cursor) {
                return delegate.isTrimmed(cache, cursor);
            }
        };
        UUID idB = UUID.randomUUID();
        AtomicInteger overflows = new AtomicInteger();
        InMemoryInvalidationTransport transportB = new InMemoryInvalidationTransport(hub);
        RecordingMetrics metrics = new RecordingMetrics();
        InvalidationService b = new InvalidationService(transportB, failing, idB,
                cache -> overflows.incrementAndGet(), metrics);
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("k", new Version(1, UUID.randomUUID()));
        b.registerTarget("c", targetB);

        transportB.disconnect();
        transportB.reconnect();

        assertEquals(1, targetB.flushCount.get(),
                "a failed read is unconfirmable integrity: flush, never \"no loss\"");
        assertEquals(1, overflows.get());
        assertEquals(1, metrics.count(Direction.DROPPED));
        b.close();
    }

    /**
     * Flush baseline ordering (reviewer-reported): a row journaled between
     * the L1 clear and the baseline establishment must stay ahead of the
     * cursor and be replayed — it is NOT covered by the clear, so it must
     * not be auto-accounted. The interleaving target performs the
     * concurrent activity inside the clear.
     */
    @Test
    void writeBetweenClearAndBaselineIsReplayed() {
        var hub = new InMemoryInvalidationTransport.Hub();
        var journal = new InMemoryJournal(2); // tiny window: forces the flush path
        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        ServiceSide a = side(idA, hub, journal, cache -> {
        });
        ServiceSide b = side(idB, hub, journal, cache -> {
        });
        FakeTarget targetB = new FakeTarget() {
            @Override
            public void evictAllL1() {
                super.evictAllL1();
                // Concurrent activity between the clear and the (former)
                // baseline read: L1 re-warms with the old value, then a new
                // invalidation is journaled whose live notification is lost.
                entries.put("k", new Version(1, idA));
                journal.append("c", new InvalidationMessage("c", "k", new Version(9, idA), idA,
                        InvalidationMessage.Type.INVALIDATE));
            }
        };
        targetB.entries.put("k", new Version(1, idA));
        for (int i = 1; i <= 3; i++) {
            journal.append("c", new InvalidationMessage("c", "x" + i, new Version(i, idA), idA,
                    InvalidationMessage.Type.INVALIDATE));
        }
        b.service().registerTarget("c", targetB); // cursor baselines at row 3

        b.transport().disconnect();
        for (int i = 4; i <= 6; i++) { // trims the cursor row: the flush path
            journal.append("c", new InvalidationMessage("c", "x" + i, new Version(i, idA), idA,
                    InvalidationMessage.Type.INVALIDATE));
        }
        b.transport().reconnect();
        assertEquals(1, targetB.flushCount.get(), "trimmed cursor: the flush fired");
        assertEquals(new Version(1, idA), targetB.entries.get("k"),
                "the stale re-warm is in place after the clear");

        b.transport().disconnect();
        b.transport().reconnect();
        assertNull(targetB.entries.get("k"),
                "the row journaled during the flush must be replayed, not baselined away");
        a.service().close();
        b.service().close();
    }

    /**
     * A failed baseline read must not move the cursor: the previous
     * confirmed position is kept (never advance past unread rows) while
     * the flush still proceeds.
     */
    @Test
    void failedBaselineReadKeepsTheConfirmedCursor() {
        var hub = new InMemoryInvalidationTransport.Hub();
        InMemoryJournal delegate = new InMemoryJournal(2);
        java.util.concurrent.atomic.AtomicBoolean failBaseline = new java.util.concurrent.atomic.AtomicBoolean();
        List<String> checkedReadCursors = new CopyOnWriteArrayList<>();
        InvalidationJournal journal = new InvalidationJournal() {
            @Override
            public String append(String cache, InvalidationMessage message) {
                return delegate.append(cache, message);
            }

            @Override
            public List<io.tiercache.spi.JournalRow> readRange(String cache, String cursorExclusive) {
                return delegate.readRange(cache, cursorExclusive);
            }

            @Override
            public io.tiercache.spi.CheckedRange checkedRead(String cache, String cursor, int maxRows) {
                checkedReadCursors.add(cursor);
                return delegate.checkedRead(cache, cursor, maxRows);
            }

            @Override
            public String endCursor(String cache) {
                if (failBaseline.getAndSet(false)) {
                    throw new RuntimeException("baseline read failed");
                }
                return delegate.endCursor(cache);
            }

            @Override
            public boolean isTrimmed(String cache, String cursor) {
                return delegate.isTrimmed(cache, cursor);
            }
        };
        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        InMemoryInvalidationTransport transportB = new InMemoryInvalidationTransport(hub);
        InvalidationService b = new InvalidationService(transportB, journal, idB, cache -> {
        });
        FakeTarget targetB = new FakeTarget();
        targetB.entries.put("k", new Version(1, idA));
        for (int i = 1; i <= 3; i++) {
            journal.append("c", new InvalidationMessage("c", "x" + i, new Version(i, idA), idA,
                    InvalidationMessage.Type.INVALIDATE));
        }
        b.registerTarget("c", targetB); // cursor = row 3

        transportB.disconnect();
        for (int i = 4; i <= 6; i++) { // trims the cursor row
            journal.append("c", new InvalidationMessage("c", "x" + i, new Version(i, idA), idA,
                    InvalidationMessage.Type.INVALIDATE));
        }
        failBaseline.set(true); // the flush's baseline read will fail once
        transportB.reconnect();
        assertEquals(1, targetB.flushCount.get());

        transportB.disconnect();
        transportB.reconnect();
        assertEquals(2, targetB.flushCount.get(),
                "the kept cursor is still trimmed: the flush path repeats honestly");
        assertEquals("3", checkedReadCursors.get(checkedReadCursors.size() - 1),
                "the cursor must keep the previous confirmed position after a failed baseline read");
        b.close();
    }
}
