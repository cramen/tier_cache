package io.tiercache;

import io.tiercache.internal.CircuitBreaker;
import io.tiercache.internal.CircuitBreakerRemoteCache;
import io.tiercache.internal.DefaultTierCache;
import io.tiercache.internal.L2UnavailableException;
import io.tiercache.spi.*;
import io.tiercache.testkit.CountingLocalCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tiercache.spi.TaggedWriteOutcome.*;
import static org.junit.jupiter.api.Assertions.*;

class TaggedWriteOutcomeTest {
    private static final Duration TTL = Duration.ofMinutes(1);
    private static final Version NEWER = new Version(Long.MAX_VALUE, UUID.randomUUID());

    private static class Legacy implements RemoteCache<String, String> {
        int writes;
        @Override public StoredEntry<String> get(String key) { return null; }
        @Override public void put(String key, StoredEntry<String> entry, Duration ttl) { writes++; }
        @Override public void evict(String key) { }
        @Override public void clear() { }
        @Override public boolean setIfAbsent(String key, StoredEntry<String> entry, Duration ttl) { return false; }
    }

    private static final class Remote extends Legacy {
        TaggedWriteOutcome outcome = WON;
        StoredEntry<String> current;
        int reads;
        Runnable duringWrite = () -> { };
        @Override public boolean supportsTaggedWriteOutcomes() { return true; }
        @Override public StoredEntry<String> get(String key) { reads++; return current; }
        @Override public TaggedWriteOutcome putTaggedIfNewer(String key, StoredEntry<String> entry,
                Duration ttl, String[] tags) {
            writes++;
            duringWrite.run();
            return outcome;
        }
    }

    private static CircuitBreaker breaker(Duration delay, int probes) {
        return new CircuitBreaker(new CircuitBreaker.Config(10, 0.5, 1, delay, probes),
                new CircuitBreaker.Listener() {
                    @Override public void onOpen() { }
                    @Override public void onClose() { }
                });
    }

    private static final class Harness {
        final CountingLocalCache<String, String> local = new CountingLocalCache<>();
        final CountingLocalCache<String, String> receiverLocal = new CountingLocalCache<>();
        final AtomicInteger publications = new AtomicInteger();
        final DefaultTierCache<String, String> cache;
        Harness(RemoteCache<String, String> remote, CircuitBreaker breaker, boolean versioned) {
            var settings = new CacheSettings(100, Duration.ofSeconds(10), null, TTL, 0,
                    NullPolicy.deny(), InvalidationMode.UPDATE, 65536);
            var receiver = new DefaultTierCache<String, String>("c", receiverLocal, remote,
                    settings, true, null, null, new VersionGenerator(), null);
            InvalidationHandler handler = new InvalidationHandler() {
                @Override public void onLocalWrite(String c, Object k, Version v, InvalidationMessage.Type t) {
                    publications.incrementAndGet();
                }
                @Override public void onLocalUpdate(String c, Object k, Object value, Version v) {
                    publications.incrementAndGet();
                    receiver.applyUpdateL1(k, value, v);
                }
                @Override public void registerTarget(String c, InvalidationTarget t) { }
                @Override public void close() { }
            };
            cache = new DefaultTierCache<>("c", local, new CircuitBreakerRemoteCache<>(remote, breaker),
                    settings, true, null, null, versioned ? new VersionGenerator() : null,
                    handler, breaker, CacheMetricsListener.NOOP);
        }
    }

    @Test void lostWriteConvergesOnceWithoutPublishingToEmptyUpdateReceiverOrLoading() {
        Remote remote = new Remote();
        remote.outcome = LOST;
        remote.current = StoredEntry.ofValue("winner", NEWER);
        Harness h = new Harness(remote, breaker(TTL, 1), true);
        h.cache.put("k", "loser", "old");
        assertEquals("winner", h.local.get("k").value());
        assertEquals(1, remote.reads);
        assertEquals(1, remote.writes);
        assertEquals(0, h.publications.get());
        assertNull(h.receiverLocal.get("k"));
        assertEquals("winner", h.cache.getOrCompute("k", key -> { throw new AssertionError("loader invoked"); }));
        assertEquals(1, remote.reads);
    }

    @Test void lostWriteWithAbsentWinnerEvictsLocalCandidate() {
        Remote remote = new Remote(); remote.outcome = LOST;
        Harness h = new Harness(remote, breaker(TTL, 1), true);
        h.local.put("k", StoredEntry.ofValue("stale"), TTL);
        h.cache.put("k", "loser", "tag");
        assertNull(h.local.get("k"));
        assertNull(h.receiverLocal.get("k"));
        assertEquals(1, remote.reads);
        assertEquals(0, h.publications.get());
    }

    @Test void winningWriteWarmsAndPublishes() {
        Remote remote = new Remote(); Harness h = new Harness(remote, breaker(TTL, 1), true);
        h.cache.put("k", "winner", "tag");
        assertEquals("winner", h.local.get("k").value());
        assertEquals("winner", h.receiverLocal.get("k").value());
        assertEquals(1, h.publications.get());
        assertEquals(0, remote.reads);
    }

    @Test void winningWriteCannotWarmAcrossGenerationChange() {
        Remote remote = new Remote(); Harness h = new Harness(remote, breaker(TTL, 1), true);
        remote.duringWrite = h.cache::evictAllL1;
        h.cache.put("k", "winner", "tag");
        assertNull(h.local.get("k"));
        assertEquals(1, h.publications.get(), "the remote win is still confirmed");
    }

    @Test void winningWriteCannotWarmAcrossNewerKeyBarrier() {
        Remote remote = new Remote(); Harness h = new Harness(remote, breaker(TTL, 1), true);
        remote.duringWrite = () -> h.cache.evictL1IfNewer("k", NEWER);
        h.cache.put("k", "winner", "tag");
        assertNull(h.local.get("k"));
        assertEquals(1, h.publications.get());
    }

    @Test void openBreakerFallsBackLocallyWithoutAttemptOrPublication() {
        Remote remote = new Remote(); CircuitBreaker b = breaker(TTL, 1); b.onFailure();
        Harness h = new Harness(remote, b, true);
        h.cache.put("k", "local", "tag");
        assertEquals("local", h.local.get("k").value());
        assertEquals(0, remote.writes);
        assertEquals(0, h.publications.get());
    }

    @Test void exhaustedHalfOpenFallsBackLocally() {
        Remote remote = new Remote(); CircuitBreaker b = breaker(Duration.ZERO, 1); b.onFailure();
        CircuitBreaker.Permit occupied = b.tryAcquirePermit(); assertNotNull(occupied);
        Harness h = new Harness(remote, b, true);
        h.cache.put("k", "local", "tag");
        assertEquals("local", h.local.get("k").value());
        assertEquals(0, remote.writes);
        assertEquals(0, h.publications.get());
        assertEquals(BreakerState.HALF_OPEN, b.state()); occupied.cancel();
    }

    @Test void admittedFailureOpensBreakerAndFallsBackLocally() {
        Remote remote = new Remote(); CircuitBreaker b = breaker(TTL, 1);
        remote.duringWrite = () -> { throw new IllegalStateException("outage"); };
        Harness h = new Harness(remote, b, true); h.cache.put("k", "local", "tag");
        assertEquals(BreakerState.OPEN, b.state());
        assertEquals("local", h.local.get("k").value());
        assertEquals(1, remote.writes);
        assertEquals(0, h.publications.get());
    }

    @Test void eachTaggedAttemptUsesExactlyOneProbe() {
        Remote remote = new Remote(); CircuitBreaker b = breaker(Duration.ZERO, 2); b.onFailure();
        Harness h = new Harness(remote, b, true);
        h.cache.put("k", "first", "tag");
        assertEquals(BreakerState.HALF_OPEN, b.state());
        h.cache.put("k", "second", "tag");
        assertEquals(BreakerState.CLOSED, b.state());
        assertEquals(2, remote.writes);
    }

    @Test void losingAttemptAndConvergenceReadAreSeparateSuccessfulProbes() {
        Remote remote = new Remote(); remote.outcome = LOST;
        remote.current = StoredEntry.ofValue("winner", NEWER);
        CircuitBreaker b = breaker(Duration.ZERO, 2); b.onFailure();
        Harness h = new Harness(remote, b, true); h.cache.put("k", "loser", "tag");
        assertEquals(BreakerState.CLOSED, b.state());
        assertEquals(1, remote.writes); assertEquals(1, remote.reads);
    }

    @Test void legacyVersionedSpiIsUnsupportedEvenWhileOpen() {
        for (boolean open : new boolean[]{false, true}) {
            Legacy legacy = new Legacy(); CircuitBreaker b = breaker(TTL, 1);
            if (open) b.onFailure();
            BreakerState before = b.state(); Harness h = new Harness(legacy, b, true);
            assertThrows(CacheConfigurationException.class, () -> h.cache.put("k", "bad", "tag"));
            assertEquals(before, b.state()); assertNull(h.local.get("k"));
            assertEquals(0, legacy.writes); assertEquals(0, h.publications.get());
            assertEquals(UNSUPPORTED, new CircuitBreakerRemoteCache<>(legacy, b)
                    .putTaggedIfNewer("k", StoredEntry.ofValue("bad", NEWER), TTL, new String[]{"tag"}));
        }
    }

    @Test void unversionedLegacySpiDelegatesAndWarms() {
        Legacy legacy = new Legacy(); Harness h = new Harness(legacy, breaker(TTL, 1), false);
        h.cache.put("k", "ok", "tag");
        assertEquals(1, legacy.writes); assertEquals("ok", h.local.get("k").value());
        assertEquals(0, h.publications.get());
        assertEquals(UNSUPPORTED, legacy.putTaggedIfNewer("k", StoredEntry.ofValue("bad", NEWER), TTL, new String[]{"tag"}));
        assertEquals(1, legacy.writes);
    }

    @Test void unsupportedResultReturnsProbeWithoutSuccessOrFailure() {
        Remote remote = new Remote(); remote.outcome = UNSUPPORTED;
        CircuitBreaker b = breaker(Duration.ZERO, 1); b.onFailure(); Harness h = new Harness(remote, b, true);
        for (int i = 0; i < 3; i++) {
            assertThrows(CacheConfigurationException.class, () -> h.cache.put("k", "bad", "tag"));
            assertEquals(BreakerState.HALF_OPEN, b.state());
        }
        assertEquals(3, remote.writes); assertNull(h.local.get("k"));
        remote.outcome = WON; h.cache.put("k", "ok", "tag");
        assertEquals(BreakerState.CLOSED, b.state());
    }

    @Test void neutralCompletionDoesNotPolluteClosedFailureWindow() {
        Remote remote = new Remote(); remote.outcome = UNSUPPORTED;
        CircuitBreaker b = new CircuitBreaker(new CircuitBreaker.Config(10, 0.5, 2, TTL, 1),
                new CircuitBreaker.Listener() { public void onOpen() { } public void onClose() { } });
        Harness h = new Harness(remote, b, true);
        assertThrows(CacheConfigurationException.class, () -> h.cache.put("k", "bad", "tag"));
        b.onFailure(); assertEquals(BreakerState.CLOSED, b.state());
        b.onFailure(); assertEquals(BreakerState.OPEN, b.state());
    }

    @Test void permitIsOnceOnlyAndCannotRetireAnotherEpisode() {
        CircuitBreaker b = breaker(Duration.ZERO, 1); b.onFailure();
        var old = b.tryAcquirePermit(); assertNotNull(old); b.onFailure();
        var current = b.tryAcquirePermit(); assertNotNull(current);
        old.cancel(); assertNull(b.tryAcquirePermit());
        current.cancel(); current.success(); current.failure();
        assertEquals(BreakerState.HALF_OPEN, b.state());
        var next = b.tryAcquirePermit(); assertNotNull(next); next.success();
        assertEquals(BreakerState.CLOSED, b.state());
    }

    @Test void nestedRejectionAndErrorRetireAdmissionNeutrally() {
        Remote remote = new Remote(); CircuitBreaker b = breaker(Duration.ZERO, 1); b.onFailure();
        Harness h = new Harness(remote, b, true);
        remote.duringWrite = () -> { throw L2UnavailableException.OPEN; };
        h.cache.put("k", "local", "tag");
        assertEquals(BreakerState.HALF_OPEN, b.state());
        remote.duringWrite = () -> { throw new AssertionError("fatal"); };
        assertThrows(AssertionError.class, () -> h.cache.put("k", "bad", "tag"));
        remote.duringWrite = () -> { }; h.cache.put("k", "ok", "tag");
        assertEquals(BreakerState.CLOSED, b.state());
        assertEquals(1, h.publications.get());
    }
}
