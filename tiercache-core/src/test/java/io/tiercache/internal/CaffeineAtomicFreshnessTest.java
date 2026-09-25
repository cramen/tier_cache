package io.tiercache.internal;

import io.tiercache.*;
import io.tiercache.spi.*;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.LongSupplier;
import static io.tiercache.spi.LocalFreshnessResult.State.*;
import static org.junit.jupiter.api.Assertions.*;

class CaffeineAtomicFreshnessTest {
    static long minute(long m) { return 1 + Duration.ofMinutes(m).toNanos(); }
    static CaffeineLocalCache<String, String> provider(LongSupplier clock) {
        return new CaffeineLocalCache<>(LocalFreshnessTest.settings(30, 2L, 10, NullPolicy.deny()), clock);
    }
    static StoredEntry<String> entry(boolean marker) {
        var version = new Version(1, UUID.randomUUID());
        var barrier = new Version(2, version.instanceId());
        StoredEntry<String> raw = marker ? StoredEntry.nullMarker(version, 123) : StoredEntry.ofValue("value", version, 123);
        return raw.withLocalFreshness(new StoredEntry.LocalFreshness(minute(30), minute(40), minute(40), minute(40), barrier));
    }
    static LocalFreshnessResult<String> read(CaffeineLocalCache<String, String> cache) {
        return cache.readFreshness("x", Duration.ofMinutes(2), Duration.ofMinutes(10));
    }

    @Test void currentFreshHolderPreservesIdentityFieldsAndShortensOnlyLogicalLifetime() {
        var now = new AtomicLong(1); var cache = provider(now::get); var old = entry(false);
        cache.put("x", old, Duration.ofMinutes(40)); now.set(minute(1));
        var result = read(cache); var value = result.entry();
        assertTrue(cache.supportsAtomicFreshnessRead()); assertEquals(FRESH, result.state());
        assertNotSame(old, value); assertSame(value, cache.get("x"));
        assertEquals(old.value(), value.value()); assertEquals(old.version(), value.version());
        assertEquals(123, value.writeTimestampMillis()); assertEquals(old.localFreshness().highestSeen(), value.localFreshness().highestSeen());
        assertEquals(minute(3), value.localFreshness().logicalDeadlineNanos());
        assertEquals(minute(13), value.localFreshness().staleServeUntilNanos());
        assertEquals(minute(40), value.localFreshness().storeRetentionFloorNanos());
        assertEquals(minute(40), value.localFreshness().retentionUntilNanos());
        for (int m = 3; m < 40; m++) {
            now.set(minute(m)); var stale = read(cache);
            assertEquals(m < 13 ? STALE_ALLOWED : EXPIRED, stale.state());
            assertSame(value, stale.entry());
        }
        now.set(minute(40)); assertNull(read(cache).entry()); assertNull(cache.get("x"));
    }

    @Test void unknownHolderKeepsItsOriginalPhysicalDeadline() {
        var now = new AtomicLong(1); var cache = provider(now::get); var unknown = StoredEntry.ofValue("unknown");
        cache.put("x", unknown, Duration.ofMinutes(5));
        for (int m = 1; m < 5; m++) { now.set(minute(m)); var r = read(cache); assertEquals(EXPIRED, r.state()); assertSame(unknown, r.entry()); }
        now.set(minute(5)); assertNull(read(cache).entry());
    }

    @Test void markerVersionAndWriteTimestampSurviveRefresh() {
        var now = new AtomicLong(1); var cache = provider(now::get); var marker = entry(true);
        cache.put("x", marker, Duration.ofMinutes(40)); now.set(minute(1)); var result = read(cache);
        assertTrue(result.entry().isNullMarker()); assertEquals(marker.version(), result.entry().version());
        assertEquals(123, result.entry().writeTimestampMillis()); assertEquals(FRESH, result.state());
        cache.evict("x"); assertNull(read(cache).entry()); assertNull(cache.get("x"));
        cache.put("x", marker, Duration.ofMinutes(39)); cache.clear(); assertNull(read(cache).entry());
    }

    @Test void legacyRelativeExpiryAndUnsupportedCapabilityRemainUnchanged() {
        var now = new AtomicLong(1); var cache = new CaffeineLocalCache<String, String>(CacheSettings.defaults(), now::get);
        assertFalse(cache.supportsAtomicFreshnessRead());
        assertThrows(UnsupportedOperationException.class, () -> read(cache));
        cache.put("x", StoredEntry.ofValue("v"), Duration.ofMinutes(5));
        now.set(minute(4)); cache.put("x", StoredEntry.ofValue("new"), Duration.ofMinutes(5));
        now.set(minute(8)); assertEquals("new", cache.get("x").value());
        now.set(minute(9)); assertNull(cache.get("x"));
    }

    @Test void invalidDurationInputsFailBeforeMutatingTheEntry() {
        var cache = provider(() -> 1L); var value = entry(false); cache.put("x", value, Duration.ofMinutes(40));
        assertThrows(IllegalArgumentException.class, () -> cache.readFreshness("x", Duration.ZERO, Duration.ofMinutes(1)));
        assertThrows(IllegalArgumentException.class, () -> cache.readFreshness("x", Duration.ofMinutes(1), Duration.ZERO));
        assertSame(value, cache.get("x"));
    }

    @Test void physicalExpiryIsRecheckedAfterAnEarlierProviderClockSample() throws Exception {
        var clock = new GatedClock(); var cache = provider(clock); cache.put("x", entry(false), Duration.ofMinutes(40));
        var pool = Executors.newSingleThreadExecutor(r -> new Thread(r, "atomic-reader"));
        try {
            clock.armed.set(true); var future = pool.submit(() -> read(cache));
            assertTrue(clock.entered.await(5, TimeUnit.SECONDS)); clock.now.set(minute(41)); clock.release.countDown();
            assertNull(future.get(5, TimeUnit.SECONDS).entry()); assertNull(cache.get("x"));
        } finally { clock.release.countDown(); pool.shutdownNow(); }
    }

    @Test void wrappingMonotonicDeadlinesRetainTheirDuration() {
        long start = Long.MAX_VALUE - 6; var now = new AtomicLong(start); var cache = provider(now::get);
        var value = StoredEntry.ofValue("v").withLocalFreshness(new StoredEntry.LocalFreshness(start + 10, start + 15, start + 15, start + 15, null));
        cache.put("x", value, Duration.ofNanos(15)); now.set(start + 3);
        var r = cache.readFreshness("x", Duration.ofNanos(4), Duration.ofNanos(5));
        assertEquals(FRESH, r.state()); assertEquals(start + 7, r.entry().localFreshness().logicalDeadlineNanos());
        assertEquals(start + 15, r.entry().localFreshness().retentionUntilNanos());
        now.set(start + 12); assertEquals(EXPIRED, cache.readFreshness("x", Duration.ofNanos(4), Duration.ofNanos(5)).state());
        now.set(start + 15); assertNull(cache.get("x"));
    }

    @Test void aConcurrentReplacementOrRemovalCannotInheritRefreshedDeadlines() throws Exception {
        for (String mutation : new String[]{"replace", "evict", "clear"}) {
            var clock = new GatedClock(); var cache = provider(clock); var old = entry(false);
            cache.put("x", old, Duration.ofMinutes(40));
            var pool = Executors.newFixedThreadPool(2, new java.util.concurrent.ThreadFactory() {
                int count;
                public Thread newThread(Runnable r) { return new Thread(r, count++ == 0 ? "atomic-reader" : "writer"); }
            });
            var started = new CountDownLatch(1);
            try {
                clock.armed.set(true); var reader = pool.submit(() -> read(cache));
                assertTrue(clock.entered.await(5, TimeUnit.SECONDS));
                var replacement = StoredEntry.<String>nullMarker().withLocalFreshness(new StoredEntry.LocalFreshness(
                        minute(2), minute(12), minute(12), minute(12), null));
                var writer = pool.submit(() -> {
                    started.countDown();
                    if (mutation.equals("evict")) cache.evict("x");
                    else if (mutation.equals("clear")) cache.clear();
                    else cache.put("x", replacement, Duration.ofMinutes(12));
                });
                assertTrue(started.await(5, TimeUnit.SECONDS)); clock.release.countDown();
                reader.get(5, TimeUnit.SECONDS); writer.get(5, TimeUnit.SECONDS);
                if (!mutation.equals("replace")) assertNull(cache.get("x"));
                else { assertSame(replacement, cache.get("x")); clock.now.set(minute(12)); assertNull(cache.get("x")); }
            } finally { clock.release.countDown(); pool.shutdownNow(); }
        }
    }

    @Test void aDelayedSecondReaderUsesTheFirstReadersCurrentLifetime() throws Exception {
        var clock = new GatedClock(); var cache = provider(clock); cache.put("x", entry(false), Duration.ofMinutes(40));
        var pool = Executors.newFixedThreadPool(2, new java.util.concurrent.ThreadFactory() {
            int count;
            public Thread newThread(Runnable r) { return new Thread(r, count++ == 0 ? "atomic-reader" : "second-reader"); }
        });
        try {
            clock.armed.set(true); var first = pool.submit(() -> read(cache));
            assertTrue(clock.entered.await(5, TimeUnit.SECONDS));
            var second = pool.submit(() -> read(cache));
            clock.now.set(minute(1)); clock.release.countDown();
            var a = first.get(5, TimeUnit.SECONDS); var b = second.get(5, TimeUnit.SECONDS);
            assertEquals(FRESH, a.state()); assertEquals(FRESH, b.state()); assertNotSame(a.entry(), b.entry());
            assertSame(b.entry(), cache.get("x")); assertEquals(minute(3), b.entry().localFreshness().logicalDeadlineNanos());
            assertEquals(minute(40), b.entry().localFreshness().storeRetentionFloorNanos());
        } finally { clock.release.countDown(); pool.shutdownNow(); }
    }

    @Test void anAdmittedHalfOpenProbeCannotServeTheRetainedStaleValue() {
        var time = new AtomicLong(1); var breakerTime = new AtomicLong(1);
        var reads = new AtomicInteger(); var stale = new AtomicInteger();
        var settings = LocalFreshnessTest.settings(30, 2L, 10, NullPolicy.deny());
        var l1 = new CaffeineLocalCache<String, String>(settings, time::get);
        RemoteCache<String, String> unavailable = new RemoteCache<>() {
            public StoredEntry<String> get(String key) { reads.incrementAndGet(); throw new IllegalStateException("offline"); }
            public void put(String key, StoredEntry<String> entry, Duration ttl) { }
            public void evict(String key) { }
            public void clear() { }
            public boolean setIfAbsent(String key, StoredEntry<String> entry, Duration ttl) { return true; }
        };
        var breaker = new CircuitBreaker(new CircuitBreaker.Config(1, 1, 1, Duration.ofNanos(5), 1),
                new CircuitBreaker.Listener() { public void onOpen() { } public void onClose() { } }, breakerTime::get);
        var cache = new DefaultTierCache<>("probe", l1, new CircuitBreakerRemoteCache<>(unavailable, breaker), settings,
                true, null, null, null, null, breaker, new CacheMetricsListener() {
                    public void onRequest(String name, Outcome outcome) { if (outcome == Outcome.STALE_DEGRADED) stale.incrementAndGet(); }
                }, null, new TtlJitter(), () -> true, time::get);
        cache.put("x", "retained"); breaker.onFailure();
        time.set(minute(1)); assertEquals("retained", cache.get("x"));
        time.set(minute(3)); assertEquals("retained", cache.get("x")); assertEquals(1, stale.get()); assertEquals(0, reads.get());
        breakerTime.set(6); assertEquals(BreakerState.HALF_OPEN, breaker.state());
        assertNull(cache.get("x")); assertEquals(1, reads.get()); assertEquals(1, stale.get());
        assertEquals("retained", cache.get("x")); assertEquals(2, stale.get());
        assertEquals(minute(13), l1.get("x").localFreshness().staleServeUntilNanos());
    }

    @Test void observationsStayImmutableAndExplicitDurationPairsAreHonored() {
        var now = new AtomicLong(1); var cache = provider(now::get); cache.put("x", entry(false), Duration.ofMinutes(40));
        now.set(minute(1)); var fresh = read(cache);
        now.set(minute(3)); var stale = read(cache);
        assertSame(fresh.entry(), stale.entry()); assertEquals(FRESH, fresh.state()); assertEquals(STALE_ALLOWED, stale.state());
        now.set(minute(4)); assertSame(stale, read(cache));
        cache.put("x", entry(false), Duration.ofMinutes(36));
        var customWindow = cache.readFreshness("x", Duration.ofMinutes(2), Duration.ofMinutes(3));
        assertEquals(minute(6), customWindow.entry().localFreshness().logicalDeadlineNanos());
        assertEquals(minute(9), customWindow.entry().localFreshness().staleServeUntilNanos());
        assertEquals(minute(40), customWindow.entry().localFreshness().retentionUntilNanos());
        assertEquals(FRESH, fresh.state()); assertSame(fresh.entry(), stale.entry());
    }

    static final class GatedClock implements LongSupplier {
        final AtomicLong now = new AtomicLong(1);
        final AtomicBoolean armed = new AtomicBoolean();
        final AtomicInteger samples = new AtomicInteger();
        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        public long getAsLong() {
            if (Thread.currentThread().getName().equals("atomic-reader") && armed.get() && samples.incrementAndGet() == 2) {
                entered.countDown();
                try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("clock gate timeout"); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
            }
            return now.get();
        }
    }
}
