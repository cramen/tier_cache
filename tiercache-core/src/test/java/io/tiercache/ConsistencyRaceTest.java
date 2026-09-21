package io.tiercache;

import io.tiercache.internal.CircuitBreaker;
import io.tiercache.internal.CircuitBreakerRemoteCache;
import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.LocalCache;
import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrency fixes from the b2e3fbd implementation review: coherent
 * value/metadata snapshots, generation/expiry ordering inside the commit,
 * early generation capture on every warm path, honest putIfAbsent
 * outcomes, and scheduler publication racing close.
 */
class ConsistencyRaceTest {

    private static final Duration L1_TTL = Duration.ofMillis(200);
    private static final Duration WINDOW = Duration.ofMillis(500);

    /** L1 whose get/put can be parked on latches (deterministic race seams). */
    private static final class GatedL1 implements LocalCache<String, String> {
        final CountingLocalCache<String, String> delegate = new CountingLocalCache<>();
        final CountDownLatch getReturned = new CountDownLatch(1);
        final CountDownLatch releaseAfterGet = new CountDownLatch(1);
        final AtomicBoolean gateGet = new AtomicBoolean();

        @Override
        public StoredEntry<String> get(String key) {
            StoredEntry<String> entry = delegate.get(key);
            if (gateGet.getAndSet(false)) {
                getReturned.countDown();
                awaitQuietly(releaseAfterGet);
            }
            return entry;
        }

        @Override
        public void put(String key, StoredEntry<String> entry, Duration ttl) {
            delegate.put(key, entry, ttl);
        }

        @Override
        public void evict(String key) {
            delegate.evict(key);
        }

        @Override
        public void clear() {
            delegate.clear();
        }

        @Override
        public boolean setIfAbsent(String key, StoredEntry<String> entry, Duration ttl) {
            return delegate.setIfAbsent(key, entry, ttl);
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static DefaultTierCache<String, String> engine(LocalCache<String, String> l1,
            RemoteCache<String, String> l2, CacheSettings settings) {
        return new DefaultTierCache<>("c", l1, l2, settings, true, null, null,
                new VersionGenerator(), null);
    }

    private static CacheSettings windowed(Duration accessTtl) {
        return new CacheSettings(10_000, L1_TTL, accessTtl, Duration.ofHours(1), 0.0,
                NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024,
                Duration.ZERO, false, Duration.ofSeconds(1), WINDOW);
    }

    /**
     * P1: a reader holding a stale L1 snapshot must never re-put it over a
     * completed concurrent put. The slide happens only for the CURRENT
     * value/metadata snapshot.
     */
    @Test
    void freshAccessNeverRestoresStaleValueOverCompletedPut() throws Exception {
        GatedL1 gated = new GatedL1();
        VersionedL2 l2 = new VersionedL2();
        DefaultTierCache<String, String> a = engine(gated, l2, windowed(Duration.ofMillis(100)));
        a.put("k", "v1");
        gated.gateGet.set(true);
        AtomicReference<String> readerResult = new AtomicReference<>();
        Thread reader = new Thread(() -> readerResult.set(a.get("k")));
        reader.start();
        if (!gated.getReturned.await(5, TimeUnit.SECONDS)) {
            throw new AssertionError("the reader never reached the L1 get");
        }
        a.put("k", "v2"); // completes fully on the SAME instance while parked
        gated.releaseAfterGet.countDown();
        reader.join(5_000);

        assertEquals("v2", readerResult.get(),
                "the concurrent reader itself must see the coherent snapshot, never v1");
        assertEquals("v2", a.get("k"), "the completed put must win; no stale re-put");
    }

    /**
     * P1 (follow-up): an entry evicted between the caller's L1 read and
     * the freshness snapshot must never produce a null FRESH snapshot
     * (NPE) — the read continues to the normal L2 path instead.
     */
    @Test
    void evictedBetweenReadAndSnapshotNeverNpe() throws Exception {
        GatedL1 gated = new GatedL1();
        VersionedL2 l2 = new VersionedL2();
        DefaultTierCache<String, String> a = engine(gated, l2, windowed(null));
        a.put("k", "v1");
        gated.gateGet.set(true);
        AtomicReference<String> result = new AtomicReference<>();
        Thread reader = new Thread(() -> result.set(a.get("k")));
        reader.start();
        if (!gated.getReturned.await(5, TimeUnit.SECONDS)) {
            throw new AssertionError("the reader never reached the L1 get");
        }
        a.evict("k"); // the value vanishes while the reader is parked
        gated.releaseAfterGet.countDown();
        reader.join(5_000);

        assertNull(l2.delegate.get("k"), "the engine's evict removes L2 as well");
        assertNull(result.get(), "evicted everywhere: an honest miss, never an NPE");

        // And with the value still in L2 (size-eviction shape), the read
        // converges from L2 instead of failing.
        a.put("x", "vx");
        gated.gateGet.set(true);
        AtomicReference<String> resultX = new AtomicReference<>();
        Thread readerX = new Thread(() -> resultX.set(a.get("x")));
        readerX.start();
        if (!gated.getReturned.await(5, TimeUnit.SECONDS)) {
            throw new AssertionError("the second reader never reached the L1 get");
        }
        a.evictAllL1(); // L1 vanishes, L2 keeps the value
        gated.releaseAfterGet.countDown();
        readerX.join(5_000);
        assertEquals("vx", resultX.get(),
                "with L2 intact the read converges from L2 instead of failing");
    }

    /**
     * P1: a barrier expiring inside the commit's own metadata lookup bumps
     * the generation BEFORE the commit's final check — the stale write is
     * refused, never committed.
     */
    @Test
    void expiredBarrierDuringCommitIsRefused() throws Exception {
        Duration previous = io.tiercache.internal.DefaultTierCache.L1_META_EXPIRY;
        io.tiercache.internal.DefaultTierCache.L1_META_EXPIRY = Duration.ofMillis(50);
        try {
            GatedL1 gated = new GatedL1();
            VersionedL2 l2 = new VersionedL2();
            DefaultTierCache<String, String> a = engine(gated, l2, windowed(null));
            a.put("k", "v1");
            UUID id = UUID.randomUUID();
            a.evictL1IfNewer("k", new Version(9_999_999_999_999_999L, id)); // barrier

            // The commit's own metadata lookup must be the first to discover
            // the expiry — and the generation bump it causes must refuse the
            // stale warm. Simulate via the load path with a slow L2 read.
            CountDownLatch readEntered = new CountDownLatch(1);
            CountDownLatch releaseRead = new CountDownLatch(1);
            RemoteCache<String, String> gatedL2 = new RemoteCache<>() {
                final RemoteCache<String, String> inner = l2;

                @Override
                public StoredEntry<String> get(String key) {
                    readEntered.countDown();
                    sleep(120); // past the 50 ms barrier expiry, untouched
                    awaitQuietly(releaseRead);
                    return inner.get(key);
                }

                @Override
                public void put(String key, StoredEntry<String> entry, Duration ttl) {
                    inner.put(key, entry, ttl);
                }

                @Override
                public void evict(String key) {
                    inner.evict(key);
                }

                @Override
                public void clear() {
                    inner.clear();
                }

                @Override
                public boolean setIfAbsent(String key, StoredEntry<String> entry, Duration ttl) {
                    return inner.setIfAbsent(key, entry, ttl);
                }
            };
            DefaultTierCache<String, String> slow = engine(new CountingLocalCache<>(), gatedL2,
                    windowed(null));
            // Absent-key barrier; L2 holds the value; L1 has nothing.
            slow.evictL1IfNewer("k", new Version(9_999_999_999_999_999L, id));
            l2.put("k", StoredEntry.ofValue("v1", new Version(60, id)), Duration.ofMinutes(1));
            Thread reader = new Thread(() -> slow.get("k"));
            reader.start();
            if (!readEntered.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("the L2 read never started");
            }
            releaseRead.countDown();
            reader.join(5_000);

            assertNull(slow.versionOfL1Entry("k"),
                    "the expiring barrier bumps the generation before the commit's check");
        } finally {
            io.tiercache.internal.DefaultTierCache.L1_META_EXPIRY = previous;
        }
    }

    /**
     * P1: the tagged put captures the generation BEFORE the L2 I/O — a
     * generation bump between the transport write and the warm refuses it.
     */
    @Test
    void taggedPutRefusesWarmAfterGenerationBump() throws Exception {
        InMemoryRemoteCache<String, String> delegate = new InMemoryRemoteCache<>();
        CountDownLatch taggedWritten = new CountDownLatch(1);
        CountDownLatch releaseTagged = new CountDownLatch(1);
        RemoteCache<String, String> gated = new RemoteCache<>() {
            @Override
            public StoredEntry<String> get(String key) {
                return delegate.get(key);
            }

            @Override
            public void put(String key, StoredEntry<String> entry, Duration ttl) {
                delegate.put(key, entry, ttl);
            }

            @Override
            public void putTagged(String key, StoredEntry<String> entry, Duration ttl,
                    String[] tags) {
                taggedWritten.countDown();
                awaitQuietly(releaseTagged);
                delegate.putTagged(key, entry, ttl, tags);
            }

            @Override
            public void evict(String key) {
                delegate.evict(key);
            }

            @Override
            public void clear() {
                delegate.clear();
            }

            @Override
            public boolean setIfAbsent(String key, StoredEntry<String> entry, Duration ttl) {
                return delegate.setIfAbsent(key, entry, ttl);
            }
        };
        GatedL1 l1 = new GatedL1();
        DefaultTierCache<String, String> a = engine(l1, gated, windowed(null));

        Thread writer = new Thread(() -> a.put("k", "old", "tag"));
        writer.start();
        if (!taggedWritten.await(5, TimeUnit.SECONDS)) {
            throw new AssertionError("the tagged write never reached the L2 call");
        }
        a.evictAllL1(); // generation bump between the L2 write and the warm
        releaseTagged.countDown();
        writer.join(5_000);

        assertNull(a.versionOfL1Entry("k"),
                "the generation bump must refuse the late tagged warm");
    }

    /**
     * P2: past the SWR horizon the retained-hit classification is a hard
     * miss that goes to the LOADER — never a false null.
     */
    @Test
    void swrBoundaryHardMissGoesToLoader() {
        InMemoryRemoteCache<String, String> delegate = new InMemoryRemoteCache<>();
        CacheSettings settings = new CacheSettings(10_000, Duration.ofMillis(50), null,
                Duration.ofMillis(100), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE,
                64 * 1024, Duration.ofSeconds(1), false, Duration.ofSeconds(1), WINDOW);
        DefaultTierCache<String, String> a = engine(new CountingLocalCache<>(), delegate, settings);
        // A frame aged past l2Ttl + staleTtl (100 ms + 1 s) sits in L2; the
        // engine's L1 holds a copy (planted directly, not re-stamped).
        delegate.put("k", StoredEntry.ofValue("old", null,
                System.currentTimeMillis() - 2_000), Duration.ofMillis(100),
                Duration.ofSeconds(1));
        a.applyUpdateL1("k", "old", new Version(50, UUID.randomUUID()));
        sleep(150); // L1 logical TTL (50 ms) expired; the frame is past its SWR horizon

        assertEquals("new", a.getOrCompute("k", key -> "new"),
                "past the SWR horizon getOrCompute must load, not return a false null");
    }

    /**
     * P2: putIfAbsent reports the REAL outcome — a refused commit (a
     * generation change racing the degraded insert) is a loss, never a
     * fabricated win.
     */
    @Test
    void degradedPutIfAbsentReportsRealOutcome() throws Exception {
        Duration previous = DefaultTierCache.L1_META_EXPIRY;
        DefaultTierCache.L1_META_EXPIRY = Duration.ofMillis(50);
        try {
            InMemoryRemoteCache<String, String> delegate = new InMemoryRemoteCache<>();
            CircuitBreaker breaker = new CircuitBreaker(
                    new CircuitBreaker.Config(1, 1.0, 1, Duration.ofMillis(200), 1),
                    new CircuitBreaker.Listener() {
                        @Override
                        public void onOpen() {
                        }

                        @Override
                        public void onClose() {
                        }
                    });
            breaker.onFailure(); // degraded from the start
            DefaultTierCache<String, String> a = engine(new CountingLocalCache<>(),
                    new CircuitBreakerRemoteCache<>(delegate, breaker), windowed(null));
            UUID id = UUID.randomUUID();
            a.evictL1IfNewer("k", new Version(100, id)); // barrier for the key
            sleep(100); // it expired, untouched

            // The commit's own metadata lookup discovers the expiry, bumps
            // the generation, and the insert is refused — honestly false.
            assertFalse(a.putIfAbsent("k", "saved"),
                    "a refused commit must not report success");
            assertNull(a.versionOfL1Entry("k"), "nothing was inserted");
        } finally {
            DefaultTierCache.L1_META_EXPIRY = previous;
        }
    }

    /** Minimal versioned L2 with tombstones (as in LoadStoreRaceTest). */
    private static final class VersionedL2 implements RemoteCache<String, String> {
        final InMemoryRemoteCache<String, String> delegate = new InMemoryRemoteCache<>();
        final java.util.Map<String, Version> tombstones = new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public StoredEntry<String> get(String key) {
            return delegate.get(key);
        }

        @Override
        public void put(String key, StoredEntry<String> entry, Duration ttl) {
            tombstones.remove(key);
            delegate.put(key, entry, ttl);
        }

        @Override
        public boolean putIfNewer(String key, StoredEntry<String> entry, Duration ttl) {
            StoredEntry<String> current = delegate.get(key);
            Version bar = current != null ? current.version() : tombstones.get(key);
            if (bar != null && entry.version() != null && entry.version().compareTo(bar) <= 0) {
                return false;
            }
            tombstones.remove(key);
            delegate.put(key, entry, ttl);
            return true;
        }

        @Override
        public void evict(String key) {
            delegate.evict(key);
        }

        @Override
        public void evict(String key, Version version) {
            delegate.evict(key);
            if (version != null) {
                tombstones.put(key, version);
            }
        }

        @Override
        public void clear() {
            delegate.clear();
            tombstones.clear();
        }

        @Override
        public boolean setIfAbsent(String key, StoredEntry<String> entry, Duration ttl) {
            return delegate.setIfAbsent(key, entry, ttl);
        }
    }
}
