package io.tiercache;

import io.tiercache.internal.DefaultTierCache;
import io.tiercache.spi.LocalCache;
import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;
import io.tiercache.testkit.CountingLocalCache;
import io.tiercache.testkit.InMemoryRemoteCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * In-flight load vs concurrent write races (load-test review): the store
 * version is minted at claim time, the losing load converges or reloads
 * once (never a false miss), L1 commits are atomic and barriered, and
 * generation churn never fabricates a miss.
 */
class LoadStoreRaceTest {

    /** In-memory L2 with version compare and tombstones for versioned evicts. */
    private static final class VersionedL2 implements RemoteCache<String, String> {
        final InMemoryRemoteCache<String, String> delegate = new InMemoryRemoteCache<>();
        final Map<String, Version> tombstones = new ConcurrentHashMap<>();
        final AtomicInteger reads = new AtomicInteger();

        @Override
        public StoredEntry<String> get(String key) {
            reads.incrementAndGet();
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

        @Override
        public List<String> keysByTag(String tag) {
            return delegate.keysByTag(tag);
        }

        @Override
        public void putTagged(String key, StoredEntry<String> entry, Duration ttl, String[] tags) {
            delegate.putTagged(key, entry, ttl, tags);
        }
    }

    /** L1 wrapper parking the first versioned put on a latch (race seam). */
    private static final class GatedL1 implements LocalCache<String, String> {
        final CountingLocalCache<String, String> delegate = new CountingLocalCache<>();
        final CountDownLatch putEntered = new CountDownLatch(1);
        final CountDownLatch releasePut = new CountDownLatch(1);
        volatile boolean armed = true;

        @Override
        public StoredEntry<String> get(String key) {
            return delegate.get(key);
        }

        @Override
        public void put(String key, StoredEntry<String> entry, Duration ttl) {
            if (armed && entry.version() != null) {
                armed = false;
                putEntered.countDown();
                awaitQuietly(releasePut);
            }
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

    private static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static DefaultTierCache<String, String> newCache(LocalCache<String, String> l1,
            RemoteCache<String, String> l2, CacheSettings settings) {
        return new DefaultTierCache<>("c", l1, l2, settings, true, null, null,
                new VersionGenerator(), null);
    }

    private static DefaultTierCache<String, String> newCache(RemoteCache<String, String> l2,
            CacheSettings settings) {
        return newCache(new CountingLocalCache<>(), l2, settings);
    }

    /**
     * Claim-time version: a slow load's stale snapshot loses to a concurrent
     * update, and the losing caller receives the updated value.
     */
    @Test
    void slowLoadCannotOverwriteConcurrentUpdate() {
        VersionedL2 l2 = new VersionedL2();
        DefaultTierCache<String, String> a = newCache(l2, CacheSettings.defaults());
        DefaultTierCache<String, String> b = newCache(l2, CacheSettings.defaults());

        String result = a.getOrCompute("k", key -> {
            sleepMillis(2); // B's write is strictly later than A's claim version
            b.put(key, "v1"); // the concurrent update lands mid-load
            return "v0"; // the stale snapshot
        });

        assertEquals("v1", result, "the losing load's caller receives the updated value");
        assertEquals("v1", a.get("k"), "A converges to the updated value");
        assertEquals("v1", l2.delegate.get("k").value(), "L2 keeps the updated value");
    }

    /**
     * A load losing to an eviction performs exactly one bounded reload and
     * the caller sees the fresh source value — never a false "not found".
     */
    @Test
    void evictedMidLoadIsReloadedOnceAndCallerSeesFreshSource() {
        VersionedL2 l2 = new VersionedL2();
        DefaultTierCache<String, String> a = newCache(l2, CacheSettings.defaults());
        DefaultTierCache<String, String> b = newCache(l2, CacheSettings.defaults());
        AtomicInteger loads = new AtomicInteger();

        String result = a.getOrCompute("k", key -> {
            if (loads.incrementAndGet() == 1) {
                sleepMillis(2);
                b.evict(key); // the versioned tombstone lands mid-load
                return "v0-stale";
            }
            return "v1-fresh";
        });

        assertEquals("v1-fresh", result, "the bounded reload returns the fresh source value");
        assertEquals(2, loads.get(), "exactly one bounded reload");
        assertEquals("v1-fresh", a.get("k"), "the reload result is the stored one");
    }

    /**
     * A remote invalidation processed while the L1 commit is parked: the
     * stale warm must not survive. Pre-fix the invalidation hit an empty L1
     * and was a no-op, after which the stale warm landed.
     */
    @Test
    void invalidationBetweenL2StoreAndL1CommitIsCaught() throws Exception {
        VersionedL2 l2 = new VersionedL2();
        GatedL1 gated = new GatedL1();
        DefaultTierCache<String, String> a = newCache(gated, l2, CacheSettings.defaults());
        AtomicReference<String> result = new AtomicReference<>();

        Thread loader = new Thread(() -> result.set(a.getOrCompute("k", key -> "v1")));
        loader.start();
        assertTrue(gated.putEntered.await(5, TimeUnit.SECONDS),
                "the load must reach the parked L1 commit");

        // The invalidation arrives while the warm is parked (it queues on
        // the same stripe in the new code; it must not be a no-op).
        Thread invalidator = new Thread(
                () -> a.evictL1IfNewer("k", new Version(9_999_999_999_999_999L, UUID.randomUUID())));
        invalidator.start();
        Thread.sleep(100); // let it reach the stripe
        gated.releasePut.countDown();
        loader.join(5_000);
        invalidator.join(5_000);

        assertEquals("v1", result.get(), "the caller still receives its freshly loaded value");
        assertNull(gated.get("k"), "the stale warm must not survive in L1");
        assertNull(a.versionOfL1Entry("k"), "no L1 version remains for the key");
    }

    /**
     * The local-write variant of the same race: a local put(v2) landing
     * while the stale warm(v1) is parked must win L1.
     */
    @Test
    void localPutBetweenL2StoreAndL1CommitWins() throws Exception {
        VersionedL2 l2 = new VersionedL2();
        GatedL1 gated = new GatedL1();
        DefaultTierCache<String, String> a = newCache(gated, l2, CacheSettings.defaults());

        Thread loader = new Thread(() -> a.getOrCompute("k", key -> "v1"));
        loader.start();
        assertTrue(gated.putEntered.await(5, TimeUnit.SECONDS),
                "the load must reach the parked L1 commit");

        Thread writer = new Thread(() -> a.put("k", "v2-local"));
        writer.start();
        Thread.sleep(100); // let it reach the stripe
        gated.releasePut.countDown();
        loader.join(5_000);
        writer.join(5_000);

        assertEquals("v2-local", gated.get("k").value(), "the local write must win L1");
        assertEquals("v2-local", a.get("k"));
    }

    /**
     * Equal-version semantics: an UPDATE installs on absent/older L1, a
     * duplicate is idempotent, a strictly older UPDATE is rejected, and an
     * L2 warm at exactly the barrier version is admitted (a tombstone at
     * that version would read as absent instead).
     */
    @Test
    void equalVersionUpdateInstallsAndBarrierAdmitsEqualWarm() {
        VersionedL2 l2 = new VersionedL2();
        DefaultTierCache<String, String> a = newCache(l2, CacheSettings.defaults());
        UUID id = UUID.randomUUID();

        a.applyUpdateL1("k", "v2", new Version(100, id));
        assertEquals("v2", a.get("k"), "UPDATE installs on an absent L1 entry");
        a.applyUpdateL1("k", "v2-dup", new Version(100, id));
        assertEquals("v2", a.get("k"), "a duplicate UPDATE is an idempotent no-op");
        a.applyUpdateL1("k", "v1", new Version(99, id));
        assertEquals("v2", a.get("k"), "a strictly older UPDATE is rejected");

        a.evictL1IfNewer("x", new Version(200, id)); // barrier at 200, key absent
        l2.put("x", StoredEntry.ofValue("x2", new Version(200, id)), Duration.ofMinutes(1));
        assertEquals("x2", a.get("x"), "an L2 warm at exactly the barrier version is admitted");
    }

    /**
     * Generation churn mid-load: only the L1 fill is refused — the caller
     * still receives the freshly loaded value (never an artificial miss),
     * and the loader budget is respected.
     */
    @Test
    void generationChurnRefusesOnlyTheL1Fill() {
        VersionedL2 l2 = new VersionedL2();
        DefaultTierCache<String, String> a = newCache(l2, CacheSettings.defaults());
        AtomicInteger loads = new AtomicInteger();

        String result = a.getOrCompute("k", key -> {
            loads.incrementAndGet();
            a.evictAllL1(); // generation bump between claim and commit
            a.evictAllL1(); // and a second one
            return "v";
        });

        assertEquals("v", result, "a found value is never converted into a miss");
        assertEquals(1, loads.get(), "no reloads are triggered by generation churn alone");
        assertEquals("v", l2.delegate.get("k").value(), "L2 still holds the stored value");
    }

    /**
     * Combined losing-load + generation-churn conflict: at most two loader
     * executions for the original request and no fabricated miss.
     */
    @Test
    void combinedEvictAndGenerationChurnRespectsTheLoaderBudget() {
        VersionedL2 l2 = new VersionedL2();
        DefaultTierCache<String, String> a = newCache(l2, CacheSettings.defaults());
        DefaultTierCache<String, String> b = newCache(l2, CacheSettings.defaults());
        AtomicInteger loads = new AtomicInteger();

        String result = a.getOrCompute("k", key -> {
            int attempt = loads.incrementAndGet();
            sleepMillis(2);
            if (attempt == 1) {
                b.evict(key0(key)); // tombstone mid-first-load
            }
            a.evictAllL1(); // generation churn on every attempt
            return "v" + attempt;
        });

        assertTrue(result != null && result.startsWith("v"), "no fabricated miss, got " + result);
        assertTrue(loads.get() <= 2, "at most two loader executions, got " + loads.get());
    }

    private static String key0(String key) {
        return key;
    }

    /**
     * Null policies stay honest: under `allow`, the marker store follows
     * the same claim-time rules (loses to a tombstone, reload re-marks);
     * under `deny`, a null result stores nothing.
     */
    @Test
    void nullPolicyPathsStayHonest() {
        CacheSettings allow = new CacheSettings(10_000, Duration.ofMinutes(5), null,
                Duration.ofHours(1), 0.0, NullPolicy.allow(Duration.ofMinutes(1)),
                InvalidationMode.INVALIDATE, 64 * 1024);
        VersionedL2 l2 = new VersionedL2();
        DefaultTierCache<String, String> a = newCache(l2, allow);
        DefaultTierCache<String, String> b = newCache(l2, allow);
        AtomicInteger loads = new AtomicInteger();

        String result = a.getOrCompute("k", key -> {
            if (loads.incrementAndGet() == 1) {
                sleepMillis(2);
                b.evict(key); // tombstone mid-load: the first marker must lose
            }
            return null;
        });

        assertNull(result, "the source is absent: null is the honest answer");
        assertEquals(2, loads.get(), "the marker lost to the tombstone and re-marked once");
        a.getOrCompute("k", key -> {
            throw new AssertionError("the null-marker must suppress further loads");
        });
        assertEquals(2, loads.get());

        VersionedL2 l2deny = new VersionedL2();
        DefaultTierCache<String, String> deny = newCache(l2deny, CacheSettings.defaults());
        assertNull(deny.getOrCompute("x", key -> null), "deny: a miss stays a miss");
        assertNull(l2deny.delegate.get("x"), "deny: nothing is stored from a null result");
    }
}
