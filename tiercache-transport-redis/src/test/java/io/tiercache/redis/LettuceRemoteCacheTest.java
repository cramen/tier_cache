package io.tiercache.redis;

import io.lettuce.core.RedisException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: redis-l2-transport — cross-instance atomicity, fail-fast timeouts
 * kept well below business timeouts, pluggable serialization.
 */
class LettuceRemoteCacheTest {

    private static GenericContainer<?> server;
    private static String redisUri;

    @BeforeAll
    static void startServer() {
        server = new GenericContainer<>(ServerProfile.image())
                .withExposedPorts(6379);
        server.start();
        redisUri = "redis://" + server.getHost() + ":" + server.getMappedPort(6379);
    }

    @AfterAll
    static void stopServer() {
        server.stop();
    }

    @Test
    void singleWinnerAcrossInstances() throws Exception {
        // Spec scenario: two transport instances, one server, one winner.
        try (LettuceRemoteCache<String, String> a = sharedNamespaceInstance();
                LettuceRemoteCache<String, String> b = sharedNamespaceInstance()) {
            // Same namespace: both instances address the same logical key.
            int threads = 8;
            var pool = Executors.newFixedThreadPool(threads);
            var start = new CountDownLatch(1);
            AtomicInteger wins = new AtomicInteger();
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                var instance = (i % 2 == 0) ? a : b;
                String candidate = "v" + i;
                futures.add(pool.submit(() -> {
                    start.await();
                    if (instance.setIfAbsent("k", io.tiercache.spi.StoredEntry.ofValue(candidate), Duration.ofMinutes(1))) {
                        wins.incrementAndGet();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (var f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            pool.shutdown();
            assertEquals(1, wins.get(), "exactly one winner across instances");
        }
    }

    @Test
    void unreachableServerFailsFast() {
        // Spec scenario: unreachable server -> unchecked exception within the
        // configured timeout, no indefinite blocking.
        try (LettuceRemoteCache<String, String> cache =
                LettuceRemoteCache.<String, String>builder("redis://127.0.0.1:1")
                        .connectTimeout(Duration.ofMillis(100))
                        .commandTimeout(Duration.ofMillis(250))
                        .build()) {
            // ignore: connection refused surfaces on the first command
        } catch (RedisException expected) {
            return; // may already fail at connect()
        }
        // If connect somehow succeeded, the command must fail fast instead.
        try (LettuceRemoteCache<String, String> cache =
                LettuceRemoteCache.<String, String>builder("redis://127.0.0.1:1")
                        .connectTimeout(Duration.ofMillis(100))
                        .commandTimeout(Duration.ofMillis(250))
                        .build()) {
            long startNanos = System.nanoTime();
            assertThrows(RedisException.class, () -> cache.get("k"));
            long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
            assertTrue(elapsedMillis < 5_000,
                    "operation must fail fast, took " + elapsedMillis + " ms");
        } catch (RedisException expected) {
            // acceptable: failure at connect time is also fail-fast
        }
    }

    @Test
    void customSerializerRoundTrip() {
        CacheSerializer<String> upperCase = new CacheSerializer<>() {
            @Override
            public byte[] toBytes(String value) {
                return value.toUpperCase().getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public String fromBytes(byte[] bytes) {
                return new String(bytes, StandardCharsets.UTF_8).toLowerCase();
            }
        };
        try (LettuceRemoteCache<String, String> cache =
                LettuceRemoteCache.<String, String>builder(redisUri)
                        .cacheName("custom-ser")
                        .keySerializer(upperCase)
                        .valueSerializer(upperCase)
                        .build()) {
            cache.put("Key", io.tiercache.spi.StoredEntry.ofValue("MixedCase"), Duration.ofMinutes(1));
            assertEquals("mixedcase", cache.get("Key").value());
        }
    }

    @Test
    void nullMarkerRoundTripThroughRedis() {
        // Spec: marker encoding — stored in Redis, reads back as cached-null.
        try (LettuceRemoteCache<String, String> cache =
                LettuceRemoteCache.<String, String>builder(redisUri)
                        .cacheName("marker-rt")
                        .build()) {
            cache.put("k", io.tiercache.spi.StoredEntry.nullMarker(), Duration.ofMinutes(1));
            io.tiercache.spi.StoredEntry<String> entry = cache.get("k");
            assertTrue(entry != null && entry.isNullMarker());

            // Real values still decode normally next to the marker.
            cache.put("v", io.tiercache.spi.StoredEntry.ofValue("data"), Duration.ofMinutes(1));
            assertEquals("data", cache.get("v").value());
        }
    }

    @Test
    void putIfAbsentThroughCoreOverRedis() {
        // Spec core-read-path: putIfAbsent winner/loser over the real transport.
        try (LettuceRemoteCache<String, String> l2 =
                LettuceRemoteCache.<String, String>builder(redisUri)
                        .cacheName("pia-core")
                        .build()) {
            io.tiercache.TierCache<String, String> cache = io.tiercache.TierCacheFactory.builder()
                    .remoteCache(l2)
                    .build()
                    .getCache("pia-core");
            assertTrue(cache.putIfAbsent("k", "first"));
            assertTrue(cache.putIfAbsent("k", "second") == false);
            assertEquals("first", cache.get("k"));
        }
    }

    @Test
    void staleWindowWriteSurvivesPastLogicalTtl() throws Exception {
        // Design D1: physical expiry is ttl + staleTtl, so the entry outlives
        // its logical TTL inside the stale window and stays parseable.
        try (LettuceRemoteCache<String, String> cache =
                LettuceRemoteCache.<String, String>builder(redisUri)
                        .cacheName("stale-window")
                        .build()) {
            long before = System.currentTimeMillis();
            cache.put("k", io.tiercache.spi.StoredEntry.ofValue("v"),
                    Duration.ofSeconds(1), Duration.ofSeconds(10));
            // Poll until the logical TTL has elapsed; the physical lifetime
            // (ttl + staleTtl = 11s) leaves a wide assertion window.
            waitFor(() -> System.currentTimeMillis() - before > 1_000);
            io.tiercache.spi.StoredEntry<String> entry = cache.get("k");
            assertTrue(entry != null && !entry.isNullMarker(),
                    "entry must survive past its logical TTL within the stale window");
            assertEquals("v", entry.value());
            assertTrue(entry.hasWriteTimestamp());
            long writeTs = entry.writeTimestampMillis();
            assertTrue(writeTs >= before && writeTs <= System.currentTimeMillis());
        }
    }

    @Test
    void staleWindowWriteExpiresAfterPhysicalTtl() throws Exception {
        try (LettuceRemoteCache<String, String> cache =
                LettuceRemoteCache.<String, String>builder(redisUri)
                        .cacheName("stale-expire")
                        .build()) {
            cache.put("k", io.tiercache.spi.StoredEntry.ofValue("v"),
                    Duration.ofMillis(500), Duration.ofSeconds(1));
            // Poll for actual server-side expiry instead of a fixed sleep.
            waitFor(() -> cache.get("k") == null);
            assertTrue(cache.get("k") == null,
                    "entry must be gone once ttl + staleTtl has elapsed");
        }
    }

    @Test
    void staleWindowWriteUsesExtendedFrame() {
        try (LettuceRemoteCache<String, String> cache =
                LettuceRemoteCache.<String, String>builder(redisUri)
                        .cacheName("stale-frame")
                        .build()) {
            cache.put("k", io.tiercache.spi.StoredEntry.ofValue("v"),
                    Duration.ofMinutes(1), Duration.ofMinutes(1));
            assertEquals(ValueFrame.TAG_VALUE_V3, rawValue(cache, "stale-frame", "k")[0],
                    "stale-window writes use the extended frame");

            // Without a stale window the legacy frame is written unchanged.
            cache.put("plain", io.tiercache.spi.StoredEntry.ofValue("v"), Duration.ofMinutes(1));
            assertEquals(ValueFrame.TAG_VALUE, rawValue(cache, "stale-frame", "plain")[0]);
            assertTrue(cache.get("plain").hasWriteTimestamp() == false);
        }
    }

    private static void waitFor(Check check) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!check.ok()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 10s");
            }
            TimeUnit.MILLISECONDS.sleep(25);
        }
    }

    private interface Check {
        boolean ok();
    }

    private static byte[] rawValue(LettuceRemoteCache<String, String> cache, String cacheName, String key) {
        return cache.connection().sync().get(RedisKeyspace.dataKey(cacheName,
                new JdkCacheSerializer<String>().toBytes(key)));
    }

    private static LettuceRemoteCache<String, String> sharedNamespaceInstance() {
        return LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("xinst") // shared namespace on purpose
                .build();
    }

    /** Tag indexes are TTL-bounded and dead members are never returned (reviewer: PTTL=-1 growth). */
    @Test
    void tagIndexesExpireWithTheData() throws Exception {
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("tag-ttl").build();
        io.tiercache.Version v = new io.tiercache.Version(1, java.util.UUID.randomUUID());
        cache.putTagged("k", io.tiercache.spi.StoredEntry.ofValue("v", v),
                Duration.ofMillis(400), new String[]{"g"});
        try (io.lettuce.core.RedisClient probe = io.lettuce.core.RedisClient.create(redisUri);
                var conn = probe.connect()) {
            Long pttl = conn.sync().pttl(new String(RedisKeyspace.tagKey("tag-ttl", "g"), StandardCharsets.US_ASCII));
            assertTrue(pttl != null && pttl > 0 && pttl <= 400,
                    "tag set must be TTL-bounded from write time, got PTTL=" + pttl);
        }

        // A dead member with a long-lived index row is filtered and pruned on lookup.
        try (io.lettuce.core.RedisClient probe = io.lettuce.core.RedisClient.create(redisUri);
                var conn = probe.connect()) {
conn.sync().sadd(new String(RedisKeyspace.tagKey("tag-ttl", "g"), StandardCharsets.US_ASCII), "tag-ttl:ghost");
        }
        assertTrue(cache.keysByTag("g").stream().noneMatch("ghost"::equals),
                "phantom members must not be returned");

        // After the data TTL, the index structures are gone with the data.
        Thread.sleep(600);
        assertTrue(cache.keysByTag("g").isEmpty());
        try (io.lettuce.core.RedisClient probe = io.lettuce.core.RedisClient.create(redisUri);
                var conn = probe.connect()) {
            assertTrue(conn.sync().keys(new String(RedisKeyspace.tagPrefix("tag-ttl"), StandardCharsets.US_ASCII) + "*").isEmpty(),
                    "tag set must expire with the data");
        }
        cache.close();
    }

    /**
     * Boundedness under UNINTERRUPTED writes (no tag reads, so the set TTL
     * cannot mask a missing cleanup by expiring the set): the writer keeps
     * writing while the measurement runs, and the size plus the live count
     * come from ONE atomic Lua probe (a consistent snapshot). Bound:
     * size &le; live + 2*live/(K-1) + 4K, K = 8. The janitor is the only
     * mechanism that can achieve this; pre-fix the set accumulates every
     * key ever written (verified on that variant).
     */
    @Test
    void tagSetSizeStaysBoundedUnderContinuousWrites() throws Exception {
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("tag-hot").build();
        Duration ttl = Duration.ofMillis(500);
        java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
        AtomicInteger writes = new AtomicInteger();
        java.util.concurrent.atomic.AtomicReference<Throwable> writerError =
                new java.util.concurrent.atomic.AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                while (!stop.get()) {
                    int i = writes.incrementAndGet();
                    cache.putTagged("k" + i, io.tiercache.spi.StoredEntry.ofValue("v"), ttl,
                            new String[]{"hot"});
                    Thread.sleep(4);
                }
            } catch (Throwable t) {
                writerError.set(t); // a dead writer must not go unnoticed
            }
        });
        writer.start();
        try {
            // > 3 TTL periods of uninterrupted load before measuring.
            Thread.sleep(1_600);
            if (writerError.get() != null) {
                throw new AssertionError("writer died before the measurement", writerError.get());
            }
            try (io.lettuce.core.RedisClient probeClient = io.lettuce.core.RedisClient.create(redisUri);
                    var probe = probeClient.connect(io.lettuce.core.codec.ByteArrayCodec.INSTANCE)) {
                // One atomic measurement: size and live count from the same
                // consistent snapshot (no SMEMBERS-then-EXISTS window).
                String measure =
                        "local members = redis.call('smembers', KEYS[1]) "
                                + "local live = 0 "
                                + "for _, m in ipairs(members) do "
                                + "if redis.call('exists', m) == 1 then live = live + 1 end "
                                + "end "
                                + "return {#members, live}";
                int writesBefore = writes.get();
                java.util.List<Object> result = probe.sync().eval(measure,
                        io.lettuce.core.ScriptOutputType.MULTI,
                        new byte[][]{RedisKeyspace.tagKey("tag-hot", "hot")});
                // The eval itself is milliseconds — too short to guarantee a
                // write inside it. Prove liveness instead: the counter must
                // advance right after the measurement, within a bounded wait.
                long livenessDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (writes.get() <= writesBefore && System.nanoTime() < livenessDeadline) {
                    Thread.sleep(10);
                }
                assertTrue(writes.get() > writesBefore,
                        "the writer must still be writing around the measurement (stuck at "
                                + writesBefore + " for over a second)");
                long size = (Long) result.get(0);
                long live = (Long) result.get(1);
                long bound = live + 2 * live / 7 + 32;
                assertTrue(size <= bound,
                        "set size " + size + " exceeds the janitor bound " + bound
                                + " (live=" + live + ", writes=" + writes.get() + ")");
                assertTrue(writes.get() > bound,
                        "the test is only meaningful if total writes (" + writes.get()
                                + ") far exceed the bound " + bound);
            }
        } finally {
            stop.set(true);
            writer.join(10_000);
            cache.close();
        }
        if (writerError.get() != null) {
            throw new AssertionError("writer died during the test", writerError.get());
        }
    }

    /**
     * Idle unique tags: hundreds of tags written once with short-lived data
     * and never touched again must not leak — each set expires within the
     * longest member TTL it has seen. Regression guard (the previous scheme
     * also expired idle sets; this locks the behavior in).
     */
    @Test
    void idleUniqueTagsExpire() throws Exception {
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("tag-idle").build();
        for (int i = 0; i < 200; i++) {
            cache.putTagged("k" + i, io.tiercache.spi.StoredEntry.ofValue("v"),
                    Duration.ofMillis(300), new String[]{"t" + i});
        }
        Thread.sleep(600);
        try (io.lettuce.core.RedisClient probe = io.lettuce.core.RedisClient.create(redisUri);
                var conn = probe.connect()) {
            assertTrue(conn.sync().keys(new String(RedisKeyspace.tagPrefix("tag-idle"), StandardCharsets.US_ASCII) + "*").isEmpty(),
                    "idle tag sets must expire within the longest member TTL");
        }
        cache.close();
    }

    /**
     * Mixed TTLs, extend-only set TTL: a short-lived entry must never shrink
     * the shared index — after it expires, the long-lived entry is still
     * found by tag lookup.
     */
    @Test
    void mixedTtlsDoNotShrinkSharedIndex() throws Exception {
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("tag-mix").build();
        cache.putTagged("long", io.tiercache.spi.StoredEntry.ofValue("v"),
                Duration.ofMillis(1_500), new String[]{"mix"});
        cache.putTagged("short", io.tiercache.spi.StoredEntry.ofValue("v"),
                Duration.ofMillis(200), new String[]{"mix"});

        Thread.sleep(400); // the short entry is gone; the long one is not
        List<String> found = cache.keysByTag("mix");
        assertTrue(found.contains("long"),
                "the long-lived member must survive the short one's expiry, got " + found);
        assertTrue(!found.contains("short"),
                "the expired member must not be returned, got " + found);
        cache.close();
    }

    /**
     * Janitor vs concurrent rewrites, orchestrated per round: a batch of
     * victim members whose data keys have EXPIRED (the dangerous absent-key
     * state) inside a LARGE set — one prune scan spans the entire rewrite
     * burst, so the position of a victim in the SMEMBERS iteration cannot
     * hide the race — then each victim rewritten EXACTLY ONCE, then a
     * rewrite FREEZE until verification (a later rewrite could re-add a
     * wrongly removed membership and mask the defect). The atomic
     * check-and-remove must preserve every rewritten membership, every
     * round. (Negative control on the non-atomic check-and-remove variant
     * loses memberships within the rounds.)
     */
    @Test
    void janitorNeverRemovesLiveMembership() throws Exception {
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("tag-race").build();
        int victims = 30;
        int filler = 800; // one scan spans the whole rewrite burst
        for (int round = 0; round < 3; round++) {
            String tag = "jr" + round;
            // The dangerous state: victims' data keys are GONE, their
            // membership rows are dead in the set.
            for (int v = 0; v < victims; v++) {
                cache.putTagged("v" + round + "-" + v, io.tiercache.spi.StoredEntry.ofValue("v"),
                        Duration.ofMillis(100), new String[]{tag});
            }
            try (io.lettuce.core.RedisClient probeClient = io.lettuce.core.RedisClient.create(redisUri);
                    var probe = probeClient.connect(io.lettuce.core.codec.ByteArrayCodec.INSTANCE)) {
                byte[] setKey = RedisKeyspace.tagKey("tag-race", tag);
                // A long-lived tag (mixed-TTL reality): the SET outlives the
                // short-lived members, so the dead membership rows persist.
                probe.sync().pexpire(setKey, 30_000);
                Thread.sleep(150); // the victims' data expires; the set lives on
                // Filler members (no data keys needed) make the prune scan long
                // enough to cover the burst deterministically.
                for (int f = 0; f < filler; f++) {
                    probe.sync().sadd(setKey, ("f" + round + "-" + f).getBytes(StandardCharsets.UTF_8));
                }
            }

            // Pin the dangerous pre-state explicitly (a setup regression
            // must fail loudly, not turn the test vacuous): every victim's
            // data is gone AND the dead membership rows survived. The
            // janitor legitimately prunes some expired victims already
            // during setup, so the victim count is a floor, not an exact.
            for (int v = 0; v < victims; v++) {
                assertNull(cache.get("v" + round + "-" + v),
                        "round " + round + ": victim data must be expired before the race");
            }
            try (io.lettuce.core.RedisClient probeClient = io.lettuce.core.RedisClient.create(redisUri);
                    var probe = probeClient.connect(io.lettuce.core.codec.ByteArrayCodec.INSTANCE)) {
                byte[] setKey = RedisKeyspace.tagKey("tag-race", tag);
                long members = probe.sync().scard(setKey);
                assertTrue(members >= filler + victims / 3 && members <= filler + victims,
                        "round " + round + ": filler plus most dead victim rows must be present "
                                + "before the race, got " + members);
            }

            CountDownLatch go = new CountDownLatch(1);
            String scanTag = tag;
            java.util.concurrent.atomic.AtomicBoolean stopScan =
                    new java.util.concurrent.atomic.AtomicBoolean();
            Thread scan = new Thread(() -> {
                awaitLatch(go);
                while (!stopScan.get()) {
                    cache.keysByTag(scanTag); // read-time prune scans race the rewrites
                }
            });
            int rewriteRound = round;
            Thread rewrite = new Thread(() -> {
                awaitLatch(go);
                // Let the first (slow, on a broken implementation) prune scan
                // settle into its member loop, then pace the burst so it
                // lands INSIDE that scan's check-and-remove window.
                try {
                    Thread.sleep(150);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int v = 0; v < victims; v++) {
                    cache.putTagged("v" + rewriteRound + "-" + v,
                            io.tiercache.spi.StoredEntry.ofValue("v"),
                            Duration.ofSeconds(30), new String[]{scanTag});
                    try {
                        Thread.sleep(8);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });
            scan.start();
            rewrite.start();
            go.countDown();
            rewrite.join(30_000); // each victim rewritten EXACTLY ONCE
            stopScan.set(true);
            scan.join(30_000);

            // FREEZE: no further victim writes before the check.
            List<String> members = cache.keysByTag(tag);
            for (int v = 0; v < victims; v++) {
                assertTrue(members.contains("v" + round + "-" + v),
                        "round " + round + ": the rewritten membership of victim " + v
                                + " must survive the racing prune");
            }
        }
        cache.close();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
