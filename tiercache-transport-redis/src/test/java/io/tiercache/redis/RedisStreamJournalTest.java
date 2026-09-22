package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import io.tiercache.spi.StoredEntry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: invalidation — journal atomicity, framing v2 round-trip and
 * backward compatibility, replay and overflow against real Redis.
 */
class RedisStreamJournalTest {

    private static GenericContainer<?> server;
    private static RedisClient client;
    private static String redisUri;

    @BeforeAll
    static void startServer() {
        server = new GenericContainer<>(ServerProfile.image())
                .withExposedPorts(6379);
        server.start();
        redisUri = "redis://" + server.getHost() + ":" + server.getMappedPort(6379);
        client = RedisClient.create(redisUri);
    }

    @AfterAll
    static void stopServer() {
        client.shutdown();
        server.stop();
    }

    @Test
    void framingV2RoundTripsVersion() {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 1000,
                new JdkCacheSerializer<>());
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("v2")
                .journal(journal)
                .build();
        Version version = new Version(42, UUID.randomUUID());
        cache.put("k", StoredEntry.ofValue("data", version), Duration.ofMinutes(1));
        StoredEntry<String> entry = cache.get("k");
        assertEquals("data", entry.value());
        assertEquals(version, entry.version());

        // Marker with version
        cache.put("m", StoredEntry.nullMarker(version), Duration.ofMinutes(1));
        assertTrue(cache.get("m").isNullMarker());
        assertEquals(version, cache.get("m").version());
        cache.close();
    }

    @Test
    void framingV1ReadsAsUnversioned() {
        // Write via a journal-less cache (v1 framing), read via v2-capable one.
        LettuceRemoteCache<String, String> plain = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("v1").build();
        plain.put("k", StoredEntry.ofValue("legacy"), Duration.ofMinutes(1));
        assertNull(plain.get("k").version(), "v1 framing decodes with null version");
        plain.close();
    }

    @Test
    void journalRowsMatchDataWritesAtomically() {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 1000,
                new JdkCacheSerializer<>());
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("jatomic")
                .journal(journal)
                .build();
        UUID origin = UUID.randomUUID();
        cache.put("a", StoredEntry.ofValue("1", new Version(1, origin)), Duration.ofMinutes(1));
        cache.evict("b", new Version(2, origin));

        List<io.tiercache.spi.JournalRow> rows = journal.readRange("jatomic", "0-0");
        assertEquals(2, rows.size());
        assertEquals("a", rows.get(0).message().key());
        assertEquals(new Version(1, origin), rows.get(0).message().version());
        assertEquals("b", rows.get(1).message().key());
        cache.close();
    }

    @Test
    void setIfAbsentWithJournalIsAtomic() {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 1000,
                new JdkCacheSerializer<>());
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("jsia")
                .journal(journal)
                .build();
        UUID origin = UUID.randomUUID();
        assertTrue(cache.setIfAbsent("k", StoredEntry.ofValue("v", new Version(1, origin)),
                Duration.ofMinutes(1)));
        // Loser: no journal row appended.
        org.junit.jupiter.api.Assertions.assertFalse(cache.setIfAbsent("k",
                StoredEntry.ofValue("x", new Version(2, origin)), Duration.ofMinutes(1)));
        assertEquals(1, journal.readRange("jsia", "0-0").size());
        cache.close();
    }

    @Test
    void extendedFrameStaysVersionComparableInLua() {
        // Stale-window write with a version goes through the conditional Lua
        // write; the script must read the version out of the extended frame.
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 1000,
                new JdkCacheSerializer<>());
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("v3")
                .journal(journal)
                .build();
        UUID origin = UUID.randomUUID();
        cache.put("k", StoredEntry.ofValue("new", new Version(5, origin)),
                Duration.ofMinutes(1), Duration.ofMinutes(1));
        StoredEntry<String> entry = cache.get("k");
        assertEquals("new", entry.value());
        assertEquals(new Version(5, origin), entry.version());
        assertTrue(entry.hasWriteTimestamp());

        // An older write loses against the extended frame's version.
        org.junit.jupiter.api.Assertions.assertFalse(cache.putIfNewer("k",
                StoredEntry.ofValue("old", new Version(4, origin)), Duration.ofMinutes(1)));
        assertEquals("new", cache.get("k").value());
        cache.close();
    }

    @Test
    void trimmedCursorIsDetected() {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 65,
                new JdkCacheSerializer<>());
        UUID origin = UUID.randomUUID();
        // Approximate MAXLEN trims lazily, so drive the stream far past the
        // window: 200 appends to a capacity-65 journal guarantee real trims.
        for (int i = 1; i <= 200; i++) {
            journal.append("trimmed", new InvalidationMessage("trimmed", "k" + i,
                    new Version(i, origin), origin, InvalidationMessage.Type.INVALIDATE));
        }
        String firstRemaining = journal.endCursor("trimmed");
        org.junit.jupiter.api.Assertions.assertFalse(journal.isTrimmed("trimmed", firstRemaining));
        // Cursor from before the window: rows it missed were genuinely trimmed.
        assertTrue(journal.isTrimmed("trimmed", "0-0"));
    }

    /**
     * Exact trim accounting (reviewer-reported): the beginning-cursor check
     * must fire even when the stream length equals the capacity exactly —
     * the old "XLEN > capacity" heuristic missed that boundary. Trims are
     * counted atomically at write time instead.
     */
    @Test
    void trimIsDetectedEvenAtExactCapacityLength() {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 1000,
                new JdkCacheSerializer<>());
        UUID origin = UUID.randomUUID();
        for (int i = 1; i <= 1200; i++) { // 200 rows past the cap: trims fire
            journal.append("boundary", new InvalidationMessage("boundary", "k" + i,
                    new Version(i, origin), origin, InvalidationMessage.Type.INVALIDATE));
        }
        var probe = client.connect(ByteArrayCodec.INSTANCE);
        try {
            var sync = probe.sync();
            byte[] streamKey = RedisStreamJournal.streamKeyBytes("boundary");
            byte[] counter = sync.get(RedisStreamJournal.trimCounterKeyBytes("boundary"));
            assertTrue(counter != null && Long.parseLong(new String(counter)) > 0,
                    "the drive must have counted real trims");
            // Land exactly on the boundary the length heuristic missed.
            sync.xtrim(streamKey, io.lettuce.core.XTrimArgs.Builder.maxlen(1000));
            assertEquals(1000L, sync.xlen(streamKey),
                    "setup: the stream must sit at exactly the capacity");
            assertTrue(journal.isTrimmed("boundary", "0-0"),
                    "a beginning cursor must see the trim even when XLEN == capacity");
        } finally {
            probe.close();
        }
    }

    /**
     * The trim counter is incremented by the L2 Lua write paths too (not
     * only by direct journal appends): every journal-appending path routes
     * through the same trim-counted append.
     */
    @Test
    void trimCounterIncrementsOnTheLuaWritePaths() {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 65,
                new JdkCacheSerializer<>());
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("jctr")
                .journal(journal)
                .build();
        io.tiercache.VersionGenerator versions = new io.tiercache.VersionGenerator();
        for (int i = 0; i < 200; i++) { // capacity 65: trims are guaranteed
            cache.put("k" + i, StoredEntry.ofValue("v", versions.next()), Duration.ofMinutes(1));
        }
        var probe = client.connect(ByteArrayCodec.INSTANCE);
        try {
            byte[] counter = probe.sync().get(RedisStreamJournal.trimCounterKeyBytes("jctr"));
            assertTrue(counter != null && Long.parseLong(new String(counter)) > 0,
                    "the conditional-write Lua path must count its trims");
        } finally {
            probe.close();
        }
        cache.close();
    }

    /**
     * Concurrent trims during checked reads: the integrity proof and the
     * range always come from one response — an intact read's head IS the
     * cursor row, a non-intact read never masquerades as a contiguous
     * prefix (its head, if any, lies strictly after the cursor).
     */
    @Test
    void checkedReadNeverReturnsATornPrefix() throws Exception {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 65,
                new JdkCacheSerializer<>());
        UUID origin = UUID.randomUUID();
        String cursor = null;
        java.util.concurrent.atomic.AtomicInteger appended = new java.util.concurrent.atomic.AtomicInteger();
        for (int i = 0; i < 50; i++) {
            String id = journal.append("torn", new InvalidationMessage("torn", "k" + i,
                    new Version(i + 1, origin), origin, InvalidationMessage.Type.INVALIDATE));
            appended.incrementAndGet();
            if (i == 40) {
                cursor = id; // will be trimmed as the writer keeps appending
            }
        }
        final String fixedCursor = cursor;
        java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
        Thread writer = new Thread(() -> {
            while (!stop.get()) {
                int i = appended.incrementAndGet();
                journal.append("torn", new InvalidationMessage("torn", "k" + i,
                        new Version(i + 1, origin), origin, InvalidationMessage.Type.INVALIDATE));
            }
        });
        writer.start();
        boolean sawTrimmedCursor = false;
        try {
            for (int i = 0; i < 2000 && !sawTrimmedCursor; i++) {
                io.tiercache.spi.CheckedRange range = journal.checkedRead("torn", fixedCursor, 50);
                if (range.startIntact()) {
                    assertTrue(range.rows().stream().allMatch(row -> RedisStreamJournal.compareIds(fixedCursor, row.cursor()) < 0),
                            "the raw-validated anchor is omitted; events start strictly after it");
                    assertTrue(range.rows().size() <= 50);
                } else if (!range.rows().isEmpty()) {
                    assertTrue(RedisStreamJournal.compareIds(fixedCursor, range.rows().get(0).cursor()) < 0,
                            "a non-intact read must not pose as a contiguous prefix");
                    sawTrimmedCursor = true;
                } else {
                    sawTrimmedCursor = true; // empty journal: integrity equally unconfirmable
                }
            }
        } finally {
            stop.set(true);
            writer.join(10_000);
        }
        assertTrue(sawTrimmedCursor, "the drive must eventually trim the cursor row");
    }

    /**
     * Regression: direct {@code append(UPDATE)} serialized the payload with
     * the key serializer while replay deserializes it with the value
     * serializer, so a row written with distinct serializers failed to
     * replay. Write and read must be symmetric on the value serializer.
     */
    @Test
    void directAppendSerializesPayloadWithValueSerializer() {
        CacheSerializer<Object> prefixedStringKeys = new CacheSerializer<>() {
            @Override
            public byte[] toBytes(Object value) {
                return ("K:" + (String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }

            @Override
            public Object fromBytes(byte[] bytes) {
                return new String(bytes, java.nio.charset.StandardCharsets.UTF_8).substring(2);
            }
        };
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 1000,
                prefixedStringKeys, new JdkCacheSerializer<>());
        UUID origin = UUID.randomUUID();
        java.util.ArrayList<String> payload = new java.util.ArrayList<>(List.of("x", "y", "z"));
        journal.append("mixed-ser", new InvalidationMessage("mixed-ser", "k",
                new Version(1, origin), origin, InvalidationMessage.Type.UPDATE, payload));

        List<io.tiercache.spi.JournalRow> rows = journal.readRange("mixed-ser", "0-0");
        assertEquals(1, rows.size());
        assertEquals("k", rows.get(0).message().key(), "key still round-trips through the key serializer");
        assertEquals(InvalidationMessage.Type.UPDATE, rows.get(0).message().type());
        assertEquals(payload, rows.get(0).message().payload(),
                "payload must round-trip through the value serializer, not the key serializer");
    }

    @Test
    void singleSerializerConstructorRoundTripsPayload() {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 1000,
                new JdkCacheSerializer<>());
        UUID origin = UUID.randomUUID();
        journal.append("single-ser", new InvalidationMessage("single-ser", "k",
                new Version(1, origin), origin, InvalidationMessage.Type.UPDATE, "v"));

        List<io.tiercache.spi.JournalRow> rows = journal.readRange("single-ser", "0-0");
        assertEquals(1, rows.size());
        assertEquals("v", rows.get(0).message().payload());
    }

    @Test
    void payloadlessMessagesAreUnaffected() {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 1000,
                new JdkCacheSerializer<>(), new JdkCacheSerializer<>());
        UUID origin = UUID.randomUUID();
        journal.append("no-payload", new InvalidationMessage("no-payload", "k",
                new Version(1, origin), origin, InvalidationMessage.Type.INVALIDATE));
        journal.append("no-payload", new InvalidationMessage("no-payload", null,
                new Version(2, origin), origin, InvalidationMessage.Type.EVICT_ALL));

        List<io.tiercache.spi.JournalRow> rows = journal.readRange("no-payload", "0-0");
        assertEquals(2, rows.size());
        assertEquals(InvalidationMessage.Type.INVALIDATE, rows.get(0).message().type());
        assertNull(rows.get(0).message().payload());
        assertEquals(InvalidationMessage.Type.EVICT_ALL, rows.get(1).message().type());
        assertNull(rows.get(1).message().key());
        assertNull(rows.get(1).message().payload());
    }

    /**
     * Cross-instance causality at the Lua conditional-write path: a fresh
     * instance's later write must beat a long-running instance's earlier
     * writes (regression: per-instance counters starting at 1 made the Lua
     * {@code newer()} comparison reject the newer write).
     */
    @Test
    void freshInstanceLaterWriteWinsConditionalLua() throws Exception {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 1000,
                new JdkCacheSerializer<>());
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("fresh-wins")
                .journal(journal)
                .build();
        io.tiercache.VersionGenerator longRunning = new io.tiercache.VersionGenerator();
        for (int i = 0; i < 100; i++) {
            assertTrue(cache.putIfNewer("k",
                    StoredEntry.ofValue("a-" + i, longRunning.next()), Duration.ofMinutes(1)));
        }
        // Strictly later in real time: the clock moves past the burst.
        Thread.sleep(5);
        io.tiercache.VersionGenerator fresh = new io.tiercache.VersionGenerator();
        assertTrue(cache.putIfNewer("k",
                StoredEntry.ofValue("b", fresh.next()), Duration.ofMinutes(1)),
                "fresh instance's later write must be accepted as newer");
        assertEquals("b", cache.get("k").value());

        // The versioned evict from the fresh instance is likewise accepted.
        cache.evict("k", fresh.next());
        assertNull(cache.get("k"), "tombstone hides the entry after the versioned evict");
        cache.close();
    }

}
