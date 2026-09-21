package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.tiercache.testkit.RemoteCacheContractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.ScriptOutputType;
import io.tiercache.Version;
import io.tiercache.InvalidationMode;
import io.tiercache.spi.StoredEntry;
import io.tiercache.spi.TaggedWriteOutcome;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static io.tiercache.spi.TaggedWriteOutcome.*;

/**
 * Runs the shared {@link RemoteCacheContractTest} suite against a real
 * server in a container. Subclasses pick the image.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractLettuceContractTest extends RemoteCacheContractTest {

    private static final int REDIS_PORT = 6379;

    private GenericContainer<?> server;
    private RedisClient client;

    abstract DockerImageName image();

    @BeforeAll
    void startServer() {
        server = new GenericContainer<>(image()).withExposedPorts(REDIS_PORT);
        server.start();
        client = RedisClient.create(
                "redis://" + server.getHost() + ":" + server.getMappedPort(REDIS_PORT));
    }

    @AfterAll
    void stopServer() {
        if (client != null) {
            client.shutdown();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Override
    protected LettuceRemoteCache<String, String> newCache() {
        // Unique namespace per contract invocation keeps tests isolated.
        return LettuceRemoteCache.<String, String>builder("redis://unused")
                .client(client)
                .cacheName("contract-" + UUID.randomUUID())
                .build();
    }

    String redisUri() {
        return "redis://" + server.getHost() + ":" + server.getMappedPort(REDIS_PORT);
    }

    RedisClient sharedClient() {
        return client;
    }
    @org.junit.jupiter.api.Test
    void rejectedTaggedWriteKeepsWinningMembership() {
        String name = "tagged-loss-" + UUID.randomUUID();
        try (var connection = client.connect(io.lettuce.core.codec.ByteArrayCodec.INSTANCE)) {
            var journal = new RedisStreamJournal(connection, 1000, new JdkCacheSerializer<>());
            try (var cache = LettuceRemoteCache.<String, String>builder(redisUri())
                    .client(client).cacheName(name).journal(journal).build()) {
                UUID writer = UUID.randomUUID();
                cache.putTagged("k", io.tiercache.spi.StoredEntry.ofValue("new",
                        new io.tiercache.Version(20, writer)), java.time.Duration.ofMinutes(1),
                        new String[]{"winner"});
                long rows = journal.size(name);
                cache.putTagged("k", io.tiercache.spi.StoredEntry.ofValue("old",
                        new io.tiercache.Version(10, writer)), java.time.Duration.ofMinutes(1),
                        new String[]{"loser"});
                org.junit.jupiter.api.Assertions.assertEquals("new", cache.get("k").value());
                org.junit.jupiter.api.Assertions.assertTrue(cache.keysByTag("loser").isEmpty(),
                        "a rejected tagged candidate cannot acquire membership");
                org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("k"), cache.keysByTag("winner"));
                org.junit.jupiter.api.Assertions.assertEquals(rows, journal.size(name));
            }
        }
    }

    @org.junit.jupiter.api.Test
    void acceptedRetaggingRemovesSupersededMembership() {
        try (var cache = newCache()) {
            cache.putTagged("k", io.tiercache.spi.StoredEntry.ofValue("first"),
                    java.time.Duration.ofMinutes(1), new String[]{"A"});
            cache.putTagged("k", io.tiercache.spi.StoredEntry.ofValue("second"),
                    java.time.Duration.ofMinutes(1), new String[]{"B"});
            org.junit.jupiter.api.Assertions.assertTrue(cache.keysByTag("A").isEmpty(),
                    "an accepted retag must remove previous membership");
            org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("k"), cache.keysByTag("B"));
        }
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    private static byte[] dataKey(String name, String key) {
        return RedisKeyspace.dataKey(name, new JdkCacheSerializer<String>().toBytes(key));
    }

    private static byte[] reverseKey(String name, String key) {
        return RedisKeyspace.reverseKey(name, new JdkCacheSerializer<String>().toBytes(key));
    }

    @Test
    void losingWriteLeavesAllBytesAndExpirationsUntouchedIncludingTombstones() {
        String name = "snapshot-" + UUID.randomUUID();
        try (var connection = client.connect(ByteArrayCodec.INSTANCE)) {
            var cmd = connection.sync();
            var journal = new RedisStreamJournal(connection, 128, new JdkCacheSerializer<>());
            try (var cache = LettuceRemoteCache.<String, String>builder(redisUri()).client(client)
                    .cacheName(name).journal(journal).build()) {
                var id = UUID.randomUUID();
                assertEquals(WON, cache.putTaggedIfNewer("k", StoredEntry.ofValue("new", new Version(20, id)),
                        Duration.ofMinutes(1), new String[]{"A"}));
                byte[] data = dataKey(name, "k");
                byte[][] keys = {data, reverseKey(name, "k"), RedisKeyspace.tagKey(name, "A"),
                        RedisKeyspace.tagKey(name, "B"), RedisStreamJournal.streamKey(name),
                        RedisStreamJournal.trimCounterKey(name)};
                for (boolean tombstone : new boolean[]{false, true}) {
                    if (tombstone) cache.evict("k", new Version(30, id));
                    List<byte[]> snapshot = new ArrayList<>();
                    for (byte[] key : keys) snapshot.add(cmd.dump(key));
                    long ttl = cmd.pttl(data);
                    assertEquals(LOST, cache.putTaggedIfNewer("k", StoredEntry.ofValue("old", new Version(10, id)),
                            Duration.ofHours(1), new String[]{"B"}));
                    for (int i = 0; i < keys.length; i++) assertArrayEquals(snapshot.get(i), cmd.dump(keys[i]));
                    assertTrue(cmd.pttl(data) <= ttl, "a loss cannot extend expiry");
                    assertTrue(cache.keysByTag("B").isEmpty());
                    if (tombstone) assertNull(cache.get("k"));
                    else assertEquals("new", cache.get("k").value());
                }
            }
        }
    }

    @Test
    void retaggingPreservesSharedMembershipExactReverseTtlAndExtendOnlyTagTtl() {
        String name = "ttl-" + UUID.randomUUID();
        try (var connection = client.connect(ByteArrayCodec.INSTANCE);
                var cache = LettuceRemoteCache.<String, String>builder(redisUri()).client(client).cacheName(name).build()) {
            var cmd = connection.sync();
            cache.putTagged("other", StoredEntry.ofValue("shared"), Duration.ofMinutes(2), new String[]{"A"});
            cache.putTagged("k", StoredEntry.ofValue("old"), Duration.ofMinutes(2), new String[]{"A", "keep"});
            long before = cmd.pttl(RedisKeyspace.tagKey(name, "keep"));
            cache.putTagged("k", StoredEntry.ofValue("new"), Duration.ofSeconds(30), new String[]{"B", "keep", "B"});
            assertEquals(List.of("other"), cache.keysByTag("A"));
            assertEquals(List.of("k"), cache.keysByTag("B"));
            byte[] data = dataKey(name, "k");
            assertSameExpiry(cmd, data, reverseKey(name, "k"));
            long after = cmd.pttl(RedisKeyspace.tagKey(name, "keep"));
            assertTrue(after > 90000 && after <= before, "retained tags must not shrink to the new 30s TTL");
            assertEquals(java.util.Set.of(RedisKeyspace.token("B"), RedisKeyspace.token("keep")), cmd.smembers(reverseKey(name, "k")).stream()
                    .map(b -> new String(b, StandardCharsets.UTF_8)).collect(java.util.stream.Collectors.toSet()));
            for (String key : cache.keysByTag("A")) cache.evict(key);
            assertEquals("new", cache.get("k").value(), "old tag eviction must not delete the retagged value");
            for (String key : cache.keysByTag("B")) cache.evict(key);
            assertNull(cache.get("k")); assertTrue(cache.keysByTag("keep").isEmpty());
            assertEquals(0, cmd.exists(reverseKey(name, "k")));
        }
    }

    @Test
    void concurrentTaggedWritersShareOneAcceptanceDecision() throws Exception {
        String name = "concurrent-" + UUID.randomUUID();
        try (var connection = client.connect(ByteArrayCodec.INSTANCE)) {
            var journal = new RedisStreamJournal(connection, 1000, new JdkCacheSerializer<>());
            try (var first = LettuceRemoteCache.<String, String>builder(redisUri()).client(client).cacheName(name).journal(journal).build();
                 var second = LettuceRemoteCache.<String, String>builder(redisUri()).client(client).cacheName(name).journal(journal).build()) {
                var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
                var start = new java.util.concurrent.CountDownLatch(1);
                var id = UUID.randomUUID();
                try {
                    var low = executor.submit(() -> { start.await(); int won = 0;
                        for (int i = 1; i <= 100; i++) if (first.putTaggedIfNewer("k", StoredEntry.ofValue("v" + i,
                                new Version(i, id)), Duration.ofMinutes(1), new String[]{"t" + i}) == WON) won++;
                        return won;
                    });
                    var high = executor.submit(() -> { start.await(); int won = 0;
                        for (int i = 200; i > 100; i--) if (second.putTaggedIfNewer("k", StoredEntry.ofValue("v" + i,
                                new Version(i, id)), Duration.ofMinutes(1), new String[]{"t" + i}) == WON) won++;
                        return won;
                    });
                    start.countDown();
                    int accepted = low.get(15, java.util.concurrent.TimeUnit.SECONDS) + high.get(15, java.util.concurrent.TimeUnit.SECONDS);
                    assertEquals(accepted, journal.size(name));
                    assertEquals("v200", first.get("k").value());
                    assertEquals(List.of("k"), first.keysByTag("t200"));
                    for (int i = 1; i < 200; i++) assertTrue(first.keysByTag("t" + i).isEmpty());
                } finally { executor.shutdownNow(); }
            }
        }
    }

    @Test
    void updateJournalAndTrimAccountingRemainCompatible() {
        String name = "journal-" + UUID.randomUUID();
        try (var connection = client.connect(ByteArrayCodec.INSTANCE)) {
            var journal = new RedisStreamJournal(connection, 128, new JdkCacheSerializer<>());
            try (var cache = LettuceRemoteCache.<String, String>builder(redisUri()).client(client).cacheName(name)
                    .journal(journal).invalidationMode(InvalidationMode.UPDATE, 65536).build()) {
                var id = UUID.randomUUID();
                for (int i = 1; i <= 350; i++) assertEquals(WON, cache.putTaggedIfNewer("k",
                        StoredEntry.ofValue("value" + i, new Version(i, id)), Duration.ofMinutes(1), new String[]{"tag"}));
                var rows = journal.readRange(name, "0-0");
                var last = rows.get(rows.size() - 1).message();
                assertEquals("k", last.key()); assertEquals(new Version(350, id), last.version());
                assertEquals(io.tiercache.InvalidationMessage.Type.UPDATE, last.type());
                assertEquals("value350", last.payload());
                byte[] trims = connection.sync().get(RedisStreamJournal.trimCounterKey(name));
                assertNotNull(trims); assertTrue(Long.parseLong(new String(trims, StandardCharsets.UTF_8)) > 0);
                long size = journal.size(name);
                assertEquals(LOST, cache.putTaggedIfNewer("k", StoredEntry.ofValue("old", new Version(1, id)),
                        Duration.ofMinutes(1), new String[]{"bad"}));
                assertEquals(size, journal.size(name));
                assertArrayEquals(trims, connection.sync().get(RedisStreamJournal.trimCounterKey(name)));
                assertEquals(WON, cache.putTaggedIfNewer("k", StoredEntry.nullMarker(new Version(351, id)),
                        Duration.ofMinutes(1), new String[]{"null"}));
                assertTrue(cache.get("k").isNullMarker()); assertTrue(cache.keysByTag("tag").isEmpty());
            }
        }
    }

    @Test
    void noJournalAndUnversionedTaggedWritesRemainUnfenced() {
        String name = "unfenced-" + UUID.randomUUID();
        try (var cache = LettuceRemoteCache.<String, String>builder(redisUri()).client(client).cacheName(name).build()) {
            var id = UUID.randomUUID();
            cache.putTagged("k", StoredEntry.ofValue("new", new Version(20, id)), Duration.ofMinutes(1), new String[]{"A"});
            assertEquals(WON, cache.putTaggedIfNewer("k", StoredEntry.ofValue("old", new Version(10, id)),
                    Duration.ofMinutes(1), new String[]{"B"}));
            assertEquals("old", cache.get("k").value()); assertTrue(cache.keysByTag("A").isEmpty());
        }
        try (var connection = client.connect(ByteArrayCodec.INSTANCE)) {
            var journal = new RedisStreamJournal(connection, 128, new JdkCacheSerializer<>());
            try (var cache = LettuceRemoteCache.<String, String>builder(redisUri()).client(client).cacheName(name).journal(journal).build()) {
                assertEquals(WON, cache.putTaggedIfNewer("k", StoredEntry.ofValue("plain"), Duration.ofMinutes(1), new String[]{"C"}));
                assertEquals(0, journal.size(name)); assertEquals("plain", cache.get("k").value());
                assertTrue(cache.keysByTag("B").isEmpty());
            }
        }
    }

    @Test
    void invalidTaggedArgumentsDoNotPartiallyMutateRedis() {
        try (var cache = newCache()) {
            cache.putTagged("k", StoredEntry.ofValue("old"), Duration.ofMinutes(1), new String[]{"A"});
            assertThrows(NullPointerException.class, () -> cache.putTaggedIfNewer("k", StoredEntry.ofValue("bad"),
                    Duration.ofMinutes(1), new String[]{"B", null}));
            assertThrows(IllegalArgumentException.class, () -> cache.putTaggedIfNewer("k", StoredEntry.ofValue("bad"),
                    Duration.ZERO, new String[]{"B"}));
            assertEquals("old", cache.get("k").value());
            assertEquals(List.of("k"), cache.keysByTag("A")); assertTrue(cache.keysByTag("B").isEmpty());
        }
    }
    @Test
    void clearDoesNotCrossHierarchicalNamespace() {
        assertClearIsolation("user", "user:roles");
    }

    @Test
    void clearDoesNotInterpretNamespaceGlob() {
        assertClearIsolation("a?", "a1");
    }

    private void assertClearIsolation(String cleared, String retained) {
        String suffix = "-" + UUID.randomUUID();
        try (var first = LettuceRemoteCache.<String, String>builder(redisUri()).client(client).cacheName(cleared).build();
             var second = LettuceRemoteCache.<String, String>builder(redisUri()).client(client).cacheName(retained).build()) {
            first.put(suffix, StoredEntry.ofValue("own"), Duration.ofMinutes(1));
            second.put(suffix, StoredEntry.ofValue("other"), Duration.ofMinutes(1));
            first.clear();
            assertNull(first.get(suffix));
            assertNotNull(second.get(suffix), "clear must preserve another complete cache namespace");
            assertEquals("other", second.get(suffix).value());
            second.evict(suffix);
        }
    }

    @Test
    void largeTaggedWriteUsesOneExpirationInstant() {
        String name = "expiry-" + UUID.randomUUID();
        try (var connection = client.connect(ByteArrayCodec.INSTANCE);
             var cache = LettuceRemoteCache.<String, String>builder(redisUri()).client(client).cacheName(name).build()) {
            String[] tags = java.util.stream.IntStream.range(0, 3000).mapToObj(i -> "t" + i).toArray(String[]::new);
            cache.putTagged("k", StoredEntry.ofValue("v"), Duration.ofSeconds(30), tags);
            assertSameExpiry(connection.sync(), dataKey(name, "k"), reverseKey(name, "k"));
        }
    }

    private static void assertSameExpiry(io.lettuce.core.api.sync.RedisCommands<byte[], byte[]> commands,
            byte[] data, byte[] reverse) {
        // Redis 6.2 updates time during Lua execution. Compare PTTLs only when
        // both were sampled inside one server millisecond, without a tolerance.
        String script = "local a=redis.call('time'); local d=redis.call('pttl',KEYS[1]); "
                + "local r=redis.call('pttl',KEYS[2]); local b=redis.call('time'); "
                + "return {a[1]*1000+math.floor(a[2]/1000), b[1]*1000+math.floor(b[2]/1000), d, r}";
        for (int i = 0; i < 20; i++) {
            List<Long> sample = commands.eval(script, ScriptOutputType.MULTI, new byte[][]{data, reverse});
            if (sample.get(0).equals(sample.get(1))) {
                assertTrue(sample.get(2) > 0, "data must still be live");
                assertEquals(sample.get(2), sample.get(3), "data and reverse index must expire at exactly the same instant");
                return;
            }
        }
        fail("could not sample both PTTLs within one Redis millisecond");
    }

}
