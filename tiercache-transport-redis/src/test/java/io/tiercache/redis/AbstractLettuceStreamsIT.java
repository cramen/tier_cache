package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.CacheSettings;
import io.tiercache.NullPolicy;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.spi.InvalidationListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: invalidation — durable Streams profile over real Redis: events flow,
 * pending resumes within retained history or a safe registration/reset baseline.
 */
abstract class AbstractLettuceStreamsIT {

    abstract DockerImageName image();

    private GenericContainer<?> server;
    private RedisClient clientA;
    private RedisClient clientB;
    private TierCacheFactory factoryA;
    private TierCacheFactory factoryB;
    private LettuceStreamsInvalidationTransport transportB;
    private final java.util.UUID instanceB = java.util.UUID.randomUUID();

    @BeforeEach
    void startServer() {
        server = new GenericContainer<>(image())
                .withExposedPorts(6379);
        server.start();
        String uri = "redis://" + server.getHost() + ":" + server.getMappedPort(6379);
        clientA = RedisClient.create(uri);
        clientB = RedisClient.create(uri);
        factoryA = side(clientA, uri, null);
        factoryB = side(clientB, uri, new LettuceStreamsInvalidationTransport(clientB,
                new JdkCacheSerializer<>(), new JdkCacheSerializer<>(), instanceB));
        transportB = getTransport(factoryB);
    }

    @AfterEach
    void stop() {
        factoryA.close();
        factoryB.close();
        clientA.shutdown();
        clientB.shutdown();
        server.stop();
    }

    private TierCacheFactory side(RedisClient client, String uri,
            LettuceStreamsInvalidationTransport streams) {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE),
                1000, new JdkCacheSerializer<>());
        LettuceRemoteCache<String, String> l2 = LettuceRemoteCache.<String, String>builder(uri)
                .client(client)
                .cacheName("streams")
                .journal(journal)
                .build();
        var builder = TierCacheFactory.builder()
                .defaults(new CacheSettings(10_000, Duration.ofMinutes(5), null,
                        Duration.ofHours(1), 0.0, NullPolicy.deny(), io.tiercache.InvalidationMode.INVALIDATE, 64 * 1024))
                .remoteCache(l2);
        if (streams != null) {
            builder.invalidation(versions -> new InvalidationService(streams, journal,
                    versions.instanceId(), InvalidationListener.NOOP));
            this.transportB = streams;
        }
        return builder.build();
    }

    private LettuceStreamsInvalidationTransport getTransport(TierCacheFactory f) {
        return transportB; // set during side() for B
    }

    @Test
    void eventsFlowThroughStreamsProfile() throws Exception {
        TierCache<String, String> a = factoryA.getCache("streams");
        TierCache<String, String> b = factoryB.getCache("streams");
        awaitConsumerGroup("streams");

        a.put("k", "old");
        assertEquals("old", b.get("k")); // warms B's L1

        a.put("k", "new");
        waitFor(() -> "new".equals(b.get("k")));
    }

    /**
     * Regression: the consumer group must exist by the time subscription
     * returns. With lazy creation in the reader thread, entries published
     * between subscribe() and the loop's first pass sat permanently behind
     * the group cursor and were never delivered.
     */
    @Test
    void subscriptionCreatesConsumerGroupEagerly() {
        factoryB.getCache("streams");
        byte[] stream = RedisStreamJournal.streamKeyBytes("streams");
        byte[] group = RedisKeyspace.group("streams", instanceB);
        var connection = clientB.connect(ByteArrayCodec.INSTANCE);
        try {
            assertTrue(groupExists(connection.sync(), stream, group),
                    "consumer group must exist synchronously after subscribe");
        } finally {
            connection.close();
        }
    }

    @Test
    void stableResumeEstablishesSafeRegistrationBaseline() throws Exception {
        TierCache<String, String> a = factoryA.getCache("streams");
        TierCache<String, String> b = factoryB.getCache("streams");
        awaitConsumerGroup("streams");

        a.put("keep", "v");
        assertEquals("v", b.get("keep")); // warm B's L1

        // Simulate a partition of B: its reader thread stops consuming.
        transportB.close(); // transport closed: reader stops
        for (int i = 0; i < 5; i++) {
            a.put("flood-" + i, "v" + i);
        }
        a.evict("keep");
        assertEquals("v", b.get("keep"), "sanity: stale while partitioned");

        // Reconnect with the SAME instance identity: the durable group cursor
        // continues from where the partition started.
        LettuceStreamsInvalidationTransport reconnected = new LettuceStreamsInvalidationTransport(
                clientB, new JdkCacheSerializer<>(), new JdkCacheSerializer<>(), instanceB);
        // Rebuild service wiring on B's factory caches: re-register targets.
        // (The factory's service still points at the closed transport; instead
        // verify via a new service subscription on the same cache.)
        RedisStreamJournal journal = new RedisStreamJournal(clientB.connect(ByteArrayCodec.INSTANCE),
                1000, new JdkCacheSerializer<>());
        InvalidationService service = new InvalidationService(reconnected, journal,
                java.util.UUID.randomUUID(), InvalidationListener.NOOP);
        // Note: the service's origin id is fresh; LWW applies on versions, not ids.
        service.registerTarget("streams", (io.tiercache.spi.InvalidationTarget) factoryB.getCache("streams"));
        waitFor(() -> b.get("keep") == null);
        assertEquals("v0", b.get("flood-0"));
        service.close();
    }

    /**
     * Subscription creates the consumer group eagerly (see
     * {@link #subscriptionCreatesConsumerGroupEagerly}), so this helper is a
     * belt-and-braces confirmation rather than a race guard.
     */
    private void awaitConsumerGroup(String cache) throws InterruptedException {
        byte[] stream = RedisStreamJournal.streamKeyBytes(cache);
        byte[] group = RedisKeyspace.group(cache, instanceB);
        var connection = clientB.connect(ByteArrayCodec.INSTANCE);
        try {
            var sync = connection.sync();
            waitFor(() -> groupExists(sync, stream, group));
        } finally {
            connection.close();
        }
    }

    private static boolean groupExists(io.lettuce.core.api.sync.RedisCommands<byte[], byte[]> sync,
            byte[] stream, byte[] group) {
        List<Object> groups;
        try {
            groups = sync.xinfoGroups(stream);
        } catch (RuntimeException e) {
            return false; // stream not created yet (mkstream still pending)
        }
        for (Object g : groups) {
            List<Object> row = (List<Object>) g;
            for (int i = 0; i + 1 < row.size(); i += 2) {
                if ("name".equals(asString(row.get(i)))
                        && Arrays.equals(group, asBytes(row.get(i + 1)))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static String asString(Object o) {
        return o instanceof byte[] ? new String((byte[]) o, StandardCharsets.UTF_8) : String.valueOf(o);
    }

    private static byte[] asBytes(Object o) {
        return o instanceof byte[] ? (byte[]) o : String.valueOf(o).getBytes(StandardCharsets.UTF_8);
    }

    private static void waitFor(Check check) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!check.ok()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 10s");
            }
            Thread.sleep(50);
        }
    }

    @Test
    void poisonBatchCannotStrandPendingOrLeaveAStaleVictim() throws Exception {
        TierCache<String, String> a = factoryA.getCache("streams");
        TierCache<String, String> b = factoryB.getCache("streams");
        var observed = new java.util.concurrent.CopyOnWriteArrayList<Object>();
        var field = TierCacheFactory.class.getDeclaredField("invalidation"); field.setAccessible(true);
        ((io.tiercache.spi.InvalidationHandler) field.get(factoryB)).setEventListener((c, event) -> observed.add(event.key()));
        a.put("victim", "old"); a.put("batch", "old");
        waitFor(() -> observed.contains("victim") && observed.contains("batch"));
        assertEquals("old", b.get("victim")); assertEquals("old", b.get("batch")); observed.clear();
        byte[] stream = RedisKeyspace.journal("streams"); byte[] group = RedisKeyspace.group("streams", instanceB);
        var serializer = new JdkCacheSerializer<String>();
        var version = new io.tiercache.Version(Long.MAX_VALUE - 1, java.util.UUID.randomUUID());
        try (var connection = clientA.connect(ByteArrayCodec.INSTANCE)) {
            var cmd = connection.sync();
            cmd.del(RedisKeyspace.dataKey("streams", serializer.toBytes("victim")),
                    RedisKeyspace.dataKey("streams", serializer.toBytes("batch")));
            cmd.eval("redis.call('xadd',KEYS[1],'*','t',string.char(99),'k',ARGV[1],'v',ARGV[3]); "
                            + "redis.call('xadd',KEYS[1],'*','t',string.char(0),'k',ARGV[2],'v',ARGV[3]); return 1",
                    io.lettuce.core.ScriptOutputType.INTEGER, new byte[][]{stream},
                    serializer.toBytes("victim"), serializer.toBytes("batch"), version.toWire().getBytes(StandardCharsets.UTF_8));
            waitFor(() -> transportB.lastReaderError != null);
            var journal = new RedisStreamJournal(connection, 1000, new JdkCacheSerializer<>());
            journal.append("streams", new io.tiercache.InvalidationMessage("streams", "later", version,
                    version.instanceId(), io.tiercache.InvalidationMessage.Type.INVALIDATE));
            try { waitFor(() -> cmd.xpending(stream, group).getCount() == 0); }
            finally { System.out.println("Poison batch: pending=" + cmd.xpending(stream, group).getCount() + ", delivered=" + observed); }
            org.junit.jupiter.api.Assertions.assertNull(b.get("victim"), "ACK/skip without a clear leaves the corrupt-only victim stale");
            org.junit.jupiter.api.Assertions.assertNull(b.get("batch"), "the batch remainder must be applied or covered by a safe clear");
            journal.append("streams", new io.tiercache.InvalidationMessage("streams", "after", version,
                    version.instanceId(), io.tiercache.InvalidationMessage.Type.INVALIDATE));
            waitFor(() -> observed.contains("after"));
        }
    }

    private interface Check {
        boolean ok();
    }
}


class RedisStreamsIT extends AbstractLettuceStreamsIT {
    @Override
    DockerImageName image() {
        return DockerImageName.parse("redis:6.2-alpine");
    }
}

class ValkeyStreamsIT extends AbstractLettuceStreamsIT {
    @Override
    DockerImageName image() {
        return DockerImageName.parse("valkey/valkey:8.0-alpine");
    }
}
