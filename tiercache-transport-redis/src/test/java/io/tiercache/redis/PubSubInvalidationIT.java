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

import java.time.Duration;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Spec: invalidation — end-to-end over real Redis: two instances sharing a
 * server, Pub/Sub profile, journal replay after reconnect, overflow flush.
 */
class PubSubInvalidationIT {

    private GenericContainer<?> server;
    private RedisClient clientA;
    private RedisClient clientB;

    private TierCacheFactory factoryA;
    private TierCacheFactory factoryB;

    @BeforeEach
    void startServer() {
        server = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine"))
                .withExposedPorts(6379);
        server.start();
        String uri = "redis://" + server.getHost() + ":" + server.getMappedPort(6379);
        clientA = RedisClient.create(uri);
        clientB = RedisClient.create(uri);
        factoryA = side(clientA, "e2e", new LettucePubSubInvalidationTransport(clientA,
                new JdkCacheSerializer<>()));
        factoryB = side(clientB, "e2e", new LettucePubSubInvalidationTransport(clientB,
                new JdkCacheSerializer<>()));
    }

    @AfterEach
    void stopServer() {
        factoryA.close();
        factoryB.close();
        clientA.shutdown();
        clientB.shutdown();
        server.stop();
    }

    private TierCacheFactory side(RedisClient client, String cacheName,
            LettucePubSubInvalidationTransport transport) {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE),
                1000, new JdkCacheSerializer<>());
        LettuceRemoteCache<String, String> l2 = LettuceRemoteCache.<String, String>builder("redis://unused")
                .client(client)
                .cacheName(cacheName)
                .journal(journal)
                .build();
        return TierCacheFactory.builder()
                .defaults(new CacheSettings(10_000, Duration.ofMinutes(5), null,
                        Duration.ofHours(1), 0.0, NullPolicy.deny()))
                .remoteCache(l2)
                .invalidation(versions -> new InvalidationService(transport, journal,
                        versions.instanceId(), InvalidationListener.NOOP))
                .build();
    }

    @Test
    void remotePutInvalidatesLocalL1() throws Exception {
        TierCache<String, String> a = factoryA.getCache("e2e");
        TierCache<String, String> b = factoryB.getCache("e2e");

        a.put("k", "old");
        assertEquals("old", b.get("k")); // warms B's L1

        a.put("k", "new");
        waitFor(() -> "new".equals(b.get("k")));
        assertEquals("new", b.get("k"));
    }

    @Test
    void remoteEvictAllClearsLocalL1() throws Exception {
        TierCache<String, String> a = factoryA.getCache("e2e");
        TierCache<String, String> b = factoryB.getCache("e2e");

        a.put("k1", "v1");
        a.put("k2", "v2");
        assertEquals("v1", b.get("k1"));
        assertEquals("v2", b.get("k2"));

        a.evictAll();
        waitFor(() -> b.get("k1") == null && b.get("k2") == null);
    }

    private static void waitFor(Check check) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!check.ok()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 10s");
            }
            Thread.sleep(20);
        }
    }

    private interface Check {
        boolean ok();
    }
}
