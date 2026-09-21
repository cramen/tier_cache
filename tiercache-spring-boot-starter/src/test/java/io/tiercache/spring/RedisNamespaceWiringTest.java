package io.tiercache.spring;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.tiercache.TierCacheFactory;
import io.tiercache.TierCache;
import io.tiercache.InvalidationMessage;
import io.tiercache.redis.JdkCacheSerializer;
import io.tiercache.redis.RedisKeyspace;
import io.tiercache.redis.RedisStreamJournal;
import io.tiercache.spi.InvalidationHandler;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class RedisNamespaceWiringTest {
    @Test void v2NamespacesAndReplayWorkThroughTwoSpringContexts() {
        for (String image : new String[]{"redis:6.2-alpine", "valkey/valkey:8.0-alpine"}) {
            try (var server = new GenericContainer<>(DockerImageName.parse(image)).withExposedPorts(6379)) {
                server.start();
                String uri = "redis://" + server.getHost() + ":" + server.getMappedPort(6379);
                var runner = new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(TiercacheAutoConfiguration.class))
                        .withPropertyValues("tiercache.enabled=true", "tiercache.redis-uri=" + uri);
                runner.run(a -> runner.run(b -> {
                    assertNull(a.getStartupFailure()); assertNull(b.getStartupFailure());
                    verify(a.getBean(TierCacheFactory.class), b.getBean(TierCacheFactory.class),
                            a.getBean(RedisStreamJournal.class), a.getBean(io.lettuce.core.RedisClient.class), "spring:");
                }));
            }
        }
    }

    private static Object field(Object object, String name) throws Exception {
        var field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
    }

    @SuppressWarnings("unchecked")
    private static void verify(TierCacheFactory a, TierCacheFactory b, RedisStreamJournal journal,
            io.lettuce.core.RedisClient client, String prefix) throws Exception {
        TierCache<String, String> writer = a.getCache("user");
        TierCache<String, String> reader = b.getCache("user");
        TierCache<String, String> rolesA = a.getCache("user:roles");
        TierCache<String, String> rolesB = b.getCache("user:roles");
        InvalidationHandler handler = (InvalidationHandler) field(b, "invalidation");
        Object transport = field(handler, "transport");
        var subscriber = (StatefulRedisPubSubConnection<byte[], byte[]>) field(transport, "connection");
        Runnable reconnect = (Runnable) field(transport, "reconnectListener");
        var initial = new CountDownLatch(1);
        handler.setEventListener((cache, event) -> { if ("user".equals(cache) && "k".equals(event.key())) initial.countDown(); });
        writer.put("k", "old");
        assertTrue(initial.await(5, TimeUnit.SECONDS));
        assertEquals("old", reader.get("k"));
        try (var probe = client.connect(ByteArrayCodec.INSTANCE)) {
            var serializer = new JdkCacheSerializer<String>();
            assertNotNull(probe.sync().get(RedisKeyspace.dataKey(prefix + "user", serializer.toBytes("k"))));
            assertTrue(journal.size("user") > 0);
            assertEquals(0, journal.size(prefix + "user"), "physical prefix must not replace logical journal identity");

            // Deterministic Pub/Sub gap. Invoke the very callback registered by production wiring.
            subscriber.sync().unsubscribe();
            writer.put("k", "new");
            assertEquals("old", reader.get("k"));
            subscriber.sync().subscribe(RedisKeyspace.channel("user"), RedisKeyspace.channel("user:roles"));
            reconnect.run();
            assertEquals("new", reader.get("k"), "reconnect replay must use the writer's logical journal");

            subscriber.sync().unsubscribe();
            writer.evict("k");
            writer.put("clear", "cached");
            assertEquals("cached", reader.get("clear"));
            rolesA.put("keep", "roles");
            assertEquals("roles", rolesB.get("keep"));
            writer.evictAll();
            assertEquals("cached", reader.get("clear"), "the test must have a real missed invalidation");
            assertNotNull(probe.sync().get(RedisKeyspace.dataKey(prefix + "user:roles", serializer.toBytes("keep"))));
            assertTrue(journal.readRange("user", "0-0").stream()
                    .anyMatch(row -> row.message().type() == InvalidationMessage.Type.EVICT_ALL));
            subscriber.sync().subscribe(RedisKeyspace.channel("user"), RedisKeyspace.channel("user:roles"));
            reconnect.run();
            assertNull(reader.get("clear")); assertNull(reader.get("k"));
            assertEquals("roles", rolesB.get("keep"));

            var tagged = new CountDownLatch(1);
            handler.setEventListener((cache, event) -> { if ("tagged".equals(event.key())) tagged.countDown(); });
            writer.put("tagged", "v", "A:B");
            assertTrue(tagged.await(5, TimeUnit.SECONDS));
            assertEquals("v", reader.get("tagged"));
            writer.evictByTag("A:B");
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (reader.get("tagged") != null && System.nanoTime() < deadline) Thread.sleep(10);
            assertNull(reader.get("tagged"));
        }
    }
}
