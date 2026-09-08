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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: redis-l2-transport — cross-instance atomicity, fail-fast timeouts
 * (F-33), pluggable serialization.
 */
class LettuceRemoteCacheTest {

    private static GenericContainer<?> server;
    private static String redisUri;

    @BeforeAll
    static void startServer() {
        server = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine"))
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
                    if (instance.setIfAbsent("k", candidate, Duration.ofMinutes(1))) {
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
        // configured timeout, no indefinite blocking (F-33).
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
            cache.put("Key", "MixedCase", Duration.ofMinutes(1));
            assertEquals("mixedcase", cache.get("Key"));
        }
    }

    private static LettuceRemoteCache<String, String> sharedNamespaceInstance() {
        return LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("xinst") // shared namespace on purpose
                .build();
    }
}
