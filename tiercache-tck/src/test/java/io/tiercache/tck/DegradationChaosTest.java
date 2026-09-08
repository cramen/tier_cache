package io.tiercache.tck;

import io.tiercache.CacheSettings;
import io.tiercache.NullPolicy;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.internal.CircuitBreaker;
import io.tiercache.redis.LettuceRemoteCache;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Redis degradation (T-05): the server is paused under read load; business
 * operations continue at L1-only latency, zero infrastructure exceptions
 * escape, the degraded signal fires; after unpause the cache recovers and
 * L1 survives (no reconnect flush). And reconnect storm (T-06): several
 * instances recover at once without a loader spike.
 */
abstract class AbstractDegradationChaosTest {

    abstract DockerImageName image();

    private static final CircuitBreaker.Config FAST =
            new CircuitBreaker.Config(10, 0.5, 3, Duration.ofMillis(200), 2);

    private TierCacheFactory factoryFor(String uri) {
        // Own client so the fast command timeout applies (fail fast when paused).
        LettuceRemoteCache<String, String> l2 = LettuceRemoteCache.<String, String>builder(uri)
                .cacheName("degradation")
                .commandTimeout(Duration.ofMillis(100))
                .build();
        return TierCacheFactory.builder()
                .defaults(new CacheSettings(10_000, Duration.ofMinutes(5), null,
                        Duration.ofHours(1), 0.0, NullPolicy.deny()))
                .remoteCache(l2)
                .circuitBreakerConfig(FAST)
                .build();
    }

    @Test
    void t05RedisOutageDegradesToL1Only() throws Exception {
        try (var server = AbstractInvalidationChaosTest.startServer(image())) {
            String uri = AbstractInvalidationChaosTest.uri(server);
            TierCacheFactory factory = factoryFor(uri);
            TierCache<String, String> cache = factory.getCache("degradation");
            try {
                cache.put("hot", "v");
                assertEquals("v", cache.get("hot")); // warm L1

                pause(server, true);
                long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
                while (!factory.isDegraded() && System.nanoTime() < deadline) {
                    cache.get("cold-" + System.nanoTime() % 1000); // failures accumulate
                }
                assertTrue(factory.isDegraded(), "breaker must open during the outage");

                // Business operations at L1-only level, zero escaping exceptions.
                AtomicLong worstNanos = new AtomicLong();
                AtomicInteger failures = new AtomicInteger();
                for (int i = 0; i < 500; i++) {
                    long start = System.nanoTime();
                    try {
                        assertEquals("v", cache.get("hot"));
                    } catch (RuntimeException e) {
                        failures.incrementAndGet();
                    }
                    worstNanos.updateAndGet(w -> Math.max(w, System.nanoTime() - start));
                }
                assertEquals(0, failures.get(), "no infrastructure exception may escape");
                assertTrue(worstNanos.get() < 50_000_000,
                        "degraded reads must stay at L1 level, worst was " + worstNanos.get() / 1_000_000 + " ms");

                pause(server, false);
                long recoveryDeadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
                while (factory.isDegraded() && System.nanoTime() < recoveryDeadline) {
                    cache.get("hot2-" + System.nanoTime() % 1000); // probes drive recovery
                    Thread.sleep(50);
                }
                assertFalse(factory.isDegraded(), "breaker must close after recovery");
                assertEquals("v", cache.get("hot"), "L1 survived recovery (no reconnect flush)");
            } finally {
                factory.close();
            }
        }
    }

    @Test
    void t06ReconnectWithoutLoaderSpike() throws Exception {
        try (var server = AbstractInvalidationChaosTest.startServer(image())) {
            String uri = AbstractInvalidationChaosTest.uri(server);
            int instances = 3;
            List<TierCacheFactory> factories = new ArrayList<>();
            List<TierCache<String, String>> caches = new ArrayList<>();
            AtomicInteger loaderCalls = new AtomicInteger();
            try {
                for (int i = 0; i < instances; i++) {
                    factories.add(factoryFor(uri));
                    caches.add(factories.get(i).getCache("degradation"));
                }
                // Warm both levels on all instances.
                for (int k = 0; k < 20; k++) {
                    String key = "warm-" + k;
                    caches.get(0).put(key, "v" + k);
                    for (TierCache<String, String> cache : caches) {
                        assertEquals("v" + k, cache.get(key));
                    }
                }
                int baselineLoaderCalls = loaderCalls.get();

                pause(server, true);
                Thread.sleep(300);
                pause(server, false);

                // Mass reconnect: all instances read all keys; L1 survived on
                // every instance, so the loader must stay idle.
                long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                for (int round = 0; round < 5 && System.nanoTime() < deadline; round++) {
                    for (TierCache<String, String> cache : caches) {
                        for (int k = 0; k < 20; k++) {
                            cache.getOrCompute("warm-" + k, key -> {
                                loaderCalls.incrementAndGet();
                                return "unexpected";
                            });
                        }
                    }
                }
                int afterRecovery = loaderCalls.get() - baselineLoaderCalls;
                assertTrue(afterRecovery <= Math.max(2, 2 * baselineLoaderCalls),
                        "loader spike after mass reconnect: " + afterRecovery + " calls");
            } finally {
                factories.forEach(TierCacheFactory::close);
            }
        }
    }

    private static void pause(org.testcontainers.containers.GenericContainer<?> server, boolean pause) {
        var dockerClient = org.testcontainers.DockerClientFactory.instance().client();
        String id = server.getContainerId();
        if (pause) {
            dockerClient.pauseContainerCmd(id).exec();
        } else {
            dockerClient.unpauseContainerCmd(id).exec();
        }
    }
}

class RedisDegradationChaosTest extends AbstractDegradationChaosTest {
    @Override
    DockerImageName image() {
        return DockerImageName.parse("redis:6.2-alpine");
    }
}

class ValkeyDegradationChaosTest extends AbstractDegradationChaosTest {
    @Override
    DockerImageName image() {
        return DockerImageName.parse("valkey/valkey:8.0-alpine");
    }
}
