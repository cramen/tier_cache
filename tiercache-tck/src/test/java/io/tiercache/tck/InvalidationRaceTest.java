package io.tiercache.tck;

import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Invalidation/write race: two instances race puts and evicts with
 * randomized delays. Versioned writes + last-write-wins application must
 * converge every instance's L1 to the L2 content — zero resurrected or
 * stale values after quiescence.
 */
abstract class AbstractInvalidationRaceTest extends AbstractInvalidationChaosTest {

    abstract DockerImageName image();

    @Test
    void raceConvergesToL2Truth() throws Exception {
        try (var server = startServer(image())) {
            String uri = uri(server);
            Side a = new Side(io.lettuce.core.RedisClient.create(uri), uri, 1000);
            Side b = new Side(io.lettuce.core.RedisClient.create(uri), uri, 1000);
            try {
                Random random = new Random(42);
                String[] keys = new String[8];
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = "race-" + i;
                }

                ExecutorService pool = Executors.newFixedThreadPool(4);
                List<Future<?>> futures = new ArrayList<>();
                for (int round = 0; round < 50; round++) {
                    String key = keys[random.nextInt(keys.length)];
                    String value = "round-" + round;
                    Side first = random.nextBoolean() ? a : b;
                    Side second = first == a ? b : a;
                    boolean evict = random.nextInt(4) == 0; // 25% evicts
                    futures.add(pool.submit(() -> {
                        sleepQuietly(random.nextInt(4));
                        if (evict) {
                            first.cache.evict(key);
                        } else {
                            first.cache.put(key, value + "-x");
                        }
                    }));
                    futures.add(pool.submit(() -> {
                        sleepQuietly(random.nextInt(4));
                        second.cache.put(key, value + "-y");
                    }));
                }
                for (Future<?> f : futures) {
                    f.get();
                }
                pool.shutdown();

                // Let in-flight invalidation events land.
                Thread.sleep(1000);

                for (String key : keys) {
                    String truth = l2Truth(a, key);
                    assertEquals(truth, a.cache.get(key),
                            "instance A diverged from L2 for " + key);
                    assertEquals(truth, b.cache.get(key),
                            "instance B diverged from L2 for " + key);
                }
            } finally {
                a.close();
                b.close();
            }
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}

class RedisInvalidationRaceTest extends AbstractInvalidationRaceTest {
    @Override
    DockerImageName image() {
        return DockerImageName.parse("redis:6.2-alpine");
    }
}

class ValkeyInvalidationRaceTest extends AbstractInvalidationRaceTest {
    @Override
    DockerImageName image() {
        return DockerImageName.parse("valkey/valkey:8.0-alpine");
    }
}
