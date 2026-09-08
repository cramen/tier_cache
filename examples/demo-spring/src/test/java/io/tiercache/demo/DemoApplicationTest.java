package io.tiercache.demo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke test: the demo context starts against a real Redis and the
 * two-level cache serves the second call without recomputing.
 */
@SpringBootTest
@Testcontainers
class DemoApplicationTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProps(DynamicPropertyRegistry registry) {
        registry.add("tiercache.redis-uri",
                () -> "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
    }

    @Autowired
    DemoApplication.GreetingService service;

    @Test
    void secondCallIsServedFromCache() {
        long first = timed(() -> service.greeting("world"));
        long second = timed(() -> service.greeting("world"));
        assertThat(service.greeting("world")).isEqualTo("Hello, world!");
        assertThat(first).isGreaterThanOrEqualTo(400); // simulated expensive call
        assertThat(second).isLessThan(200); // served from cache
    }

    private static long timed(Runnable r) {
        long start = System.nanoTime();
        r.run();
        return (System.nanoTime() - start) / 1_000_000;
    }
}
