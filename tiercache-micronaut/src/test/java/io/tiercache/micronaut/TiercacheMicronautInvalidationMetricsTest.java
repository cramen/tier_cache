package io.tiercache.micronaut;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.tiercache.TierCacheFactory;
import io.tiercache.spi.CacheMetricsListener;
import jakarta.inject.Singleton;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spec: observability — the Micronaut wiring must build the invalidation
 * engine with the application's metrics listener (custom bean, the
 * auto-created registry-backed one, or an explicit NOOP fallback) instead
 * of silently dropping invalidation metrics.
 */
class TiercacheMicronautInvalidationMetricsTest {

    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379);

    @BeforeAll
    static void startRedis() {
        REDIS.start();
    }

    @AfterAll
    static void stopRedis() {
        REDIS.stop();
    }

    /** Recording listener, active only under the dedicated env. */
    @Factory
    @Requires(env = "custom-listener-test")
    static class CustomListenerConfig {
        @Singleton
        RecordingListener recordingListener() {
            return new RecordingListener();
        }
    }

    /** Registry only (the auto-created listener shape), dedicated env. */
    @Factory
    @Requires(env = "registry-listener-test")
    static class RegistryConfig {
        @Singleton
        SimpleMeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    private static final class RecordingListener implements CacheMetricsListener {
        final Map<Direction, AtomicInteger> counts = new ConcurrentHashMap<>();

        @Override
        public void onInvalidation(String cache, Direction direction) {
            counts.computeIfAbsent(direction, d -> new AtomicInteger()).incrementAndGet();
        }

        final java.util.concurrent.atomic.AtomicLong acknowledged = new java.util.concurrent.atomic.AtomicLong();
        @Override public void onPublication(String cache, io.tiercache.spi.PublicationOutcome outcome, long count) {
            if (outcome == io.tiercache.spi.PublicationOutcome.ACKNOWLEDGED) acknowledged.addAndGet(count);
        }

        int count(Direction direction) {
            var counter = counts.get(direction);
            return counter == null ? 0 : counter.get();
        }
    }

    private Map<String, Object> config() {
        Map<String, Object> config = new HashMap<>();
        config.put("tiercache.enabled", "true");
        config.put("tiercache.redis-uri",
                "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        return config;
    }

    @Test
    void customListenerReceivesInvalidationMetrics() throws Exception {
        try (ApplicationContext context = ApplicationContext.run(config(), "custom-listener-test")) {
            RecordingListener listener = context.getBean(RecordingListener.class);
            context.getBean(TierCacheFactory.class).getCache("m").put("k", "v");
            assertThat(listener.count(CacheMetricsListener.Direction.SENT))
                    .as("the custom listener must count invalidation SENT (pre-fix: NOOP)")
                    .isGreaterThanOrEqualTo(1);
            awaitPublication(() -> listener.acknowledged.get() == 1);
        }
    }

    @Test
    void autoCreatedListenerCountsInvalidations() throws Exception {
        try (ApplicationContext context = ApplicationContext.run(config(), "registry-listener-test")) {
            SimpleMeterRegistry registry = context.getBean(SimpleMeterRegistry.class);
            context.getBean(TierCacheFactory.class).getCache("m").put("k", "v");
            var counter = registry.find("tiercache.invalidation").tags("direction", "sent").counter();
            assertThat(counter).as("invalidation SENT must reach the auto-created listener")
                    .isNotNull();
            assertThat(counter.count()).isGreaterThanOrEqualTo(1.0);
            awaitPublication(() -> {
                var acknowledged = registry.find("tiercache.invalidation.publish")
                        .tags("cache", "m", "outcome", "acknowledged").counter();
                return acknowledged != null && acknowledged.count() == 1;
            });
        }
    }

    @Test
    void wiringWorksWithoutAnyListener() {
        // No registry, no custom listener: the explicit NOOP fallback must
        // keep the wiring functional.
        try (ApplicationContext context = ApplicationContext.run(config())) {
            context.getBean(TierCacheFactory.class).getCache("m").put("k", "v");
            assertThat(context.containsBean(TierCacheFactory.class)).isTrue();
        }
    }
    private static void awaitPublication(java.util.function.BooleanSupplier condition) throws Exception {
        long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < until) Thread.sleep(5);
        org.junit.jupiter.api.Assertions.assertTrue(condition.getAsBoolean());
    }

}
