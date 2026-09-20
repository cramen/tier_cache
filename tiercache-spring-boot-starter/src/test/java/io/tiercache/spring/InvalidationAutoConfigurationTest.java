package io.tiercache.spring;

import io.tiercache.TierCacheFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spec: invalidation — starter wiring of the Pub/Sub profile (real Redis).
 */
class InvalidationAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TiercacheAutoConfiguration.class));

    @Test
    void invalidationIsWiredByDefaultWithRedisTransport() {
        try (GenericContainer<?> redis = new GenericContainer<>(
                DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start();
            String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
            runner.withPropertyValues("tiercache.enabled=true", "tiercache.redis-uri=" + uri)
                    .run(context -> {
                        assertThat(context).hasNotFailed();
                        assertThat(context).hasSingleBean(TierCacheManager.class);
                        assertThat(context).hasBean("tiercacheInvalidationHandlerFactory");
                        assertThat(context).hasBean("tiercacheInvalidationJournal");
                        // And the whole chain works: a put publishes without error.
                        context.getBean(TierCacheFactory.class).getCache("wiring").put("k", "v");
                    });
        }
    }

    @Test
    void streamsProfileIsSelectable() {
        try (GenericContainer<?> redis = new GenericContainer<>(
                DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start();
            String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
            runner.withPropertyValues("tiercache.enabled=true", "tiercache.redis-uri=" + uri,
                            "tiercache.invalidation.profile=streams")
                    .run(context -> {
                        assertThat(context).hasNotFailed();
                        context.getBean(TierCacheFactory.class).getCache("s").put("k", "v");
                    });
        }
    }

    @Test
    void invalidationOptOutSkipsBeans() {
        try (GenericContainer<?> redis = new GenericContainer<>(
                DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start();
            String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
            runner.withPropertyValues("tiercache.enabled=true", "tiercache.redis-uri=" + uri,
                            "tiercache.invalidation.enabled=false")
                    .run(context -> {
                        assertThat(context).hasNotFailed();
                        assertThat(context).hasSingleBean(TierCacheManager.class);
                        assertThat(context).doesNotHaveBean("tiercacheInvalidationHandlerFactory");
                        assertThat(context.getBeanNamesForType(
                                io.tiercache.redis.RedisStreamJournal.class)).isEmpty();
                    });
        }
    }

    /** Recording listener for the wiring assertions. */
    private static final class RecordingListener implements io.tiercache.spi.CacheMetricsListener {
        final java.util.Map<Direction, java.util.concurrent.atomic.AtomicInteger> counts =
                new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public void onInvalidation(String cache, Direction direction) {
            counts.computeIfAbsent(direction, d -> new java.util.concurrent.atomic.AtomicInteger())
                    .incrementAndGet();
        }

        int count(Direction direction) {
            var counter = counts.get(direction);
            return counter == null ? 0 : counter.get();
        }
    }

    private static void awaitTrue(java.util.function.BooleanSupplier check, String description)
            throws InterruptedException {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(10).toNanos();
        while (!check.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 10s: " + description);
            }
            Thread.sleep(20);
        }
    }

    /**
     * The starter must build the invalidation engine with the CONTEXT's
     * metrics listener, not the no-op fallback: a custom listener bean sees
     * SENT locally and RECEIVED on a second instance (pre-fix: NOOP).
     */
    @Test
    void invalidationEventsReachTheCustomMetricsListener() throws Exception {
        try (GenericContainer<?> redis = new GenericContainer<>(
                DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start();
            String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
            RecordingListener listenerA = new RecordingListener();
            RecordingListener listenerB = new RecordingListener();
            var runnerA = new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(TiercacheAutoConfiguration.class))
                    .withBean(io.tiercache.spi.CacheMetricsListener.class, () -> listenerA);
            var runnerB = new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(TiercacheAutoConfiguration.class))
                    .withBean(io.tiercache.spi.CacheMetricsListener.class, () -> listenerB);

            runnerB.withPropertyValues("tiercache.enabled=true", "tiercache.redis-uri=" + uri)
                    .run(contextB -> {
                        assertThat(contextB).hasNotFailed();
                        contextB.getBean(TierCacheFactory.class).getCache("m"); // subscribe
                        runnerA.withPropertyValues("tiercache.enabled=true",
                                        "tiercache.redis-uri=" + uri)
                                .run(contextA -> {
                                    assertThat(contextA).hasNotFailed();
                                    contextA.getBean(TierCacheFactory.class).getCache("m")
                                            .put("k", "v");
                                    awaitTrue(() -> listenerA.count(
                                                    io.tiercache.spi.CacheMetricsListener.Direction.SENT) >= 1,
                                            "A's listener must count SENT");
                                    awaitTrue(() -> listenerB.count(
                                                    io.tiercache.spi.CacheMetricsListener.Direction.RECEIVED) >= 1,
                                            "B's listener must count RECEIVED");
                                });
                    });
        }
    }

    /**
     * The auto-configured (Micrometer) listener is wired the same way when
     * no custom bean takes precedence (pre-fix: the metric never appeared).
     */
    @Test
    void autoConfiguredMicrometerListenerCountsInvalidations() {
        try (GenericContainer<?> redis = new GenericContainer<>(
                DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start();
            String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
            io.micrometer.core.instrument.simple.SimpleMeterRegistry registry =
                    new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(TiercacheAutoConfiguration.class,
                            TiercacheMetricsAutoConfiguration.class))
                    .withBean(io.micrometer.core.instrument.MeterRegistry.class, () -> registry)
                    .withPropertyValues("tiercache.enabled=true", "tiercache.redis-uri=" + uri)
                    .run(context -> {
                        assertThat(context).hasNotFailed();
                        context.getBean(TierCacheFactory.class).getCache("m").put("k", "v");
                        var counter = registry.find("tiercache.invalidation")
                                .tags("direction", "sent").counter();
                        assertThat(counter).as("invalidation SENT must reach the auto-configured "
                                + "Micrometer listener").isNotNull();
                        assertThat(counter.count()).isGreaterThanOrEqualTo(1.0);
                    });
        }
    }

    /**
     * No listener bean and no metrics module on the classpath: the wiring
     * falls back to NOOP explicitly and keeps working.
     */
    @Test
    void wiringWorksWithoutListenerOrMetricsModule() {
        try (GenericContainer<?> redis = new GenericContainer<>(
                DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start();
            String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
            runner.withClassLoader(new org.springframework.boot.test.context.FilteredClassLoader(
                            "io.micrometer"))
                    .withPropertyValues("tiercache.enabled=true", "tiercache.redis-uri=" + uri)
                    .run(context -> {
                        assertThat(context).hasNotFailed();
                        context.getBean(TierCacheFactory.class).getCache("m").put("k", "v");
                    });
        }
    }
}
