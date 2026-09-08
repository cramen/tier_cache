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
}
