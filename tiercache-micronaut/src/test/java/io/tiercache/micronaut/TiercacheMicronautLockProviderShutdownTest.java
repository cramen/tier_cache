package io.tiercache.micronaut;

import io.micronaut.context.ApplicationContext;
import io.tiercache.redis.LettuceLockProvider;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Rebuild-lock provider ownership (load-test review): the provider is a
 * managed bean whose compensation scheduler is shut down with the context
 * (preDestroy) — verified through a REAL context, not a direct close.
 */
class TiercacheMicronautLockProviderShutdownTest {

    private static long compensationThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith("tiercache-lock-compensation"))
                .count();
    }

    @Test
    void contextCloseTerminatesTheCompensationScheduler() throws Exception {
        try (GenericContainer<?> redis = new GenericContainer<>(
                DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start();
            Map<String, Object> config = new HashMap<>();
            config.put("tiercache.enabled", "true");
            config.put("tiercache.redis-uri",
                    "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));

            ApplicationContext context = ApplicationContext.run(config);
            assertThat(context.containsBean(LettuceLockProvider.class)).isTrue();
            LettuceLockProvider provider = context.getBean(LettuceLockProvider.class);

            provider.tryLock("preflight", Duration.ofSeconds(5)).release();
            var field = LettuceLockProvider.class.getDeclaredField("ownedConnection");
            field.setAccessible(true);
            var owned = (io.lettuce.core.api.StatefulRedisConnection<?, ?>) field.get(provider);
            redis.getDockerClient().pauseContainerCmd(redis.getContainerId()).exec();
            try {
                org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                        () -> provider.tryLock("shutdown", Duration.ofSeconds(30)));
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                while (compensationThreads() == 0 && System.nanoTime() < deadline) {
                    Thread.sleep(20);
                }
                assertThat(compensationThreads())
                        .as("the pending compensation spins up the scheduler")
                        .isGreaterThan(0);
            } finally {
                redis.getDockerClient().unpauseContainerCmd(redis.getContainerId()).exec();
            }

            context.close();
            assertThat(owned.isOpen()).isFalse();
            provider.close();
            org.junit.jupiter.api.Assertions.assertThrows(io.tiercache.internal.LockProviderClosedException.class,
                    () -> provider.tryLock("closed", Duration.ofSeconds(5)));
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (compensationThreads() > 0 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertThat(compensationThreads())
                    .as("context close must terminate the compensation scheduler")
                    .isZero();
        }
    }
}
