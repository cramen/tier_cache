package io.tiercache.spring;

import io.lettuce.core.RedisClient;
import io.tiercache.redis.LettuceLockProvider;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Rebuild-lock provider ownership (load-test review): the provider is a
 * managed bean whose compensation scheduler is shut down by the context's
 * destroy callback — verified through a REAL context, not a direct close.
 */
class LockProviderShutdownTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TiercacheAutoConfiguration.class));

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
            String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
            runner.withPropertyValues("tiercache.enabled=true", "tiercache.redis-uri=" + uri)
                    .run(context -> {
                        assertThat(context).hasNotFailed();
                        assertThat(context).hasSingleBean(LettuceLockProvider.class);
                        LettuceLockProvider provider = context.getBean(LettuceLockProvider.class);

                        // Force an ambiguous acquire so the scheduler spins up.
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
                            redis.getDockerClient()
                                    .unpauseContainerCmd(redis.getContainerId()).exec();
                        }
                    });
            // After the context is closed, the scheduler must be gone.
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
