package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.tiercache.testkit.RemoteCacheContractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

/**
 * Runs the shared {@link RemoteCacheContractTest} suite against a real
 * server in a container (N-08). Subclasses pick the image.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractLettuceContractTest extends RemoteCacheContractTest {

    private static final int REDIS_PORT = 6379;

    private GenericContainer<?> server;
    private RedisClient client;

    abstract DockerImageName image();

    @BeforeAll
    void startServer() {
        server = new GenericContainer<>(image()).withExposedPorts(REDIS_PORT);
        server.start();
        client = RedisClient.create(
                "redis://" + server.getHost() + ":" + server.getMappedPort(REDIS_PORT));
    }

    @AfterAll
    void stopServer() {
        if (client != null) {
            client.shutdown();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Override
    protected LettuceRemoteCache<String, String> newCache() {
        // Unique namespace per contract invocation keeps tests isolated.
        return LettuceRemoteCache.<String, String>builder("redis://unused")
                .client(client)
                .cacheName("contract-" + UUID.randomUUID())
                .build();
    }

    String redisUri() {
        return "redis://" + server.getHost() + ":" + server.getMappedPort(REDIS_PORT);
    }

    RedisClient sharedClient() {
        return client;
    }
}
