package io.tiercache.redis;

import org.testcontainers.utility.DockerImageName;

/** Contract suite against Redis 6.2 (the supported baseline). */
class RedisLettuceContractTest extends AbstractNamespaceContractTest {

    @Override
    DockerImageName image() {
        return ServerProfile.image();
    }
}
