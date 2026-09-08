package io.tiercache.redis;

import org.testcontainers.utility.DockerImageName;

/** Contract suite against Redis 6.2 (the supported baseline). */
class RedisLettuceContractTest extends AbstractLettuceContractTest {

    @Override
    DockerImageName image() {
        return DockerImageName.parse("redis:6.2-alpine");
    }
}
