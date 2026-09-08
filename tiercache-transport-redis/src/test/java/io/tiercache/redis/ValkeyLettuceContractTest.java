package io.tiercache.redis;

import org.testcontainers.utility.DockerImageName;

/** Contract suite against Valkey. */
class ValkeyLettuceContractTest extends AbstractLettuceContractTest {

    @Override
    DockerImageName image() {
        return DockerImageName.parse("valkey/valkey:8.0-alpine");
    }
}
