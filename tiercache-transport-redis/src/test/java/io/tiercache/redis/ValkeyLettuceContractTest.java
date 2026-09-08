package io.tiercache.redis;

import org.testcontainers.utility.DockerImageName;

/** Contract suite against Valkey (N-08). */
class ValkeyLettuceContractTest extends AbstractLettuceContractTest {

    @Override
    DockerImageName image() {
        return DockerImageName.parse("valkey/valkey:8.0-alpine");
    }
}
