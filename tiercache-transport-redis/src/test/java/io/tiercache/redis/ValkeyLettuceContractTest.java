package io.tiercache.redis;

import org.testcontainers.utility.DockerImageName;

/** Contract suite against Valkey. */
class ValkeyLettuceContractTest extends AbstractNamespaceContractTest {

    @Override
    DockerImageName image() {
        return DockerImageName.parse(System.getProperty("tiercache.test.valkeyImage", "valkey/valkey:9.1.2-alpine"));
    }
}
