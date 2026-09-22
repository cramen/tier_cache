package io.tiercache.redis;

import org.testcontainers.utility.DockerImageName;

/** Pinned test inputs shared by all transport integration tests. */
final class ServerProfile {
    private ServerProfile() {}
    static DockerImageName image() {
        return DockerImageName.parse(System.getProperty("tiercache.test.serverImage", "redis:6.2.24-alpine"));
    }
}
