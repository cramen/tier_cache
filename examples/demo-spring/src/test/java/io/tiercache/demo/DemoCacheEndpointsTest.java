package io.tiercache.demo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the cache endpoints (write, read, repeated read, evict) over
 * HTTP on the JVM path — the same surface the native smoke suite curls.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class DemoCacheEndpointsTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProps(DynamicPropertyRegistry registry) {
        registry.add("tiercache.redis-uri",
                () -> "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
    }

    @Autowired
    TestRestTemplate rest;

    @Test
    void writeReadRepeatedReadEvict() {
        String url = "/cache/smoke-key";

        assertThat(rest.getForEntity(url, String.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);

        HttpHeaders plainText = new HttpHeaders();
        plainText.setContentType(MediaType.TEXT_PLAIN);
        ResponseEntity<Void> written = rest.exchange(url, HttpMethod.PUT,
                new HttpEntity<>("smoke-value", plainText), Void.class);
        assertThat(written.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        ResponseEntity<String> first = rest.getForEntity(url, String.class);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getBody()).isEqualTo("smoke-value");

        ResponseEntity<String> repeated = rest.getForEntity(url, String.class);
        assertThat(repeated.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(repeated.getBody()).isEqualTo("smoke-value");

        ResponseEntity<Void> evicted = rest.exchange(url, HttpMethod.DELETE,
                HttpEntity.EMPTY, Void.class);
        assertThat(evicted.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        assertThat(rest.getForEntity(url, String.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }
}
