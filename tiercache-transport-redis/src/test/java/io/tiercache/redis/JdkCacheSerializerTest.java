package io.tiercache.redis;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Spec: redis-l2-transport — serializer round trip (no container needed). */
class JdkCacheSerializerTest {

    @Test
    void roundTrip() {
        CacheSerializer<String> serializer = new JdkCacheSerializer<>();
        assertEquals("hello", serializer.fromBytes(serializer.toBytes("hello")));
    }
}
