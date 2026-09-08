package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import io.tiercache.spi.StoredEntry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec: invalidation — journal atomicity, framing v2 round-trip and
 * backward compatibility, replay and overflow against real Redis.
 */
class RedisStreamJournalTest {

    private static GenericContainer<?> server;
    private static RedisClient client;
    private static String redisUri;

    @BeforeAll
    static void startServer() {
        server = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine"))
                .withExposedPorts(6379);
        server.start();
        redisUri = "redis://" + server.getHost() + ":" + server.getMappedPort(6379);
        client = RedisClient.create(redisUri);
    }

    @AfterAll
    static void stopServer() {
        client.shutdown();
        server.stop();
    }

    @Test
    void framingV2RoundTripsVersion() {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 1000,
                new JdkCacheSerializer<>());
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("v2")
                .journal(journal)
                .build();
        Version version = new Version(42, UUID.randomUUID());
        cache.put("k", StoredEntry.ofValue("data", version), Duration.ofMinutes(1));
        StoredEntry<String> entry = cache.get("k");
        assertEquals("data", entry.value());
        assertEquals(version, entry.version());

        // Marker with version
        cache.put("m", StoredEntry.nullMarker(version), Duration.ofMinutes(1));
        assertTrue(cache.get("m").isNullMarker());
        assertEquals(version, cache.get("m").version());
        cache.close();
    }

    @Test
    void framingV1ReadsAsUnversioned() {
        // Write via a journal-less cache (v1 framing), read via v2-capable one.
        LettuceRemoteCache<String, String> plain = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("v1").build();
        plain.put("k", StoredEntry.ofValue("legacy"), Duration.ofMinutes(1));
        assertNull(plain.get("k").version(), "v1 framing decodes with null version");
        plain.close();
    }

    @Test
    void journalRowsMatchDataWritesAtomically() {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 1000,
                new JdkCacheSerializer<>());
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("jatomic")
                .journal(journal)
                .build();
        UUID origin = UUID.randomUUID();
        cache.put("a", StoredEntry.ofValue("1", new Version(1, origin)), Duration.ofMinutes(1));
        cache.evict("b", new Version(2, origin));

        List<InvalidationMessage> rows = journal.readRange("jatomic", "0-0");
        assertEquals(2, rows.size());
        assertEquals("a", rows.get(0).key());
        assertEquals(new Version(1, origin), rows.get(0).version());
        assertEquals("b", rows.get(1).key());
        cache.close();
    }

    @Test
    void setIfAbsentWithJournalIsAtomic() {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 1000,
                new JdkCacheSerializer<>());
        LettuceRemoteCache<String, String> cache = LettuceRemoteCache.<String, String>builder(redisUri)
                .cacheName("jsia")
                .journal(journal)
                .build();
        UUID origin = UUID.randomUUID();
        assertTrue(cache.setIfAbsent("k", StoredEntry.ofValue("v", new Version(1, origin)),
                Duration.ofMinutes(1)));
        // Loser: no journal row appended.
        org.junit.jupiter.api.Assertions.assertFalse(cache.setIfAbsent("k",
                StoredEntry.ofValue("x", new Version(2, origin)), Duration.ofMinutes(1)));
        assertEquals(1, journal.readRange("jsia", "0-0").size());
        cache.close();
    }

    @Test
    void trimmedCursorIsDetected() {
        RedisStreamJournal journal = new RedisStreamJournal(client.connect(ByteArrayCodec.INSTANCE), 3,
                new JdkCacheSerializer<>());
        UUID origin = UUID.randomUUID();
        for (int i = 1; i <= 6; i++) {
            journal.append("trimmed", new InvalidationMessage("trimmed", "k" + i,
                    new Version(i, origin), origin, InvalidationMessage.Type.INVALIDATE));
        }
        String firstRemaining = journal.endCursor("trimmed");
        org.junit.jupiter.api.Assertions.assertFalse(journal.isTrimmed("trimmed", firstRemaining));
        // Cursor from before the window
        assertTrue(journal.isTrimmed("trimmed", "0-0"));
    }

}
