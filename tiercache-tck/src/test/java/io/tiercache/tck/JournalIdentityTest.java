package io.tiercache.tck;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.CacheSettings;
import io.tiercache.InvalidationMode;
import io.tiercache.NullPolicy;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.redis.JdkCacheSerializer;
import io.tiercache.redis.LettucePubSubInvalidationTransport;
import io.tiercache.redis.LettuceRemoteCache;
import io.tiercache.redis.RedisStreamJournal;
import io.tiercache.spi.InvalidationListener;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Journal identity regression for framework-namespaced L2 layouts. Data keys
 * are namespaced (e.g. {@code spring:users}) while the invalidation journal
 * must stay under the logical cache name: before the fix, the write path
 * appended journal rows to the namespaced stream and the recovery path
 * replayed the logical-name stream (never written), so a Pub/Sub gap left
 * stale L1 entries until TTL.
 */
class JournalIdentityTest extends AbstractInvalidationChaosTest {

    /** One instance wired the way the Spring/Micronaut integrations wire it. */
    private static final class FrameworkSide implements AutoCloseable {
        final RedisClient client;
        final LettuceRemoteCache<String, String> l2;
        final TierCacheFactory factory;
        final TierCache<String, String> cache;
        final DroppingTransport transport;

        FrameworkSide(String uri, String dataKeyPrefix) {
            this.client = RedisClient.create(uri);
            RedisStreamJournal journal = new RedisStreamJournal(
                    client.connect(ByteArrayCodec.INSTANCE), 1000, new JdkCacheSerializer<>());
            this.l2 = LettuceRemoteCache.<String, String>builder(uri)
                    .client(client)
                    .cacheName(dataKeyPrefix + CACHE)   // data keys namespaced, like the frameworks do
                    .journalName(CACHE)                 // journal stays under the logical cache name
                    .journal(journal)
                    .build();
            this.transport = new DroppingTransport(
                    new LettucePubSubInvalidationTransport(client, new JdkCacheSerializer<>()));
            this.factory = TierCacheFactory.builder()
                    .defaults(new CacheSettings(10_000, Duration.ofMinutes(5), null,
                            Duration.ofHours(1), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
                    .remoteCache(l2)
                    .invalidation(versions -> new InvalidationService(transport, journal,
                            versions.instanceId(), InvalidationListener.NOOP))
                    .build();
            this.cache = factory.getCache(CACHE);
        }

        @Override
        public void close() {
            factory.close();
            l2.close();
            client.shutdown();
        }
    }

    @Test
    void namespacedL2ReplaysLogicalJournal() throws Exception {
        try (var server = startServer(DockerImageName.parse("redis:6.2-alpine"))) {
            String uri = uri(server);
            FrameworkSide a = new FrameworkSide(uri, "spring:");
            FrameworkSide b = new FrameworkSide(uri, "spring:");
            try {
                a.cache.put("k", "old");
                assertEquals("old", b.cache.get("k")); // warm B's L1

                // The framework layout is really exercised: data keys are
                // namespaced, the journal stream is keyed by the logical name.
                try (RedisClient probe = RedisClient.create(uri);
                        var conn = probe.connect()) {
                    assertFalse(conn.sync().keys("spring:" + CACHE + ":*").isEmpty(),
                            "data keys live under the framework namespace");
                    assertFalse(conn.sync().keys(RedisStreamJournal.JOURNAL_KEYSPACE + CACHE).isEmpty(),
                            "journal stream is keyed by the logical cache name");
                }

                b.transport.disconnect();
                a.cache.put("k", "new");
                Thread.sleep(300); // the event is lost for B
                assertEquals("old", b.cache.get("k"), "sanity: B is stale while disconnected");

                // Replay must heal B from the logical-name journal, not wait for TTL.
                b.transport.reconnect();
                waitFor(() -> "new".equals(b.cache.get("k")));
            } finally {
                a.close();
                b.close();
            }
        }
    }

    /** Journal name defaults to the cache name: prefix-free programmatic wiring is unchanged. */
    @Test
    void journalNameDefaultsToCacheName() throws Exception {
        try (var server = startServer(DockerImageName.parse("redis:6.2-alpine"))) {
            String uri = uri(server);
            Side a = new Side(RedisClient.create(uri), uri, 1000);
            Side b = new Side(RedisClient.create(uri), uri, 1000);
            try {
                a.cache.put("k", "old");
                assertEquals("old", b.cache.get("k"));

                b.transport.disconnect();
                a.cache.put("k", "new");
                Thread.sleep(300);

                b.transport.reconnect();
                waitFor(() -> "new".equals(b.cache.get("k")));
            } finally {
                a.close();
                b.close();
            }
        }
    }
}
