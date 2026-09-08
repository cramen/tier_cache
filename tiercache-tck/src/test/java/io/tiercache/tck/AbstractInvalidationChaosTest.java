package io.tiercache.tck;

import io.tiercache.InvalidationMode;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.CacheSettings;
import io.tiercache.InvalidationMessage;
import io.tiercache.NullPolicy;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.redis.JdkCacheSerializer;
import io.tiercache.redis.LettucePubSubInvalidationTransport;
import io.tiercache.redis.LettuceRemoteCache;
import io.tiercache.redis.RedisStreamJournal;
import io.tiercache.spi.InvalidationListener;
import io.tiercache.spi.InvalidationTransport;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Shared harness for invalidation chaos tests: two instances over one real
 * server, with a droppable transport wrapper simulating receiver-side
 * Pub/Sub loss.
 */
abstract class AbstractInvalidationChaosTest {

    static final String CACHE = "chaos";

    /** Transport wrapper: drops deliveries while "disconnected"; reconnect replays. */
    static final class DroppingTransport implements InvalidationTransport {
        private final LettucePubSubInvalidationTransport delegate;
        private final AtomicBoolean connected = new AtomicBoolean(true);
        private Runnable reconnectListener = () -> {
        };

        DroppingTransport(LettucePubSubInvalidationTransport delegate) {
            this.delegate = delegate;
        }

        @Override
        public void publish(InvalidationMessage message) {
            delegate.publish(message);
        }

        @Override
        public AutoCloseable subscribe(String cache, Consumer<InvalidationMessage> handler) {
            return delegate.subscribe(cache, message -> {
                if (connected.get()) {
                    handler.accept(message);
                }
            });
        }

        @Override
        public void setReconnectListener(Runnable listener) {
            this.reconnectListener = listener;
        }

        void disconnect() {
            connected.set(false);
        }

        void reconnect() {
            connected.set(true);
            reconnectListener.run();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    static final class Side {
        final RedisClient client;
        final LettuceRemoteCache<String, String> l2;
        final TierCacheFactory factory;
        final TierCache<String, String> cache;
        final DroppingTransport transport;
        final RedisStreamJournal journal;

        Side(RedisClient client, String uri, int journalCapacity) {
            this(client, uri, journalCapacity,
                    io.tiercache.spi.CacheMetricsListener.NOOP,
                    io.tiercache.spi.CacheMetricsListener.NOOP, null);
        }

        Side(RedisClient client, String uri, int journalCapacity,
                io.tiercache.spi.CacheMetricsListener metrics,
                io.tiercache.spi.CacheMetricsListener serviceMetrics) {
            this(client, uri, journalCapacity, metrics, serviceMetrics, null);
        }

        Side(RedisClient client, String uri, int journalCapacity,
                io.tiercache.spi.CacheMetricsListener metrics,
                io.tiercache.spi.CacheMetricsListener serviceMetrics,
                io.tiercache.CacheOverride cacheOverride) {
            this.client = client;
            this.journal = new RedisStreamJournal(
                    client.connect(ByteArrayCodec.INSTANCE), journalCapacity, new JdkCacheSerializer<>());
            var l2Builder = LettuceRemoteCache.<String, String>builder(uri)
                    .client(client)
                    .cacheName(CACHE)
                    .journal(journal);
            if (cacheOverride != null) {
                var resolved = cacheOverride.resolve(CacheSettings.defaults());
                l2Builder.invalidationMode(resolved.invalidationMode(), resolved.payloadCapBytes());
            }
            this.l2 = l2Builder.build();
            this.transport = new DroppingTransport(
                    new LettucePubSubInvalidationTransport(client, new JdkCacheSerializer<>()));
            TierCacheFactory.Builder factoryBuilder = TierCacheFactory.builder()
                    .defaults(new CacheSettings(10_000, Duration.ofMinutes(5), null,
                            Duration.ofHours(1), 0.0, NullPolicy.deny(), InvalidationMode.INVALIDATE, 64 * 1024))
                    .remoteCache(l2)
                    .metricsListener(metrics);
            if (cacheOverride != null) {
                factoryBuilder.cache(CACHE, cacheOverride);
            }
            this.factory = factoryBuilder
                    .invalidation(versions -> new InvalidationService(transport, journal,
                            versions.instanceId(), InvalidationListener.NOOP, serviceMetrics))
                    .build();
            this.cache = factory.getCache(CACHE);
        }

        void close() {
            factory.close();
            l2.close();
            client.shutdown();
        }
    }

    static GenericContainer<?> startServer(DockerImageName image) {
        GenericContainer<?> server = new GenericContainer<>(image).withExposedPorts(6379);
        server.start();
        return server;
    }

    static String uri(GenericContainer<?> server) {
        return "redis://" + server.getHost() + ":" + server.getMappedPort(6379);
    }

    /** Current L2 truth for a key (bypasses all L1s). */
    static String l2Truth(Side side, String key) {
        var entry = side.l2.get(key);
        return entry == null || entry.isNullMarker() ? null : entry.value();
    }

    static void waitFor(Check check) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!check.ok()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 10s");
            }
            Thread.sleep(20);
        }
    }

    interface Check {
        boolean ok();
    }
}
