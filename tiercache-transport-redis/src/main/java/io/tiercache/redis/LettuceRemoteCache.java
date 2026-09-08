package io.tiercache.redis;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.tiercache.spi.RemoteCache;
import io.tiercache.spi.StoredEntry;

import java.time.Duration;
import java.util.Objects;

/**
 * {@link RemoteCache} over Redis/Valkey via Lettuce (default L2 transport,
 * N-08). Supports per-entry TTLs (F-06, {@code SET ... PX}) and atomic
 * {@code setIfAbsent} ({@code SET ... PX NX}).
 *
 * <p>Keys are namespaced as {@code <cacheName>:<serialized-key>} so multiple
 * named caches can share one server.
 *
 * <p>Default timeouts (F-33): 100 ms connect, 250 ms per command — both
 * configurable via the builder. Infrastructure failures surface as Lettuce
 * unchecked exceptions; degradation handling (circuit breaker, F-30) lives
 * in core, not here.
 *
 * <p><b>Incubating:</b> 0.x API, may change until CP-0.
 */
public final class LettuceRemoteCache<K, V> implements RemoteCache<K, V>, AutoCloseable {

    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofMillis(100);
    public static final Duration DEFAULT_COMMAND_TIMEOUT = Duration.ofMillis(250);

    private final RedisClient client;
    private final boolean ownsClient;
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final RedisCommands<byte[], byte[]> commands;
    private final byte[] keyPrefix;
    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;

    // Payload framing: every stored value is prefixed with a tag byte, so a
    // null-marker (F-25) can never collide with serializer output.
    private static final byte TAG_NULL_MARKER = 0x00;
    private static final byte TAG_VALUE = 0x01;

    private LettuceRemoteCache(Builder<K, V> builder) {
        this.ownsClient = builder.sharedClient == null;
        this.client = ownsClient ? RedisClient.create(builder.redisUri) : builder.sharedClient;
        if (ownsClient) {
            // For a caller-provided client, options are the caller's responsibility.
            this.client.setOptions(ClientOptions.builder()
                    .socketOptions(SocketOptions.builder()
                            .connectTimeout(builder.connectTimeout)
                            .build())
                    .timeoutOptions(TimeoutOptions.enabled(builder.commandTimeout))
                    .build());
        }
        this.connection = client.connect(ByteArrayCodec.INSTANCE);
        this.commands = connection.sync();
        this.keyPrefix = (builder.cacheName + ":").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        this.keySerializer = builder.keySerializer;
        this.valueSerializer = builder.valueSerializer;
    }

    public static <K, V> Builder<K, V> builder(String redisUri) {
        return new Builder<>(redisUri);
    }

    @Override
    public StoredEntry<V> get(K key) {
        byte[] bytes = commands.get(namespaced(key));
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        if (bytes[0] == TAG_NULL_MARKER) {
            return StoredEntry.nullMarker();
        }
        return StoredEntry.ofValue(valueSerializer.fromBytes(
                java.util.Arrays.copyOfRange(bytes, 1, bytes.length)));
    }

    @Override
    public void put(K key, StoredEntry<V> entry, Duration ttl) {
        commands.set(namespaced(key), encode(entry), SetArgs.Builder.px(ttl));
    }

    @Override
    public void evict(K key) {
        commands.del(namespaced(key));
    }

    @Override
    public boolean setIfAbsent(K key, V value, Duration ttl) {
        String result = commands.set(namespaced(key), encode(StoredEntry.ofValue(value)),
                SetArgs.Builder.px(ttl).nx());
        return "OK".equals(result);
    }

    private byte[] encode(StoredEntry<V> entry) {
        if (entry.isNullMarker()) {
            return new byte[]{TAG_NULL_MARKER};
        }
        byte[] payload = valueSerializer.toBytes(entry.value());
        byte[] out = new byte[payload.length + 1];
        out[0] = TAG_VALUE;
        System.arraycopy(payload, 0, out, 1, payload.length);
        return out;
    }

    private byte[] namespaced(K key) {
        byte[] serialized = keySerializer.toBytes(key);
        byte[] out = new byte[keyPrefix.length + serialized.length];
        System.arraycopy(keyPrefix, 0, out, 0, keyPrefix.length);
        System.arraycopy(serialized, 0, out, keyPrefix.length, serialized.length);
        return out;
    }

    @Override
    public void close() {
        connection.close();
        if (ownsClient) {
            client.shutdown();
        }
    }

    /**
     * @param <K> key type
     * @param <V> value type
     */
    public static final class Builder<K, V> {

        private final String redisUri;
        private RedisClient sharedClient;
        private String cacheName = "default";
        private CacheSerializer<K> keySerializer;
        private CacheSerializer<V> valueSerializer;
        private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        private Duration commandTimeout = DEFAULT_COMMAND_TIMEOUT;

        private Builder(String redisUri) {
            this.redisUri = Objects.requireNonNull(redisUri, "redisUri");
        }

        /**
         * Reuses an existing client (e.g. shared across caches/instances)
         * instead of creating one from {@code redisUri}. The caller then owns
         * the client lifecycle.
         */
        @SuppressWarnings("unchecked")
        public Builder<K, V> client(RedisClient sharedClient) {
            this.sharedClient = Objects.requireNonNull(sharedClient, "sharedClient");
            return this;
        }

        /**
         * Namespace for keys (used as {@code <cacheName>:} prefix). Required
         * when several caches share one server.
         */
        public Builder<K, V> cacheName(String cacheName) {
            this.cacheName = Objects.requireNonNull(cacheName, "cacheName");
            return this;
        }

        @SuppressWarnings("unchecked")
        public Builder<K, V> keySerializer(CacheSerializer<?> keySerializer) {
            this.keySerializer = (CacheSerializer<K>) Objects.requireNonNull(keySerializer);
            return this;
        }

        @SuppressWarnings("unchecked")
        public Builder<K, V> valueSerializer(CacheSerializer<?> valueSerializer) {
            this.valueSerializer = (CacheSerializer<V>) Objects.requireNonNull(valueSerializer);
            return this;
        }

        public Builder<K, V> connectTimeout(Duration connectTimeout) {
            this.connectTimeout = Objects.requireNonNull(connectTimeout, "connectTimeout");
            return this;
        }

        public Builder<K, V> commandTimeout(Duration commandTimeout) {
            this.commandTimeout = Objects.requireNonNull(commandTimeout, "commandTimeout");
            return this;
        }

        public LettuceRemoteCache<K, V> build() {
            if (keySerializer == null) {
                keySerializer = new JdkCacheSerializer<>();
            }
            if (valueSerializer == null) {
                valueSerializer = new JdkCacheSerializer<>();
            }
            return new LettuceRemoteCache<>(this);
        }
    }
}
