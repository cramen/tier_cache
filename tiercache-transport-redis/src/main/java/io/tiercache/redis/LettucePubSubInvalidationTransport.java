package io.tiercache.redis;

import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.tiercache.InvalidationMessage;
import io.tiercache.invalidation.MessageCodec;
import io.tiercache.spi.InvalidationTransport;
import io.tiercache.spi.InvalidationGapHandler;
import io.tiercache.spi.InvalidationDeliveryFence;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Default invalidation transport profile: Redis Pub/Sub (minimal latency).
 * One Pub/Sub connection per instance; per-cache channels
 * ({@code tiercache:v2:inv:<token(cache)>}); inbound events are dispatched on a
 * bounded shared worker pool, off the I/O thread, preserving per-channel order.
 *
 * <p>On reconnect (detected via the Lettuce event bus) the registered
 * reconnect listener fires so the engine can replay the journal — Pub/Sub
 * itself is not durable.
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @since 0.1.0
 */
public final class LettucePubSubInvalidationTransport implements InvalidationTransport {

    /**
     * Channel keyspace prefix.
     *
     * @since 0.1.0
     */
    public static final String CHANNEL_PREFIX = RedisKeyspace.CHANNEL;

    private final RedisClient client;
    private final CacheSerializer<Object> keySerializer;
    private final CacheSerializer<Object> valueSerializer;
    private final long payloadCapBytes;
    private final StatefulRedisPubSubConnection<byte[], byte[]> connection;
    private record Registration(String cache, AutoCloseable lane) { }
    private final Map<String, Registration> channels = new ConcurrentHashMap<>();
    private final ReentrantLock subscriptions = new ReentrantLock();
    private final PubSubDispatcher dispatcher;

    private volatile Runnable reconnectListener = () -> {
    };

    /**
     * Creates a transport with one serializer for both keys and UPDATE
     * payloads and the default payload cap (64 KiB).
     *
     * @param client        the Redis client to connect through
     * @param keySerializer serializer for message keys (also used for UPDATE
     *                      payloads)
     * @since 0.1.0
     */
    public LettucePubSubInvalidationTransport(RedisClient client,
            CacheSerializer<Object> keySerializer) {
        this(client, keySerializer, keySerializer, 64 * 1024);
    }

    /**
     * Creates a transport with distinct key/value serializers and an explicit
     * UPDATE payload cap. UPDATE messages whose serialized payload exceeds
     * {@code payloadCapBytes} degrade to plain INVALIDATE messages.
     *
     * @param client          the Redis client to connect through
     * @param keySerializer   serializer for message keys
     * @param valueSerializer serializer for UPDATE payloads
     * @param payloadCapBytes maximum serialized UPDATE payload size in bytes
     * @since 0.1.0
     */
    public LettucePubSubInvalidationTransport(RedisClient client,
            CacheSerializer<Object> keySerializer, CacheSerializer<Object> valueSerializer,
            long payloadCapBytes) {
        this(client, keySerializer, valueSerializer, payloadCapBytes, PubSubDispatchOptions.DEFAULT);
    }

    /** Creates a receiver with explicit aggregate dispatch bounds. */
    public LettucePubSubInvalidationTransport(RedisClient client,
            CacheSerializer<Object> keySerializer, CacheSerializer<Object> valueSerializer,
            long payloadCapBytes, PubSubDispatchOptions options) {
        java.util.Objects.requireNonNull(options, "options");
        this.client = client;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.payloadCapBytes = payloadCapBytes;
        this.connection = client.connectPubSub(ByteArrayCodec.INSTANCE);
        this.dispatcher = new PubSubDispatcher(options);
        connection.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(byte[] channel, byte[] message) {
                Registration registration = channels.get(channelKey(channel));
                if (registration == null) {
                    dispatcher.gap(null);
                    return;
                }
                dispatcher.accept(registration.cache(), message);
            }
        });
        // On reconnect the engine replays the journal (Pub/Sub is not durable).
        connection.addListener(new RedisConnectionStateListener() {
            @Override
            public void onRedisConnected(RedisChannelHandler<?, ?> connection,
                    java.net.SocketAddress socketAddress) {
                reconnectListener.run();
            }
        });
    }

    @Override
    public void publish(InvalidationMessage message) { publishAsync(message); }

    @Override
    public java.util.concurrent.CompletionStage<io.tiercache.spi.PublicationOutcome> publishAsync(InvalidationMessage message) {
        try {
            byte[] keyBytes = message.key() != null ? keySerializer.toBytes(message.key()) : null;
            InvalidationMessage toSend = message;
            if (message.type() == InvalidationMessage.Type.UPDATE) {
                byte[] payloadBytes = valueSerializer.toBytes(message.payload());
                if (payloadBytes.length > payloadCapBytes) {
                    // Oversized payload: degrade to plain INVALIDATE.
                    toSend = new InvalidationMessage(message.cache(), message.key(),
                            message.version(), message.originInstanceId(),
                            InvalidationMessage.Type.INVALIDATE);
                } else {
                    toSend = new InvalidationMessage(message.cache(), message.key(),
                            message.version(), message.originInstanceId(),
                            message.type(), payloadBytes);
                }
            }
            return connection.async().publish(channelName(message.cache()),
                    MessageCodec.encode(toSend, keyBytes)).thenApply(ignored -> io.tiercache.spi.PublicationOutcome.ACKNOWLEDGED);
        } catch (RuntimeException e) {
            return java.util.concurrent.CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public AutoCloseable subscribe(String cache, Consumer<InvalidationMessage> handler) {
        byte[] channel = channelName(cache);
        String route = channelKey(channel);
        subscriptions.lock();
        try {
            AutoCloseable lane = dispatcher.register(cache, bytes -> {
                InvalidationMessage message;
                try {
                    MessageCodec.Decoded decoded = MessageCodec.decode(bytes);
                    if (!cache.equals(decoded.cache())) throw new IllegalArgumentException("Invalidation channel mismatch");
                    message = toMessage(decoded);
                } catch (Throwable failure) { throw new PubSubDispatcher.DecodeFailure(failure); }
                handler.accept(message);
            });
            Registration registration = new Registration(cache, lane);
            channels.put(route, registration);
            try { connection.sync().subscribe(channel); }
            catch (RuntimeException failure) {
                channels.remove(route, registration);
                try { lane.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
            return () -> {
                subscriptions.lock();
                try {
                    lane.close();
                    if (channels.remove(route, registration)) connection.async().unsubscribe(channel);
                } finally { subscriptions.unlock(); }
            };
        } finally { subscriptions.unlock(); }
    }

    private static String channelKey(byte[] channel) {
        return new String(channel, java.nio.charset.StandardCharsets.ISO_8859_1);
    }

    @Override public boolean requiresRegistrationReset() { return true; }
    @Override public boolean isDeliveryThread() { return dispatcher.isDeliveryThread(); }
    @Override public void setMetricsListener(io.tiercache.spi.CacheMetricsListener metrics) { dispatcher.metrics(metrics); }
    @Override public void setGapHandler(InvalidationGapHandler handler) { dispatcher.recovery(handler); }

    @Override public CompletionStage<InvalidationDeliveryFence> fenceDelivery(String cache) {
        return dispatcher.fence(cache);
    }

    @Override
    public void setReconnectListener(Runnable listener) {
        this.reconnectListener = listener;
    }

    private InvalidationMessage toMessage(MessageCodec.Decoded decoded) {
        Object key = decoded.keyBytes() != null ? keySerializer.fromBytes(decoded.keyBytes()) : null;
        Object payload = decoded.payload() != null
                ? valueSerializer.fromBytes(decoded.payload()) : null;
        return new InvalidationMessage(decoded.cache(), key, decoded.version(),
                decoded.originInstanceId(), decoded.type(), payload);
    }

    private static byte[] channelName(String cache) {
        return RedisKeyspace.channel(cache);
    }

    @Override
    public void close() {
        dispatcher.close();
        channels.clear();
        connection.close();
    }
}
