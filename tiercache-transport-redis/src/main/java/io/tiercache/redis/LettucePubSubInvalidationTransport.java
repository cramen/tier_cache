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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Default invalidation transport profile: Redis Pub/Sub (minimal latency).
 * One Pub/Sub connection per instance; per-cache channels
 * ({@code tiercache:v2:inv:<token(cache)>}); inbound events are dispatched on a
 * single daemon executor, off the I/O thread, preserving per-channel order.
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
    private final Map<String, Consumer<InvalidationMessage>> handlers = new ConcurrentHashMap<>();
    private final ExecutorService dispatcher;

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
        this.client = client;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.payloadCapBytes = payloadCapBytes;
        this.connection = client.connectPubSub(ByteArrayCodec.INSTANCE);
        this.dispatcher = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "tiercache-invalidation");
            t.setDaemon(true);
            return t;
        });
        connection.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(byte[] channel, byte[] message) {
                MessageCodec.Decoded decoded = MessageCodec.decode(message);
                Consumer<InvalidationMessage> handler = handlers.get(decoded.cache());
                if (handler == null) {
                    return;
                }
                dispatcher.execute(() -> handler.accept(toMessage(decoded)));
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
    public void publish(InvalidationMessage message) {
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
        connection.async().publish(channelName(message.cache()),
                MessageCodec.encode(toSend, keyBytes));
    }

    @Override
    public AutoCloseable subscribe(String cache, Consumer<InvalidationMessage> handler) {
        byte[] channel = channelName(cache); // validate before registering a handler
        handlers.put(cache, handler);
        connection.sync().subscribe(channel);
        return () -> {
            handlers.remove(cache);
            connection.async().unsubscribe(channel);
        };
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
        dispatcher.shutdownNow();
        connection.close();
    }
}
