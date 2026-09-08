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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Default invalidation transport profile: Redis Pub/Sub (minimal latency).
 * One Pub/Sub connection per instance; per-cache channels
 * ({@code tiercache:inv:<cache>}); inbound events are dispatched on a
 * single daemon executor, off the I/O thread, preserving per-channel order.
 *
 * <p>On reconnect (detected via the Lettuce event bus) the registered
 * reconnect listener fires so the engine can replay the journal — Pub/Sub
 * itself is not durable.
 *
 * <p><b>Incubating:</b> 0.x API, may change before 1.0.
 */
public final class LettucePubSubInvalidationTransport implements InvalidationTransport {

    /** Channel keyspace prefix. */
    public static final String CHANNEL_PREFIX = "tiercache:inv:";

    private final RedisClient client;
    private final CacheSerializer<Object> keySerializer;
    private final StatefulRedisPubSubConnection<byte[], byte[]> connection;
    private final Map<String, Consumer<InvalidationMessage>> handlers = new ConcurrentHashMap<>();
    private final ExecutorService dispatcher;

    private volatile Runnable reconnectListener = () -> {
    };

    public LettucePubSubInvalidationTransport(RedisClient client,
            CacheSerializer<Object> keySerializer) {
        this.client = client;
        this.keySerializer = keySerializer;
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
        connection.async().publish(channelName(message.cache()),
                MessageCodec.encode(message, keyBytes));
    }

    @Override
    public AutoCloseable subscribe(String cache, Consumer<InvalidationMessage> handler) {
        handlers.put(cache, handler);
        connection.sync().subscribe(channelName(cache));
        return () -> {
            handlers.remove(cache);
            connection.async().unsubscribe(channelName(cache));
        };
    }

    @Override
    public void setReconnectListener(Runnable listener) {
        this.reconnectListener = listener;
    }

    private InvalidationMessage toMessage(MessageCodec.Decoded decoded) {
        Object key = decoded.keyBytes() != null ? keySerializer.fromBytes(decoded.keyBytes()) : null;
        return new InvalidationMessage(decoded.cache(), key, decoded.version(),
                decoded.originInstanceId(), decoded.type());
    }

    private static byte[] channelName(String cache) {
        return (CHANNEL_PREFIX + cache).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        dispatcher.shutdownNow();
        connection.close();
    }
}
