package io.tiercache.redis;

import com.sun.net.httpserver.HttpServer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.*;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.spi.*;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

/** Six-process acceptance node; no production API or application dependency. */
public final class PubSubStandNode {
    public static void main(String[] args) throws Exception {
        String uri = System.getenv().getOrDefault("REDIS_URI", "redis://redis:6379");
        var client = RedisClient.create(uri);
        client.setOptions(io.lettuce.core.ClientOptions.builder()
                .timeoutOptions(io.lettuce.core.TimeoutOptions.enabled(Duration.ofSeconds(1))).build());
        var connection = client.connect(ByteArrayCodec.INSTANCE);
        var serializer = new JdkCacheSerializer<Object>();
        var journal = new RedisStreamJournal(connection, 10000, serializer);
        var transport = new LettucePubSubInvalidationTransport(client, serializer, serializer, 65536,
                new PubSubDispatchOptions(2, 16, 65536));
        AtomicReference<LongSupplier> messages = new AtomicReference<>(() -> 0), bytes = new AtomicReference<>(() -> 0);
        Map<String, BooleanSupplier> pending = new ConcurrentHashMap<>();
        AtomicLong rejected = new AtomicLong(), repairs = new AtomicLong(), sourceCalls = new AtomicLong();
        AtomicReference<CountDownLatch> pause = new AtomicReference<>();
        AtomicBoolean handlerPaused = new AtomicBoolean();
        var metrics = new CacheMetricsListener() {
            public AutoCloseable registerDispatch(LongSupplier count, LongSupplier size) {
                messages.set(count); bytes.set(size); return () -> { messages.set(() -> 0); bytes.set(() -> 0); };
            }
            public AutoCloseable registerDispatchPending(String cache, BooleanSupplier state) {
                pending.put(cache, state); return () -> pending.remove(cache, state);
            }
            public void onDispatchRejected(DispatchReason reason, long count) { rejected.addAndGet(count); }
            public void onDispatchRepair(String cache, RecoveryResult.Status result) { repairs.incrementAndGet(); }
        };
        List<LettuceRemoteCache<Object, Object>> remotes = new CopyOnWriteArrayList<>();
        var settings = new CacheSettings(10000, Duration.ofMinutes(5), null, Duration.ofHours(1), 0,
                NullPolicy.deny(), InvalidationMode.UPDATE, 65536);
        var factory = TierCacheFactory.builder().defaults(settings).metricsListener(metrics)
                .remoteCacheFactory(cache -> {
                    var remote = LettuceRemoteCache.builder(uri).client(client).cacheName(cache).journal(journal).invalidationMode(InvalidationMode.UPDATE, 65536).build();
                    remotes.add(remote); return remote;
                }).invalidation(version -> new InvalidationService(transport, journal, version.instanceId(), InvalidationListener.NOOP, metrics))
                .invalidationEventListener((cache, event) -> {
                    var gate = pause.get();
                    if (gate != null && cache.equals("a") && Thread.currentThread().getName().startsWith("tiercache-invalidation")) {
                        handlerPaused.set(true);
                        try { gate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                        finally { handlerPaused.set(false); }
                    }
                }).build();
        TierCache<String, String> a = factory.getCache("a"), b = factory.getCache("b");
        var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        var server = HttpServer.create(new InetSocketAddress(8080), 128);
        var workers = Executors.newFixedThreadPool(8); server.setExecutor(workers);
        server.createContext("/", exchange -> {
            int status = 200; String body;
            try {
                Map<String, String> query = new HashMap<>();
                String raw = exchange.getRequestURI().getRawQuery();
                if (raw != null) for (String item : raw.split("&")) {
                    String[] part = item.split("=", 2); query.put(part[0], URLDecoder.decode(part.length == 2 ? part[1] : "", StandardCharsets.UTF_8));
                }
                var cache = query.getOrDefault("cache", "a").equals("a") ? a : b;
                String path = exchange.getRequestURI().getPath();
                body = switch (path) {
                    case "/put" -> { cache.put("key", query.getOrDefault("value", "value")); yield "ok"; }
                    case "/burst" -> {
                        int count = Integer.parseInt(query.getOrDefault("n", "1000"));
                        for (int i = 0; i < count; i++) cache.put("key", "burst-" + i);
                        yield "burst-" + (count - 1);
                    }
                    case "/get" -> String.valueOf(cache.getOrCompute(query.getOrDefault("key", "key"), key -> {
                        sourceCalls.incrementAndGet();
                        try { return http.send(HttpRequest.newBuilder(URI.create("http://origin:8080/data/" + key)).GET().build(), HttpResponse.BodyHandlers.ofString()).body(); }
                        catch (Exception error) { throw new IllegalStateException(error); }
                    }));
                    case "/pause" -> { pause.set(new CountDownLatch(1)); yield "paused"; }
                    case "/resume" -> { var gate = pause.getAndSet(null); if (gate != null) gate.countDown(); yield "resumed"; }
                    case "/clear" -> { cache.evictAll(); yield "cleared"; }
                    case "/stats" -> "{\"a\":" + json(local(a)) + ",\"b\":" + json(local(b))
                            + ",\"messages\":" + messages.get().getAsLong() + ",\"bytes\":" + bytes.get().getAsLong()
                            + ",\"pendingA\":" + pending.getOrDefault("a", () -> false).getAsBoolean()
                            + ",\"pendingB\":" + pending.getOrDefault("b", () -> false).getAsBoolean()
                            + ",\"handlerPaused\":" + handlerPaused.get() + ",\"rejected\":" + rejected.get()
                            + ",\"repairs\":" + repairs.get() + ",\"sourceCalls\":" + sourceCalls.get()
                            + ",\"heapBytes\":" + java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() + "}";
                    default -> "ready";
                };
            } catch (Throwable failure) { status = 500; body = failure.getClass().getSimpleName(); }
            byte[] encoded = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, encoded.length); exchange.getResponseBody().write(encoded); exchange.close();
        });
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            var gate = pause.getAndSet(null); if (gate != null) gate.countDown();
            server.stop(0); workers.shutdownNow(); factory.close();
            remotes.forEach(LettuceRemoteCache::close); connection.close(); client.shutdown();
        }));
        System.out.println("PubSubStandNode ready");
    }
    private static String local(TierCache<?, ?> cache) throws Exception {
        var field = cache.getClass().getDeclaredField("l1"); field.setAccessible(true);
        @SuppressWarnings("unchecked") var local = (LocalCache<Object, Object>) field.get(cache);
        var entry = local.get("key"); return entry == null ? null : String.valueOf(entry.value());
    }
    private static String json(String value) { return value == null ? "null" : "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\""; }
}
