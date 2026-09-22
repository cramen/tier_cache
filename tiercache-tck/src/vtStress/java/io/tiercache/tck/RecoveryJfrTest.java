package io.tiercache.tck;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.tiercache.*;
import io.tiercache.internal.CircuitBreaker;
import io.tiercache.invalidation.InvalidationService;
import io.tiercache.redis.*;
import io.tiercache.spi.*;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import com.sun.net.httpserver.HttpServer;

import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

/** Real Redis journal I/O, a virtual-thread HTTP probe and the registered reconnect callback. */
class RecoveryJfrTest {
    private static Object field(Object instance, String name) throws Exception {
        var field = instance.getClass().getDeclaredField(name); field.setAccessible(true); return field.get(instance);
    }

    @Test void slowJournalDoesNotPinOrBlockProbeAndReconnectCallback() throws Exception {
        Path path = Path.of(System.getProperty("tiercache.recovery.jfr"));
        Files.createDirectories(path.getParent());
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var reads = new AtomicInteger(); var held = new AtomicBoolean(); var virtualJournal = new AtomicBoolean();
        var serviceRef = new AtomicReference<InvalidationService>(); var breakerRef = new AtomicReference<CircuitBreaker>();
        try (var redis = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine")).withExposedPorts(6379)) {
            redis.start(); String uri = "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
            try (var client = RedisClient.create(uri); var journalConnection = client.connect(ByteArrayCodec.INSTANCE);
                 var pauseConnection = client.connect();
                 var remote = LettuceRemoteCache.<Object, Object>builder(uri).client(client).cacheName("recovery-jfr").build();
                 var virtual = Executors.newVirtualThreadPerTaskExecutor()) {
                var journal = new RedisStreamJournal(journalConnection, 10000, new JdkCacheSerializer<>());
                InvalidationJournal observed = new InvalidationJournal() {
                    public String append(String c, InvalidationMessage m) { return journal.append(c, m); }
                    public List<JournalRow> readRange(String c, String p) { return journal.readRange(c, p); }
                    public String endCursor(String c) { return journal.endCursor(c); }
                    public boolean isTrimmed(String c, String p) { return journal.isTrimmed(c, p); }
                    public CheckedRange checkedRead(String c, String p, int count) {
                        try {
                            Object state = ((Map<?, ?>) field(serviceRef.get(), "states")).get(c);
                            held.compareAndSet(false, Thread.holdsLock(state) || Thread.holdsLock(breakerRef.get()));
                            virtualJournal.compareAndSet(false, Thread.currentThread().isVirtual());
                            if (reads.incrementAndGet() == 1) {
                                entered.countDown();
                                if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("journal gate timeout");
                            }
                            // Delay the actual server read, not merely an in-memory journal call.
                            pauseConnection.sync().clientPause(25);
                            return journal.checkedRead(c, p, count);
                        } catch (Exception e) { throw new IllegalStateException(e); }
                    }
                };
                var transport = new LettucePubSubInvalidationTransport(client, new JdkCacheSerializer<>());
                try (var factory = TierCacheFactory.builder().remoteCache(remote)
                        .circuitBreakerConfig(new CircuitBreaker.Config(2, 1, 1, Duration.ZERO, 1))
                        .invalidation(v -> {
                            var service = new InvalidationService(transport, observed, v.instanceId(), InvalidationListener.NOOP);
                            serviceRef.set(service); return service;
                        }).build()) {
                    var cache = factory.getCache("c");
                    CircuitBreaker breaker = (CircuitBreaker) field(factory, "breaker"); breakerRef.set(breaker);
                    remote.put("probe", StoredEntry.ofValue("available"), Duration.ofMinutes(1));
                    UUID writer = UUID.randomUUID();
                    for (int i = 1; i <= 1000; i++) journal.append("c", new InvalidationMessage("c", "k" + i,
                            new Version(i, writer), writer, InvalidationMessage.Type.INVALIDATE));
                    HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
                    server.setExecutor(virtual);
                    server.createContext("/probe", exchange -> {
                        byte[] body = String.valueOf(cache.get("probe")).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, body.length);
                        try (var out = exchange.getResponseBody()) { out.write(body); }
                    });
                    server.start();
                    try (Recording recording = new Recording(); HttpClient http = HttpClient.newHttpClient()) {
                        recording.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO).withStackTrace();
                        recording.setDestination(path); recording.start();
                        breaker.onFailure();
                        URI endpoint = new URI("http", null, server.getAddress().getAddress().getHostAddress(), server.getAddress().getPort(), "/probe", null, null);
                        var response = http.sendAsync(HttpRequest.newBuilder(endpoint).GET().build(), HttpResponse.BodyHandlers.ofString());
                        try {
                            assertTrue(entered.await(5, TimeUnit.SECONDS));
                            assertEquals("available", response.get(1, TimeUnit.SECONDS).body());
                            assertEquals(BreakerState.HALF_OPEN, breaker.state());
                            Runnable reconnect = (Runnable) field(transport, "reconnectListener");
                            virtual.submit(reconnect).get(1, TimeUnit.SECONDS);
                            assertFalse(held.get()); assertFalse(virtualJournal.get());
                        } finally { release.countDown(); }
                        long until = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                        while (breaker.state() != BreakerState.CLOSED && System.nanoTime() < until) Thread.sleep(10);
                        assertEquals(BreakerState.CLOSED, breaker.state()); assertTrue(reads.get() >= 5);
                        recording.stop();
                    } finally { release.countDown(); server.stop(0); }
                }
            }
        }
        var pinned = RecordingFile.readAllEvents(path).stream()
                .filter(e -> e.getEventType().getName().equals("jdk.VirtualThreadPinned"))
                .filter(e -> e.getStackTrace() != null && e.getStackTrace().getFrames().stream().anyMatch(f -> {
                    String name = f.getMethod().getType().getName();
                    return name.startsWith("io.tiercache.internal.") || name.startsWith("io.tiercache.invalidation.")
                            || name.startsWith("io.tiercache.redis.") || name.equals("io.tiercache.TierCacheFactory");
                })).toList();
        System.out.println("Recovery JFR: JDK=" + Runtime.version().feature() + ", real journal reads=" + reads.get()
                + ", library pinning=" + pinned.size() + ", recording=" + path);
        assertTrue(pinned.isEmpty(), () -> "library monitor pinning: " + pinned);
        assertFalse(held.get());
    }
}
