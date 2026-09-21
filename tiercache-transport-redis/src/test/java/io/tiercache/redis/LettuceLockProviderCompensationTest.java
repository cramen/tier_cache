package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ambiguous lock-acquire compensation (load-test review): a timed-out
 * acquire that executed server-side is cleaned by the token-checked
 * compensating release — retried past zero deletes, bounded in pending
 * tasks, with the residual bounded by one lease.
 */
class LettuceLockProviderCompensationTest {

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

    /**
     * A commands proxy delegating to a real connection, with per-command
     * fault injection for {@code set(String, String, SetArgs)} and
     * {@code eval}.
     */
    @SuppressWarnings("unchecked")
    private static RedisCommands<String, String> proxy(StatefulRedisConnection<String, String> real,
            BiFunction<Object[], Method, Object> override) {
        InvocationHandler handler = (proxy, method, args) -> {
            Object overridden = override.apply(args, method);
            if (overridden != null) {
                return overridden;
            }
            return method.invoke(real.sync(), args);
        };
        return (RedisCommands<String, String>) Proxy.newProxyInstance(
                LettuceLockProviderCompensationTest.class.getClassLoader(),
                new Class<?>[]{RedisCommands.class}, handler);
    }

    private static Object passthrough() {
        return null;
    }

    private static void awaitTrue(Supplier<Boolean> check, long timeoutMillis, String description)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!check.get()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeoutMillis + "ms: "
                        + description);
            }
            Thread.sleep(20);
        }
    }

    private static String lockKey(String name) {
        return LettuceLockProvider.LOCK_KEYSPACE + name;
    }

    /**
     * The bench's mechanism: the SET executes server-side, the caller sees
     * a timeout. The compensating release must remove the orphan promptly
     * (pre-fix it lingered for the lease).
     */
    @Test
    void timedOutAcquireThatExecutedIsCompensated() throws Exception {
        var connection = client.connect();
        Map<String, String> captured = new ConcurrentHashMap<>();
        java.util.concurrent.atomic.AtomicBoolean fired = new java.util.concurrent.atomic.AtomicBoolean();
        RedisCommands<String, String> commands = proxy(connection, (args, method) -> {
            if (!fired.get() && "set".equals(method.getName()) && args != null && args.length == 3
                    && args[2] instanceof SetArgs) {
                fired.set(true);
                // Execute for real, then report a client timeout.
                try {
                    method.invoke(connection.sync(), args);
                } catch (Exception ignored) {
                }
                captured.put((String) args[0], (String) args[1]);
                throw new RedisCommandTimeoutException("simulated client timeout");
            }
            return passthrough();
        });
        LettuceLockProvider provider = new LettuceLockProvider(connectionTo(commands));
        String key = lockKey("comp-1");

        assertThrows(RedisCommandTimeoutException.class,
                () -> provider.tryLock("comp-1", Duration.ofSeconds(30)));
        assertNotNull(connection.sync().get(key), "the SET executed server-side");

        awaitTrue(() -> connection.sync().get(key) == null, 5_000,
                "the compensation must remove the orphan promptly");
        assertNotNull(provider.tryLock("comp-1", Duration.ofSeconds(5)),
                "another node acquires promptly after the cleanup");
        provider.close();
        connection.close();
    }

    /** Wraps a commands proxy back into a connection-shaped object. */
    private static StatefulRedisConnection<String, String> connectionTo(
            RedisCommands<String, String> commands) {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("sync".equals(method.getName())) {
                return commands;
            }
            if ("close".equals(method.getName())) {
                return null;
            }
            throw new UnsupportedOperationException(method.getName());
        };
        return (StatefulRedisConnection<String, String>) Proxy.newProxyInstance(
                LettuceLockProviderCompensationTest.class.getClassLoader(),
                new Class<?>[]{StatefulRedisConnection.class}, handler);
    }

    /**
     * The harder ordering: compensating deletes return 0 while the SET has
     * not landed yet, and the SET executes afterwards — a stop-on-zero
     * design would leave the orphan, the retry must not stop.
     */
    @Test
    void lateSetAfterZeroDeletesIsStillCleaned() throws Exception {
        var connection = client.connect();
        Map<String, String> captured = new ConcurrentHashMap<>();
        RedisCommands<String, String> commands = proxy(connection, (args, method) -> {
            if ("set".equals(method.getName()) && args != null && args.length == 3
                    && args[2] instanceof SetArgs) {
                // Do NOT execute: capture the token and report a timeout.
                captured.put((String) args[0], (String) args[1]);
                throw new RedisCommandTimeoutException("simulated client timeout");
            }
            return passthrough();
        });
        LettuceLockProvider provider = new LettuceLockProvider(connectionTo(commands));
        String key = lockKey("comp-2");

        assertThrows(RedisCommandTimeoutException.class,
                () -> provider.tryLock("comp-2", Duration.ofSeconds(30)));
        Thread.sleep(800); // several compensating deletes have returned 0 by now
        // The SET finally "executes" — with the captured token.
        connection.sync().set(key, captured.get(key), SetArgs.Builder.px(30_000).nx());

        awaitTrue(() -> connection.sync().get(key) == null, 5_000,
                "a later retry must remove the orphan (never stop on zero deletes)");
        provider.close();
        connection.close();
    }

    /**
     * Bounded machinery: a batch of accepted compensations with slow
     * commands all complete within the window, and the pending count never
     * exceeds the cap (overflow is dropped, not queued).
     */
    @Test
    void acceptedTasksFitTheWindowAndOverflowIsBounded() throws Exception {
        var connection = client.connect();
        // Real orphans with matching tokens via a slow capturing proxy:
        // each SET executes, then a client timeout is reported.
        Map<String, String> captured = new ConcurrentHashMap<>();
        RedisCommands<String, String> capturing = proxy(connection, (args, method) -> {
            if ("set".equals(method.getName()) && args != null && args.length == 3
                    && args[2] instanceof SetArgs) {
                try {
                    method.invoke(connection.sync(), args);
                } catch (Exception ignored) {
                }
                captured.put((String) args[0], (String) args[1]);
                throw new RedisCommandTimeoutException("simulated client timeout");
            }
            if ("eval".equals(method.getName())) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return passthrough();
        });
        LettuceLockProvider slowProvider = new LettuceLockProvider(connectionTo(capturing));
        for (int i = 0; i < 10; i++) {
            final int index = i;
            assertThrows(RedisCommandTimeoutException.class,
                    () -> slowProvider.tryLock("comp-3-" + index, Duration.ofSeconds(30)));
        }
        int peak = slowProvider.pendingCompensations();
        assertTrue(peak > 0 && peak <= 64, "pending compensations bounded, got " + peak);

        awaitTrue(() -> slowProvider.pendingCompensations() == 0, 30_000,
                "all accepted compensations complete within the window");
        for (int i = 0; i < 10; i++) {
            assertNull(connection.sync().get(lockKey("comp-3-" + i)),
                    "orphan " + i + " must be cleaned");
        }

        // Overflow: 70 ambiguous acquires against the cap of 64.
        RedisCommands<String, String> failing = proxy(connection, (args, method) -> {
            if ("set".equals(method.getName()) && args != null && args.length == 3
                    && args[2] instanceof SetArgs) {
                throw new RedisCommandTimeoutException("simulated client timeout");
            }
            return passthrough();
        });
        LettuceLockProvider failingProvider = new LettuceLockProvider(connectionTo(failing));
        for (int i = 0; i < 70; i++) {
            final int index = i;
            assertThrows(RedisCommandTimeoutException.class,
                    () -> failingProvider.tryLock("comp-4-" + index, Duration.ofSeconds(30)));
        }
        assertTrue(failingProvider.pendingCompensations() <= 64,
                "the pending cap holds, got " + failingProvider.pendingCompensations());
        slowProvider.close();
        failingProvider.close();
        connection.close();
    }

    /**
     * The residual contract: a compensation dropped at the cap leaves the
     * orphan to expire on its own within one lease.
     */
    @Test
    void droppedCompensationFallsBackToTheLease() throws Exception {
        var connection = client.connect();
        // Fill the cap with never-ending compensations (no SET executed, so
        // every delete is a 0 and the tasks keep retrying).
        RedisCommands<String, String> failing = proxy(connection, (args, method) -> {
            if ("set".equals(method.getName()) && args != null && args.length == 3
                    && args[2] instanceof SetArgs) {
                throw new RedisCommandTimeoutException("simulated client timeout");
            }
            return passthrough();
        });
        LettuceLockProvider provider = new LettuceLockProvider(connectionTo(failing));
        for (int i = 0; i < 64; i++) {
            final int index = i;
            assertThrows(RedisCommandTimeoutException.class,
                    () -> provider.tryLock("comp-5-" + index, Duration.ofSeconds(30)));
        }
        assertEquals(64, provider.pendingCompensations(), "the cap is full");

        // The 65th ambiguous acquire is dropped — but its SET DID execute
        // (plant it via a capturing proxy).
        Map<String, String> captured = new ConcurrentHashMap<>();
        RedisCommands<String, String> executing = proxy(connection, (args, method) -> {
            if ("set".equals(method.getName()) && args != null && args.length == 3
                    && args[2] instanceof SetArgs) {
                try {
                    method.invoke(connection.sync(), args);
                } catch (Exception ignored) {
                }
                captured.put((String) args[0], (String) args[1]);
                throw new RedisCommandTimeoutException("simulated client timeout");
            }
            return passthrough();
        });
        LettuceLockProvider executingProvider = new LettuceLockProvider(connectionTo(executing));
        assertThrows(RedisCommandTimeoutException.class,
                () -> executingProvider.tryLock("comp-5-x", Duration.ofSeconds(2)));
        String orphanKey = lockKey("comp-5-x");
        assertNotNull(connection.sync().get(orphanKey), "the late SET executed");

        awaitTrue(() -> connection.sync().get(orphanKey) == null, 5_000,
                "with no compensation, the orphan self-expires within its lease");
        provider.close();
        executingProvider.close();
        connection.close();
    }

    /**
     * A different owner's lock is never removed by the compensation: after
     * the orphan is cleaned, a fresh acquire works and its lease is
     * untouched by any leftover retries.
     */
    @Test
    void compensationNeverRemovesAnotherOwnersLock() throws Exception {
        var connection = client.connect();
        LettuceLockProvider owner = new LettuceLockProvider(connection);
        io.tiercache.spi.DistributedLock lock = owner.tryLock("comp-6", Duration.ofSeconds(10));
        assertNotNull(lock, "a fresh acquire works");

        // A compensation for a DIFFERENT token retries harmlessly against it.
        Map<String, String> captured = new ConcurrentHashMap<>();
        RedisCommands<String, String> failing = proxy(connection, (args, method) -> {
            if ("set".equals(method.getName()) && args != null && args.length == 3
                    && args[2] instanceof SetArgs) {
                captured.put((String) args[0], (String) args[1]);
                throw new RedisCommandTimeoutException("simulated client timeout");
            }
            return passthrough();
        });
        LettuceLockProvider failingProvider = new LettuceLockProvider(connectionTo(failing));
        assertThrows(RedisCommandTimeoutException.class,
                () -> failingProvider.tryLock("comp-6", Duration.ofSeconds(30)));
        Thread.sleep(900); // let several compensating attempts run
        assertNotNull(connection.sync().get(lockKey("comp-6")),
                "the other owner's lease is untouched by the token-checked retries");
        lock.release();
        failingProvider.close();
        owner.close();
        connection.close();
    }

    /**
     * Close during a latched acquire: no machinery is created after close,
     * and the caller sees the original Redis timeout — never a
     * RejectedExecutionException.
     */
    @Test
    void closeDuringAcquireCreatesNoMachineryAndKeepsTheTimeout() throws Exception {
        var connection = client.connect();
        java.util.concurrent.CountDownLatch setEntered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch releaseSet = new java.util.concurrent.CountDownLatch(1);
        RedisCommands<String, String> commands = proxy(connection, (args, method) -> {
            if ("set".equals(method.getName()) && args != null && args.length == 3
                    && args[2] instanceof SetArgs) {
                setEntered.countDown();
                try {
                    releaseSet.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new RedisCommandTimeoutException("simulated client timeout");
            }
            return passthrough();
        });
        LettuceLockProvider provider = new LettuceLockProvider(connectionTo(commands));

        java.util.concurrent.atomic.AtomicReference<Throwable> seen =
                new java.util.concurrent.atomic.AtomicReference<>();
        Thread acquirer = new Thread(() -> {
            try {
                provider.tryLock("close-race", Duration.ofSeconds(30));
            } catch (Throwable t) {
                seen.set(t);
            }
        });
        acquirer.start();
        if (!setEntered.await(5, TimeUnit.SECONDS)) {
            throw new AssertionError("the acquire never reached the latched SET");
        }
        provider.close();
        releaseSet.countDown();
        acquirer.join(5_000);

        assertTrue(seen.get() instanceof RedisCommandTimeoutException,
                "the original timeout surfaces, got " + seen.get());
        assertTrue(provider.isClosed());
        long compensationThreads = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith("tiercache-lock-compensation")).count();
        assertEquals(0, compensationThreads,
                "no scheduler may be created after close");
        connection.close();
    }

    /**
     * Close during a latched acquire with an ALREADY-created scheduler:
     * the scheduling race is swallowed, the original timeout still
     * surfaces, and pending bookkeeping is released.
     */
    @Test
    void closeWithLiveSchedulerSwallowsTheSchedulingRace() throws Exception {
        var connection = client.connect();
        // The failing proxy always reports a client timeout on acquire.
        RedisCommands<String, String> failing = proxy(connection, (args, method) -> {
            if ("set".equals(method.getName()) && args != null && args.length == 3
                    && args[2] instanceof SetArgs) {
                throw new RedisCommandTimeoutException("simulated client timeout");
            }
            return passthrough();
        });
        LettuceLockProvider provider = new LettuceLockProvider(connectionTo(failing));
        // First ambiguous acquire spins the scheduler up.
        assertThrows(RedisCommandTimeoutException.class,
                () -> provider.tryLock("close-live-1", Duration.ofSeconds(30)));

        provider.close();
        // Another ambiguous acquire on the closed provider: the scheduling
        // race must be swallowed and the ORIGINAL timeout must surface.
        try {
            provider.tryLock("close-live-2", Duration.ofSeconds(30));
            throw new AssertionError("the acquire must fail");
        } catch (Throwable t) {
            assertTrue(t instanceof RedisCommandTimeoutException,
                    "the original timeout surfaces even with a live scheduler, got " + t);
        }
        assertTrue(provider.pendingCompensations() <= 1,
                "bookkeeping stays bounded after close, got " + provider.pendingCompensations());
        connection.close();
    }

    /**
     * Real pause/unpause round trip (the bench's outage shape): the
     * provider cleans the orphan once Redis resumes.
     */
    @Test
    void realPauseUnpauseRoundTrip() throws Exception {
        var timeoutClient = RedisClient.create(io.lettuce.core.RedisURI.builder()
                .withHost(server.getHost()).withPort(server.getMappedPort(6379))
                .withTimeout(Duration.ofMillis(500)).build());
        var connection = timeoutClient.connect();
        LettuceLockProvider provider = new LettuceLockProvider(connection);
        String key = lockKey("comp-7");

        server.getDockerClient().pauseContainerCmd(server.getContainerId()).exec();
        try {
            assertThrows(Exception.class, () -> provider.tryLock("comp-7", Duration.ofSeconds(30)),
                    "the acquire times out while Redis is paused");
        } finally {
            server.getDockerClient().unpauseContainerCmd(server.getContainerId()).exec();
        }

        // The SET may or may not have executed; in either case there must
        // be no lingering orphan shortly after recovery.
        Thread.sleep(1_500);
        var probe = client.connect();
        try {
            awaitTrue(() -> probe.sync().get(key) == null
                            || provider.tryLock("comp-7b", Duration.ofSeconds(3)) != null,
                    5_000, "the provider recovers after the unpause");
            assertTrue(provider.tryLock("comp-7", Duration.ofSeconds(3)) != null
                            || probe.sync().get(key) == null,
                    "no orphan blocks re-acquisition after recovery");
        } finally {
            probe.close();
            provider.close();
            connection.close();
            timeoutClient.shutdown();
        }
    }
}
