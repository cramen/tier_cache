package io.tiercache.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.tiercache.spi.DistributedLock;
import io.tiercache.spi.DistributedLockProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.HashSet;
import java.util.Set;
import io.tiercache.internal.LockProviderClosedException;

/**
 * {@link DistributedLockProvider} over Redis/Valkey via Lettuce, used for
 * cluster-wide rebuild coordination.
 *
 * <p>Acquire: {@code SET name token PX lease NX}. Release: Lua
 * compare-and-delete on the ownership token, so a stale holder can never
 * release another's lock. Extend: Lua token-checked {@code PEXPIRE ... XX}.
 * Locks live in the {@code tiercache:v2:rebuild:*} keyspace, separate from data
 * entries.
 *
 * <p><b>Ambiguous acquire compensation.</b> A failed acquire (client-side
 * timeout, connection error) may still have executed server-side. On a
 * {@code RuntimeException} from the acquire, the provider schedules a
 * best-effort compensating release with the same token-checked
 * compare-and-delete, retried until it either deletes (terminal: one
 * command executes at most once, and on a surviving connection no
 * execution can follow the delete) or the compensation window
 * ({@code max(2 x lease, 30 s)}) expires. A zero delete never stops the
 * retry — it proves nothing about a later-executing SET. Compensations
 * are bounded (pending cap per provider, small worker pool); overflow is
 * dropped with a warning, and the residual case — the SET executing only
 * after the window or over a dropped connection — leaves an orphan that
 * self-expires within one lease.
 *
 * <p><b>Internal — not part of the supported API.</b>
 *
 * @since 0.1.0
 */
public final class LettuceLockProvider implements DistributedLockProvider, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LettuceLockProvider.class);

    /**
     * Keyspace prefix for rebuild locks (never collides with data keys).
     *
     * @since 0.1.0
     */
    public static final String LOCK_KEYSPACE = RedisKeyspace.LOCK;

    private static final String RELEASE_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1])"
                    + " else return 0 end";

    // Token check implies the key exists, so plain PEXPIRE suffices
    // (PEXPIRE ... XX requires Redis 7.0; our baseline is 6.2).
    private static final String EXTEND_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1],"
                    + " ARGV[2]) else return 0 end";

    /** Retry interval for a pending compensation. */
    static final long COMPENSATION_RETRY_MILLIS = 250;
    /** Compensation window floor (the lease dominates when longer). */
    static final Duration COMPENSATION_MIN_WINDOW = Duration.ofSeconds(30);
    /** Pending compensations per provider; overflow is dropped with a warning. */
    static final int COMPENSATION_PENDING_CAP = 64;
    /** Compensation worker threads per provider. */
    private static final int COMPENSATION_THREADS = 2;

    private final RedisClient client;                // non-null ⇒ connect lazily
    private final StatefulRedisConnection<String, String> providedConnection;
    private volatile RedisCommands<String, String> commands;
    private final ReentrantLock lifecycle = new ReentrantLock();
    private final Set<Compensation> compensations = new HashSet<>();
    private StatefulRedisConnection<String, String> ownedConnection;
    private CompletableFuture<RedisCommands<String, String>> initializing;
    private volatile ScheduledExecutorService compensationScheduler;
    private volatile boolean closed;

    /**
     * Creates a provider that opens its own connection from {@code client}.
     * The connection is opened LAZILY on first use, so wiring the provider
     * never fails on an unreachable server at startup. The caller keeps
     * ownership of the client.
     *
     * @param client the Redis client to connect through
     * @since 0.1.0
     */
    public LettuceLockProvider(RedisClient client) {
        this.client = client;
        this.providedConnection = null;
    }

    /**
     * Creates a provider over an existing connection. The caller keeps
     * ownership of the connection.
     *
     * @param connection the connection to issue lock commands on
     * @since 0.1.0
     */
    public LettuceLockProvider(StatefulRedisConnection<String, String> connection) {
        this.client = null;
        this.providedConnection = connection;
    }

    private RedisCommands<String, String> commands() {
        CompletableFuture<RedisCommands<String, String>> attempt;
        boolean creator = false;
        lifecycle.lock();
        try {
            if (closed) throw new LockProviderClosedException();
            if (commands != null) return commands;
            attempt = initializing;
            if (attempt == null) {
                initializing = attempt = new CompletableFuture<>();
                creator = true;
            }
        } finally { lifecycle.unlock(); }
        if (creator) {
            StatefulRedisConnection<String, String> created = null;
            try {
                var connection = providedConnection;
                if (connection == null) connection = created = client.connect();
                var resolved = connection.sync();
                boolean accepted;
                lifecycle.lock();
                try {
                    accepted = !closed && initializing == attempt;
                    if (accepted) {
                        commands = resolved;
                        ownedConnection = created;
                        created = null; // ownership transferred to close
                        initializing = null;
                    }
                } finally { lifecycle.unlock(); }
                if (accepted) attempt.complete(resolved);
                else attempt.completeExceptionally(new LockProviderClosedException());
            } catch (RuntimeException | Error e) {
                lifecycle.lock();
                try { if (initializing == attempt) initializing = null; }
                finally { lifecycle.unlock(); }
                attempt.completeExceptionally(e);
            } finally {
                if (created != null) closeConnection(created);
            }
        }
        try { return attempt.join(); }
        catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException runtime) throw runtime;
            if (e.getCause() instanceof Error error) throw error;
            throw e;
        }
    }

    @Override
    public DistributedLock tryLock(String name, Duration lease) {
        var captured = commands();
        String token = UUID.randomUUID().toString();
        String key = RedisKeyspace.lock(name);
        if (closed) throw new LockProviderClosedException(); // dispatch admission
        String result;
        try {
            result = captured.set(key, token, SetArgs.Builder.px(lease).nx());
        } catch (RuntimeException e) {
            scheduleCompensation(captured, key, token, lease);
            throw e;
        }
        if (closed) {
            if ("OK".equals(result)) {
                try { captured.eval(RELEASE_SCRIPT, ScriptOutputType.INTEGER, new String[]{key}, token); }
                catch (RuntimeException ignored) { /* Lease bounds failed shutdown cleanup. */ }
            }
            throw new LockProviderClosedException();
        }
        return "OK".equals(result) ? new LettuceLock(key, token, captured) : null;
    }

    private void scheduleCompensation(RedisCommands<String, String> captured,
            String key, String token, Duration lease) {
        long window = Math.max(2 * lease.toMillis(), COMPENSATION_MIN_WINDOW.toMillis());
        var task = new Compensation(captured, key, token,
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(window));
        lifecycle.lock();
        try {
            if (closed) return;
            if (compensations.size() >= COMPENSATION_PENDING_CAP) {
                log.warn("Too many pending lock compensations; cleanup falls back to lease expiry");
                return;
            }
            if (compensationScheduler == null) {
                compensationScheduler = Executors.newScheduledThreadPool(COMPENSATION_THREADS, r -> {
                    Thread t = new Thread(r, "tiercache-lock-compensation"); t.setDaemon(true); return t;
                });
            }
            compensations.add(task);
            task.scheduleLocked();
        } finally { lifecycle.unlock(); }
    }

    private final class Compensation implements Runnable {
        final RedisCommands<String, String> captured;
        final String key, token;
        final long deadline;
        Compensation(RedisCommands<String, String> captured, String key, String token, long deadline) {
            this.captured = captured; this.key = key; this.token = token; this.deadline = deadline;
        }
        void scheduleLocked() {
            if (closed || !compensations.contains(this)) { compensations.remove(this); return; }
            try { compensationScheduler.schedule(this, COMPENSATION_RETRY_MILLIS, TimeUnit.MILLISECONDS); }
            catch (RejectedExecutionException e) { compensations.remove(this); }
        }
        @Override public void run() {
            lifecycle.lock();
            try {
                if (closed || !compensations.contains(this)) return;
                if (System.nanoTime() >= deadline) { compensations.remove(this); return; }
            } finally { lifecycle.unlock(); }
            Long deleted = null;
            try { deleted = captured.eval(RELEASE_SCRIPT, ScriptOutputType.INTEGER, new String[]{key}, token); }
            catch (RuntimeException ignored) { /* Retry ambiguous cleanup within the window. */ }
            lifecycle.lock();
            try {
                if (closed || Long.valueOf(1).equals(deleted) || System.nanoTime() >= deadline) {
                    compensations.remove(this);
                } else scheduleLocked();
            } finally { lifecycle.unlock(); }
        }
    }

    /** Stops auxiliary work and closes only the connection created by this provider. */
    @Override
    public void close() {
        ScheduledExecutorService scheduler;
        StatefulRedisConnection<String, String> connection;
        CompletableFuture<RedisCommands<String, String>> attempt;
        lifecycle.lock();
        try {
            if (closed) return;
            closed = true;
            scheduler = compensationScheduler;
            connection = ownedConnection;
            ownedConnection = null;
            commands = null;
            attempt = initializing;
            initializing = null;
            compensations.clear();
        } finally { lifecycle.unlock(); }
        if (attempt != null) attempt.completeExceptionally(new LockProviderClosedException());
        if (scheduler != null) scheduler.shutdownNow();
        if (connection != null) closeConnection(connection);
    }

    private static void closeConnection(StatefulRedisConnection<String, String> connection) {
        try { connection.close(); }
        catch (RuntimeException e) { log.warn("Owned lock connection close failed ({})", e.getClass().getSimpleName()); }
    }

    /** Pending compensations (test/diagnostics seam). */
    int pendingCompensations() {
        lifecycle.lock();
        try { return compensations.size(); }
        finally { lifecycle.unlock(); }
    }

    boolean isClosed() { return closed; }

    private final class LettuceLock implements DistributedLock {
        private final String fullName;
        private final String token;

        private final RedisCommands<String, String> captured;

        LettuceLock(String fullName, String token, RedisCommands<String, String> captured) {
            this.captured = captured;
            this.fullName = fullName;
            this.token = token;
        }

        @Override
        public boolean extend(Duration lease) {
            if (closed) return false;
            Long extended = captured.eval(EXTEND_SCRIPT, ScriptOutputType.INTEGER,
                    new String[]{fullName}, token, String.valueOf(lease.toMillis()));
            return extended != null && extended == 1L;
        }

        @Override
        public void release() {
            captured.eval(RELEASE_SCRIPT, ScriptOutputType.INTEGER,
                    new String[]{fullName}, token);
        }
    }
}
