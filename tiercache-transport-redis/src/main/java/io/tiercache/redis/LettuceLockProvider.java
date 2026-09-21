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
import java.util.concurrent.atomic.AtomicInteger;

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
    private final AtomicInteger pendingCompensations = new AtomicInteger();
    private final Object schedulerLock = new Object();
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
        RedisCommands<String, String> resolved = commands;
        if (resolved == null) {
            synchronized (schedulerLock) {
                resolved = commands;
                if (resolved == null) {
                    resolved = (providedConnection != null ? providedConnection
                            : client.connect()).sync();
                    commands = resolved;
                }
            }
        }
        return resolved;
    }

    @Override
    public DistributedLock tryLock(String name, Duration lease) {
        String token = UUID.randomUUID().toString();
        String key = RedisKeyspace.lock(name);
        try {
            String result = commands().set(key, token, SetArgs.Builder.px(lease).nx());
            return "OK".equals(result) ? new LettuceLock(key, token) : null;
        } catch (RuntimeException e) {
            // Ambiguous outcome: the command may have executed server-side.
            scheduleCompensation(key, token, lease);
            throw e;
        }
    }

    /**
     * Schedules a best-effort compensating release for an ambiguous
     * acquire: retried until DELETE=1 (terminal) or the window expires —
     * never stopped by a zero delete. Overflow is dropped with a warning;
     * a task that never gets an attempt within the window expires into the
     * documented residual (an orphan self-expiring within one lease).
     */
    private void scheduleCompensation(String key, String token, Duration lease) {
        if (closed) {
            // No machinery is created and no retry is accepted after close;
            // the caller still sees the original acquire exception.
            return;
        }
        long windowMillis = Math.max(2 * lease.toMillis(), COMPENSATION_MIN_WINDOW.toMillis());
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(windowMillis);
        if (pendingCompensations.incrementAndGet() > COMPENSATION_PENDING_CAP) {
            pendingCompensations.decrementAndGet();
            log.warn("Too many pending lock compensations; dropping cleanup for '{}'. "
                    + "If the acquire executed, the orphan expires within its lease.", key);
            return;
        }
        try {
            scheduleCompensationAttempt(key, token, deadlineNanos);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // The scheduler shut down between the checks and the schedule:
            // best-effort must never replace the original acquire failure.
            pendingCompensations.decrementAndGet();
            log.debug("Lock compensation scheduling raced provider close for '{}'", key, e);
        }
    }

    private void scheduleCompensationAttempt(String key, String token, long deadlineNanos) {
        try {
            compensationScheduler().schedule(() -> attemptCompensation(key, token, deadlineNanos),
                    COMPENSATION_RETRY_MILLIS, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Closed between the checks and the (re)schedule: release the
            // bookkeeping; best-effort stays silent.
            pendingCompensations.decrementAndGet();
            log.debug("Lock compensation scheduling raced provider close for '{}'", key, e);
        }
    }

    private void attemptCompensation(String key, String token, long deadlineNanos) {
        if (closed) {
            pendingCompensations.decrementAndGet();
            return;
        }
        Long deleted = null;
        try {
            deleted = commands().eval(RELEASE_SCRIPT, ScriptOutputType.INTEGER,
                    new String[]{key}, token);
        } catch (RuntimeException e) {
            // Server still unreachable: retry within the window.
        }
        if (deleted != null && deleted == 1L) {
            pendingCompensations.decrementAndGet();
            return; // terminal: our lock existed and was removed
        }
        if (System.nanoTime() >= deadlineNanos) {
            pendingCompensations.decrementAndGet();
            log.warn("Lock compensation window expired for '{}'. If the acquire executed "
                    + "afterwards, the orphan expires within its lease.", key);
            return;
        }
        scheduleCompensationAttempt(key, token, deadlineNanos);
    }

    private ScheduledExecutorService compensationScheduler() {
        ScheduledExecutorService scheduler = compensationScheduler;
        if (scheduler == null) {
            synchronized (schedulerLock) {
                scheduler = compensationScheduler;
                if (scheduler == null && !closed) {
                    scheduler = Executors.newScheduledThreadPool(COMPENSATION_THREADS, runnable -> {
                        Thread thread = new Thread(runnable, "tiercache-lock-compensation");
                        thread.setDaemon(true);
                        return thread;
                    });
                    compensationScheduler = scheduler;
                }
            }
        }
        if (scheduler == null) {
            throw new java.util.concurrent.RejectedExecutionException(
                    "lock provider is closed");
        }
        return scheduler;
    }

    /**
     * Stops the compensation machinery. Pending compensations are
     * discarded; the scheduler is shut down. The Redis connection is NOT
     * closed (the caller keeps its ownership).
     *
     * @since 1.4.0
     */
    @Override
    public void close() {
        ScheduledExecutorService scheduler;
        // The closed transition and the scheduler lookup happen under the
        // same lifecycle lock as creation/publication: no scheduler can be
        // published after a completed close.
        synchronized (schedulerLock) {
            closed = true;
            scheduler = compensationScheduler;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /** Pending compensations (test/diagnostics seam). */
    int pendingCompensations() {
        return pendingCompensations.get();
    }

    /** True after {@link #close()} (test/diagnostics seam). */
    boolean isClosed() {
        return closed;
    }

    private final class LettuceLock implements DistributedLock {
        private final String fullName;
        private final String token;

        LettuceLock(String fullName, String token) {
            this.fullName = fullName;
            this.token = token;
        }

        @Override
        public boolean extend(Duration lease) {
            Long extended = commands().eval(EXTEND_SCRIPT, ScriptOutputType.INTEGER,
                    new String[]{fullName}, token, String.valueOf(lease.toMillis()));
            return extended != null && extended == 1L;
        }

        @Override
        public void release() {
            commands().eval(RELEASE_SCRIPT, ScriptOutputType.INTEGER,
                    new String[]{fullName}, token);
        }
    }
}
