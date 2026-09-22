package io.tiercache.invalidation;

import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import io.tiercache.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Versioned invalidation and bounded, triggered journal recovery. State gates
 * cover only in-memory target/cursor commits. Journal I/O and observers never
 * execute under those gates. Internal API, not an application entry point.
 */
public final class InvalidationService implements InvalidationHandler {
    private static final Logger log = LoggerFactory.getLogger(InvalidationService.class);
    private static final int READ_BATCH = 256;
    private static final int MAX_BATCHES = 16;
    private static final int WINDOW_CAP = 128;
    private static final int WINDOW_HARD_CAP = 512;
    private static final int CONFIRMED_CAP = 256;

    private final InvalidationTransport transport;
    private final InvalidationJournal journal;
    private final UUID originInstanceId;
    private final InvalidationListener listener;
    private final CacheMetricsListener metrics;
    private final java.util.function.LongSupplier clock;
    private volatile InvalidationEventListener eventListener = InvalidationEventListener.NOOP;
    private final Map<String, CacheState> states = new ConcurrentHashMap<>();
    private final Object lifecycle = new Object();
    private final Object executorGate = new Object();
    private ScheduledExecutorService executor;
    private boolean ownsExecutor;
    private volatile boolean closed;
    private CompletableFuture<Boolean> aggregate;
    private Set<CacheState> aggregateTargets = Set.of();

    private static final class CacheState {
        final String cache;
        InvalidationTarget target;
        String cursor;
        long generation, registration, token, deliveries;
        final Map<Version, Long> applied = new HashMap<>();
        final LinkedHashSet<Version> confirmed = new LinkedHashSet<>();
        ScheduledFuture<?> scheduled;
        boolean running, ready, tick, replay, reset, resyncRequired, retired, resetOnFailure;
        boolean metricClaimed;
        AutoCloseable subscription, gauge;
        volatile boolean pending;
        int failures;
        long retryAt, nextCorruptionLog;
        RecoveryResult lastResult, safeResult;
        long safeTargetGeneration;
        CompletableFuture<RecoveryResult> resetCompletion;
        CacheState(String cache) { this.cache = cache; }
    }

    private record Snapshot(long generation, long token, String cursor, long targetGeneration,
                            InvalidationTarget target, long deliverySequence) { }
    private enum Kind { TICK_DONE, CAUGHT_UP, RESET_SAFE, NO_JOURNAL, FAILED, OBSOLETE }
    private record Pass(Kind kind, String baseline, long generation, long targetGeneration) {
        Pass(Kind kind, String baseline, long generation) { this(kind, baseline, generation, -1); }
    }

    /** Creates an engine with no metrics binder. */
    public InvalidationService(InvalidationTransport transport, InvalidationJournal journal,
            UUID originInstanceId, InvalidationListener listener) {
        this(transport, journal, originInstanceId, listener, CacheMetricsListener.NOOP);
    }

    /** Creates an engine; standalone engines lazily own two workers unless configured by a factory. */
    public InvalidationService(InvalidationTransport transport, InvalidationJournal journal,
            UUID originInstanceId, InvalidationListener listener, CacheMetricsListener metrics) {
        this(transport, journal, originInstanceId, listener, metrics, System::nanoTime);
    }

    InvalidationService(InvalidationTransport transport, InvalidationJournal journal,
            UUID originInstanceId, InvalidationListener listener, CacheMetricsListener metrics,
            java.util.function.LongSupplier clock) {
        this.clock = clock;
        this.transport = Objects.requireNonNull(transport);
        this.journal = journal;
        this.originInstanceId = Objects.requireNonNull(originInstanceId);
        this.listener = listener == null ? InvalidationListener.NOOP : listener;
        this.metrics = metrics == null ? CacheMetricsListener.NOOP : metrics;
        transport.setMetricsListener(this.metrics);
        transport.setGapHandler(new InvalidationGapHandler() {
            public CompletionStage<RecoveryResult> reset(String cache) { return resetAsync(cache); }
            public RecoveryResult registrationBaseline(String cache) {
                CacheState state = states.get(cache);
                if (state == null) return null;
                synchronized (state) { return currentProof(state, state.safeResult) ? state.safeResult : null; }
            }
            public boolean isCurrent(String cache, RecoveryResult result) {
                CacheState state = states.get(cache);
                if (state == null) return false;
                synchronized (state) { return currentProof(state, result); }
            }
        });
        transport.setReconnectListener(this::onReconnect);
    }

    private boolean currentProof(CacheState state, RecoveryResult result) {
        return !closed && !state.retired && state.ready && result != null
                && result == state.safeResult && result.status() == RecoveryResult.Status.RESET_SAFE
                && state.generation == result.generation()
                && state.target.recoveryGeneration() == state.safeTargetGeneration;
    }

    @Override
    public void configureRecoveryExecutor(ScheduledExecutorService workers) {
        synchronized (executorGate) {
            if (executor != null || closed) throw new IllegalStateException("Recovery executor must be configured before use");
            executor = Objects.requireNonNull(workers);
        }
    }

    private ScheduledExecutorService workers() {
        synchronized (executorGate) {
            if (closed) throw new RejectedExecutionException("Invalidation service is closed");
            if (executor == null) {
                ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(2, runnable -> {
                    Thread thread = new Thread(runnable, "tiercache-recovery");
                    thread.setDaemon(true);
                    return thread;
                });
                pool.setRemoveOnCancelPolicy(true);
                executor = pool;
                ownsExecutor = true;
            }
            return executor;
        }
    }

    @Override
    public void registerTarget(String cache, InvalidationTarget target) {
        CacheState state;
        long registration;
        boolean resetRegistration = transport.requiresRegistrationReset();
        AutoCloseable retiredSubscription;
        synchronized (lifecycle) {
            if (closed) return;
            state = states.computeIfAbsent(cache, CacheState::new);
            synchronized (state) {
                if (state.ready && state.subscription != null && !resetRegistration) {
                    state.target = target;
                    state.generation++;
                    state.lastResult = null;
                    if (state.pending) { state.replay = true; schedule(state); }
                    return;
                }
                retiredSubscription = state.subscription;
                state.subscription = null;
                state.target = target;
                state.generation++;
                registration = ++state.registration;
                state.ready = false;
                state.lastResult = null;
            }
        }
        // Initial baseline and subscription are outside service/state monitors.
        closeQuietly(retiredSubscription);
        String baseline = journal == null ? "0-0" : journal.endCursor(cache);
        synchronized (state) {
            if (closed || state.registration != registration) return;
            if (resetRegistration) {
                long next = target.resetRecovery(target.recoveryGeneration());
                if (next < 0 || target.recoveryGeneration() != next) throw new IllegalStateException("Registration reset was superseded");
            }
            state.cursor = baseline;
            state.safeTargetGeneration = target.recoveryGeneration();
            state.safeResult = journal == null ? null : new RecoveryResult(RecoveryResult.Status.RESET_SAFE, baseline, state.generation);
            state.applied.clear();
            state.confirmed.clear();
            state.ready = true;
        }
        AutoCloseable subscription = transport.subscribe(cache, message -> onMessage(message, registration));
        AutoCloseable previous;
        boolean registerMetric = false;
        synchronized (state) {
            if (closed || state.registration != registration) previous = subscription;
            else {
                previous = state.subscription;
                state.subscription = subscription;
                if (!state.metricClaimed) { state.metricClaimed = true; registerMetric = true; }
            }
        }
        closeQuietly(previous);
        if (registerMetric) {
            AutoCloseable gauge = null;
            try { gauge = metrics.registerRecovery(cache, () -> state.pending); }
            catch (Throwable e) { log.warn("Recovery metric registration failed ({})", e.getClass().getSimpleName()); }
            boolean discard;
            synchronized (state) { discard = closed; if (!discard) state.gauge = gauge; }
            if (discard) closeQuietly(gauge);
        }
        synchronized (state) { if (state.pending) schedule(state); }
    }

    @Override
    public void onLocalWrite(String cache, Object key, Version version, InvalidationMessage.Type type) {
        if (closed) return;
        if (type == InvalidationMessage.Type.EVICT_ALL) {
            CacheState state = states.get(cache);
            if (state != null) synchronized (state) {
                state.generation++;
                if (state.pending) { state.replay = true; state.lastResult = null; schedule(state); }
            }
        }
        transport.publish(new InvalidationMessage(cache, key, version, originInstanceId, type));
        observe(() -> metrics.onInvalidation(cache, CacheMetricsListener.Direction.SENT));
    }

    @Override
    public void onLocalUpdate(String cache, Object key, Object value, Version version) {
        if (closed) return;
        transport.publish(new InvalidationMessage(cache, key, version, originInstanceId,
                InvalidationMessage.Type.UPDATE, value));
        observe(() -> metrics.onInvalidation(cache, CacheMetricsListener.Direction.SENT));
    }

    private void onMessage(InvalidationMessage message, long registration) {
        if (closed) {
            if (transport.requiresRegistrationReset()) throw new IllegalStateException("Invalidation service is closed");
            return;
        }
        if (message.originInstanceId().equals(originInstanceId)) return;
        CacheState state = states.get(message.cache());
        if (state == null) return;
        synchronized (state) {
            if (closed || !state.ready || state.registration != registration) {
                throw new IllegalStateException("Invalidation registration was closed or superseded");
            }
            applyLive(state.target, message);
            if (message.type() == InvalidationMessage.Type.EVICT_ALL) {
                state.generation++;
                if (state.running) state.replay = true;
            }
            if (journal != null) {
                if (!state.resyncRequired && !state.confirmed.contains(message.version())) {
                    if (state.applied.size() >= WINDOW_HARD_CAP && !state.applied.containsKey(message.version())) {
                        state.applied.clear();
                        state.resyncRequired = true;
                        state.generation++;
                    } else state.applied.putIfAbsent(message.version(), state.deliveries + 1);
                }
                if (state.resyncRequired || state.applied.size() > WINDOW_CAP) state.replay = true;
                if (++state.deliveries % 64 == 0) state.tick = true;
                if (state.replay || state.tick) { state.pending = true; schedule(state); }
            }
        }
        notifyEvent(message, false);
    }

    private static void applyLive(InvalidationTarget target, InvalidationMessage message) {
        switch (message.type()) {
            case INVALIDATE -> target.evictL1IfNewer(message.key(), message.version());
            case UPDATE -> target.applyUpdateL1(message.key(), message.payload(), message.version());
            case EVICT_ALL -> target.evictAllL1();
        }
    }

    private void notifyEvent(InvalidationMessage message, boolean replayed) {
        Object span = null;
        try { span = metrics.onInvalidationStart(message.cache()); }
        catch (Throwable e) { log.warn("Invalidation observer failed ({})", e.getClass().getSimpleName()); }
        observe(() -> metrics.onInvalidation(message.cache(), CacheMetricsListener.Direction.RECEIVED));
        observe(() -> eventListener.onEvent(message.cache(), message));
        Object handle = span;
        observe(() -> metrics.onInvalidationEnd(message.cache(), handle));
        if (replayed) observe(() -> metrics.onInvalidation(message.cache(), CacheMetricsListener.Direction.REPLAYED));
    }

    @Override public void setEventListener(InvalidationEventListener listener) {
        eventListener = listener == null ? InvalidationEventListener.NOOP : listener;
    }
    @Override public void onL2Recovery() { onReconnect(); }
    private void onReconnect() { requestAll(false); }

    @Override
    public CompletionStage<Boolean> recoverAsync(Executor ignored) { return requestAll(true); }

    private CompletionStage<Boolean> requestAll(boolean awaitResult) {
        CompletableFuture<Boolean> result;
        synchronized (lifecycle) {
            if (closed) return CompletableFuture.completedFuture(false);
            if (awaitResult && (aggregate == null || aggregate.isDone())) aggregate = new CompletableFuture<>();
            Set<CacheState> selected = new HashSet<>();
            for (CacheState state : states.values()) synchronized (state) {
                if (!state.ready) continue;
                selected.add(state);
                state.resetOnFailure = true;
                state.generation++;
                state.lastResult = null;
                state.replay = true;
                state.pending = true;
                schedule(state);
            }
            if (aggregate != null && !aggregate.isDone()) aggregateTargets = selected;
            result = aggregate;
        }
        checkAggregate();
        return result == null ? CompletableFuture.completedFuture(false) : result;
    }

    @Override
    public CompletionStage<RecoveryResult> resetAsync(String cache) {
        CacheState state = states.get(cache);
        if (state == null || closed) return CompletableFuture.completedFuture(RecoveryResult.closed());
        synchronized (state) {
            if (closed || !state.ready) return CompletableFuture.completedFuture(RecoveryResult.closed());
            if (state.resetCompletion == null || state.resetCompletion.isDone()) state.resetCompletion = new CompletableFuture<>();
            state.generation++;
            state.lastResult = null;
            state.reset = true;
            state.pending = true;
            schedule(state);
            return state.resetCompletion;
        }
    }

    // Caller owns the state gate. A running pass absorbs triggers; it alone
    // schedules its continuation. Thus at most one token exists per cache.
    private void schedule(CacheState state) {
        if (closed || state.retired || !state.ready || state.running || state.scheduled != null) return;
        long delay = state.retryAt == 0 ? 0 : Math.max(0, state.retryAt - clock.getAsLong());
        try { state.scheduled = workers().schedule(() -> run(state), delay, TimeUnit.NANOSECONDS); }
        catch (RejectedExecutionException e) { if (!closed) throw e; }
    }

    private void run(CacheState state) {
        boolean replay, reset, resetOnFailure;
        long generation;
        synchronized (state) {
            state.scheduled = null;
            if (closed || state.retired || !state.ready || state.running) return;
            state.running = true;
            state.token++;
            generation = state.generation;
            replay = state.replay || state.resyncRequired;
            reset = state.reset;
            resetOnFailure = state.resetOnFailure;
            state.replay = state.tick = state.reset = false;
        }
        Pass result;
        try { result = pass(state, replay, reset, resetOnFailure, generation); }
        catch (Throwable e) {
            log.warn("Recovery pass failed for cache '{}' ({})", state.cache, e.getClass().getSimpleName());
            result = new Pass(Kind.FAILED, null, generation);
        }
        CompletableFuture<RecoveryResult> resetWaiter = null;
        RecoveryResult completion = null;
        synchronized (state) {
            state.running = false;
            if (closed || state.retired) return;
            if (result.kind != Kind.OBSOLETE && (state.generation != result.generation
                    || (result.kind == Kind.RESET_SAFE && state.target.recoveryGeneration() != result.targetGeneration))) {
                result = obsolete(state);
            }
            if (result.kind == Kind.FAILED) {
                state.resyncRequired = true;
                state.applied.clear();
                state.replay = true;
                if (reset || result.targetGeneration >= 0) state.reset = true;
                state.failures = Math.min(6, state.failures + 1);
                long seconds = Math.min(30, 1L << (state.failures - 1));
                state.retryAt = clock.getAsLong() + TimeUnit.SECONDS.toNanos(seconds);
                completion = RecoveryResult.failed();
            } else if (result.kind != Kind.OBSOLETE) {
                if (result.kind == Kind.RESET_SAFE) {
                    // The baseline makes the clear safe, but a repeatedly
                    // unreadable history must not cause an immediate flush loop.
                    state.failures = Math.min(6, state.failures + 1);
                    state.retryAt = clock.getAsLong() + TimeUnit.SECONDS.toNanos(
                            Math.min(30, 1L << (state.failures - 1)));
                } else {
                    state.failures = 0;
                    state.retryAt = 0;
                }
                if (result.kind != Kind.TICK_DONE) {
                    completion = new RecoveryResult(switch (result.kind) {
                        case RESET_SAFE -> RecoveryResult.Status.RESET_SAFE;
                        case NO_JOURNAL -> RecoveryResult.Status.NO_JOURNAL;
                        default -> RecoveryResult.Status.CAUGHT_UP;
                    }, result.baseline, result.generation);
                }
            }
            // A newer request may have arrived after the final batch committed.
            // Do not settle its completion using this obsolete pass's result.
            if (completion != null && state.generation == result.generation) {
                state.lastResult = completion;
                if (completion.status() == RecoveryResult.Status.RESET_SAFE) {
                    state.safeResult = completion;
                    state.safeTargetGeneration = result.targetGeneration;
                }
                resetWaiter = state.resetCompletion;
                state.resetCompletion = null;
            } else completion = null;
            state.pending = state.replay || state.tick || state.reset || state.resyncRequired;
            if (state.pending) schedule(state);
        }
        if (resetWaiter != null) resetWaiter.complete(completion);
        checkAggregate();
    }

    private Snapshot snapshot(CacheState state) {
        return new Snapshot(state.generation, state.token, state.cursor,
                state.target.recoveryGeneration(), state.target, state.deliveries);
    }
    private boolean valid(CacheState state, Snapshot snapshot) {
        return !closed && !state.retired && state.generation == snapshot.generation
                && state.token == snapshot.token && Objects.equals(state.cursor, snapshot.cursor)
                && state.target == snapshot.target && state.target.recoveryGeneration() == snapshot.targetGeneration;
    }
    private Pass obsolete(CacheState state) {
        state.replay = true;
        if (state.resetCompletion != null) state.reset = true;
        return new Pass(Kind.OBSOLETE, null, -1);
    }

    private Pass pass(CacheState state, boolean replay, boolean reset, boolean resetOnFailure, long expectedGeneration) {
        if (reset || journal == null) return reset(state, expectedGeneration);
        for (int batch = 0; batch < (replay ? MAX_BATCHES : 1); batch++) {
            Snapshot snapshot;
            synchronized (state) {
                if (closed || state.retired) return new Pass(Kind.OBSOLETE, null, -1);
                if (state.generation != expectedGeneration) return obsolete(state);
                snapshot = snapshot(state);
            }
            CheckedRange range;
            try { range = journal.checkedRead(state.cache, snapshot.cursor, READ_BATCH); }
            catch (RuntimeException e) {
                synchronized (state) { if (!valid(state, snapshot)) return obsolete(state); }
                if (e instanceof JournalCorruptionException corrupt) {
                    boolean report;
                    synchronized (state) {
                        long now = clock.getAsLong();
                        report = state.nextCorruptionLog == 0 || now - state.nextCorruptionLog >= 0;
                        if (report) state.nextCorruptionLog = now + TimeUnit.SECONDS.toNanos(30);
                    }
                    observe(() -> metrics.onStreamFailure(state.cache, CacheMetricsListener.StreamResult.DECODE_FAILED));
                    if (report) observe(() -> log.warn("Journal corruption: cache={}, row={}, failure={}",
                            state.cache, corrupt.rowId(), corrupt.getClass().getSimpleName()));
                    return reset(state, expectedGeneration);
                }
                return resetOnFailure ? reset(state, expectedGeneration)
                        : new Pass(Kind.FAILED, null, expectedGeneration);
            }
            List<InvalidationMessage> notifications = new ArrayList<>();
            Pass done = null;
            synchronized (state) {
                if (!valid(state, snapshot)) return obsolete(state);
                if (!range.startIntact()) { /* baseline acquisition below, outside the gate */ }
                else {
                    List<JournalRow> rows = rowsAfterCursor(range, snapshot.cursor);
                    long targetGeneration = snapshot.targetGeneration;
                    for (JournalRow row : rows) {
                        InvalidationMessage message = row.message();
                        boolean own = message.originInstanceId().equals(originInstanceId);
                        if (!replay && !own && !state.applied.containsKey(message.version())) break;
                        if (replay && (!own || message.type() == InvalidationMessage.Type.EVICT_ALL)) {
                            long next = state.target.applyRecovery(message, targetGeneration);
                            if (next < 0 || state.target.recoveryGeneration() != next) return obsolete(state);
                            targetGeneration = next;
                            if (!own) notifications.add(message);
                            if (message.type() == InvalidationMessage.Type.EVICT_ALL) expectedGeneration = ++state.generation;
                        }
                        state.applied.remove(message.version());
                        state.confirmed.add(message.version());
                        while (state.confirmed.size() > CONFIRMED_CAP) state.confirmed.remove(state.confirmed.iterator().next());
                        state.cursor = row.cursor();
                    }
                    if (rows.isEmpty()) {
                        state.applied.values().removeIf(sequence -> sequence <= snapshot.deliverySequence);
                        state.resyncRequired = false;
                        state.resetOnFailure = false;
                        done = new Pass(replay ? Kind.CAUGHT_UP : Kind.TICK_DONE, state.cursor, state.generation);
                    } else if (!replay) done = new Pass(Kind.TICK_DONE, state.cursor, state.generation);
                }
            }
            if (!range.startIntact()) return reset(state, expectedGeneration);
            for (InvalidationMessage message : notifications) notifyEvent(message, true);
            if (done != null) return done;
        }
        synchronized (state) { return obsolete(state); } // bounded continuation, retaining cursor progress
    }

    private Pass reset(CacheState state, long expectedGeneration) {
        Snapshot snapshot;
        synchronized (state) {
            if (closed || state.retired) return new Pass(Kind.OBSOLETE, null, -1);
            if (state.generation != expectedGeneration) return obsolete(state);
            state.generation++;
            snapshot = snapshot(state);
        }
        String baseline = null;
        boolean safe = journal == null;
        if (journal != null) {
            try { baseline = journal.endCursor(state.cache); safe = baseline != null; }
            catch (RuntimeException e) { log.debug("Recovery baseline unavailable for '{}' ({})", state.cache, e.getClass().getSimpleName()); }
        }
        long generation;
        long targetGeneration;
        synchronized (state) {
            if (!valid(state, snapshot)) return obsolete(state);
            long next;
            try { next = state.target.resetRecovery(snapshot.targetGeneration); }
            catch (Throwable error) {
                // The reservation already advanced generation. Report failure
                // for that reservation, not the pass's obsolete starting epoch.
                return new Pass(Kind.FAILED, null, state.generation, snapshot.targetGeneration);
            }
            if (next < 0 || state.target.recoveryGeneration() != next) return obsolete(state);
            targetGeneration = next;
            generation = ++state.generation;
            if (safe && journal != null) state.cursor = baseline;
            state.applied.clear();
            state.confirmed.clear();
            state.resyncRequired = !safe;
            // A safe reset covers only the baseline. Follow with a bounded
            // continuation; rows appended after it must still be consumed.
            state.replay = journal != null;
        }
        boolean baselineEstablished = safe;
        if (journal == null) {
            observe(() -> log.warn("Recovery without a journal for cache '{}'; clearing L1 without claiming replay", state.cache));
        } else {
            observe(() -> log.warn("Conservative L1 reset for cache '{}'; baseline established: {}", state.cache, baselineEstablished));
            observe(() -> listener.onJournalOverflow(state.cache));
            observe(() -> metrics.onInvalidation(state.cache, CacheMetricsListener.Direction.DROPPED));
        }
        return new Pass(!safe ? Kind.FAILED : journal == null ? Kind.NO_JOURNAL : Kind.RESET_SAFE, baseline, generation, targetGeneration);
    }

    private void checkAggregate() {
        CompletableFuture<Boolean> completion = null;
        Boolean result = null;
        synchronized (lifecycle) {
            if (aggregate == null || aggregate.isDone()) return;
            boolean all = true;
            for (CacheState state : aggregateTargets) synchronized (state) {
                if (state.lastResult != null && !state.lastResult.succeeded()) { result = false; break; }
                if (state.lastResult == null) all = false;
            }
            if (result != null || all) {
                completion = aggregate;
                aggregate = null;
                aggregateTargets = Set.of();
                if (result == null) result = true;
            }
        }
        if (completion != null) completion.complete(result);
    }

    private static List<JournalRow> rowsAfterCursor(CheckedRange range, String cursor) {
        List<JournalRow> rows = range.rows();
        return !rows.isEmpty() && rows.get(0).cursor().equals(cursor) ? rows.subList(1, rows.size()) : rows;
    }

    private static void observe(Runnable callback) {
        try { callback.run(); }
        catch (Throwable e) { log.warn("Invalidation observer failed ({})", e.getClass().getSimpleName()); }
    }
    private static void closeQuietly(AutoCloseable resource) {
        if (resource != null) observe(() -> {
            try { resource.close(); } catch (Exception e) { throw new IllegalStateException(e); }
        });
    }

    @Override
    public void close() {
        List<AutoCloseable> resources = new ArrayList<>();
        List<CompletableFuture<RecoveryResult>> completions = new ArrayList<>();
        CompletableFuture<Boolean> recovery;
        synchronized (lifecycle) {
            if (closed) return;
            closed = true;
            recovery = aggregate;
            aggregate = null;
            aggregateTargets = Set.of();
            for (CacheState state : states.values()) synchronized (state) {
                state.retired = true;
                state.generation++;
                if (state.scheduled != null) state.scheduled.cancel(false);
                state.scheduled = null;
                if (state.resetCompletion != null) completions.add(state.resetCompletion);
                resources.add(state.subscription);
                resources.add(state.gauge);
            }
            states.clear();
        }
        if (recovery != null) recovery.complete(false);
        for (CompletableFuture<RecoveryResult> completion : completions) completion.complete(RecoveryResult.closed());
        resources.forEach(InvalidationService::closeQuietly);
        ScheduledExecutorService owned;
        synchronized (executorGate) { owned = ownsExecutor ? executor : null; }
        if (owned != null) owned.shutdownNow();
        closeQuietly(transport);
    }
}
