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
    private static final ThreadLocal<Boolean> INTERNAL_WORKER = ThreadLocal.withInitial(() -> false);
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
    private final PublicationObserver publications;
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
        CompletableFuture<RecoveryResult> resetCompletion, localCompletion;
        RecoveryResult localProof;
        long localTargetGeneration;
        CompletableFuture<Void> registrationCompletion;
        boolean registrationQueued, registrationRunning, waitingFence;
        long fenceSequence;
        InvalidationDeliveryFence fence;
        CacheState(String cache) { this.cache = cache; }
    }

    private record Snapshot(long generation, long token, String cursor, long targetGeneration,
                            InvalidationTarget target, long deliverySequence) { }
    private enum Kind { WAITING, WAITING_REPLAY, TICK_DONE, CAUGHT_UP, RESET_SAFE, NO_JOURNAL, FAILED, OBSOLETE }
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
        this.publications = new PublicationObserver(this.metrics, clock);
        transport.setMetricsListener(this.metrics);
        transport.setGapHandler(new InvalidationGapHandler() {
            public CompletionStage<RecoveryResult> reset(String cache) { return resetAsync(cache); }
            public CompletionStage<RecoveryResult> recoverLocalGap(String cache) { return localGapAsync(cache); }
            public boolean isLocalRecoveryCurrent(String cache, RecoveryResult result) {
                CacheState state = states.get(cache);
                if (state == null) return false;
                synchronized (state) {
                    return !closed && !state.retired && state.ready && result != null && result.succeeded()
                            && result == state.localProof && state.generation == result.generation()
                            && state.target.recoveryGeneration() == state.localTargetGeneration;
                }
            }
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

    @Override public void checkRegistrationWaitAllowed() {
        if (transport.isDeliveryThread() || INTERNAL_WORKER.get()) {
            throw new IllegalStateException("Synchronous registration cannot wait on a delivery/recovery worker; use registerTargetAsync");
        }
    }

    @Override public void registerTarget(String cache, InvalidationTarget target) {
        checkRegistrationWaitAllowed();
        try { registerTarget(cache, target, true).toCompletableFuture().join(); }
        catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException cause) throw cause;
            if (failure.getCause() instanceof Error cause) throw cause;
            throw failure;
        }
    }

    @Override public CompletionStage<Void> registerTargetAsync(String cache, InvalidationTarget target) {
        return registerTarget(cache, target, false);
    }

    private CompletionStage<Void> registerTarget(String cache, InvalidationTarget target, boolean inline) {
        Objects.requireNonNull(target, "target");
        List<CompletableFuture<RecoveryResult>> retired = new ArrayList<>();
        CompletableFuture<Void> previous, result;
        CacheState state;
        boolean ready = false;
        synchronized (lifecycle) {
            if (closed) return CompletableFuture.failedFuture(new CancellationException("Invalidation service closed"));
            state = states.computeIfAbsent(cache, CacheState::new);
            publications.register(cache);
            synchronized (state) {
                previous = state.registrationCompletion;
                if (state.resetCompletion != null) retired.add(state.resetCompletion);
                if (state.localCompletion != null) retired.add(state.localCompletion);
                state.resetCompletion = state.localCompletion = null;
                state.localProof = state.lastResult = null;
                state.target = target;
                state.generation++;
                result = new CompletableFuture<>();
                if (state.ready && state.subscription != null && !transport.requiresRegistrationReset()) {
                    // Legacy transports keep their existing subscription/target-swap contract.
                    state.registrationCompletion = null;
                    ready = true;
                    if (state.pending) { state.replay = true; schedule(state); }
                } else {
                    state.registration++;
                    state.ready = false;
                    state.registrationCompletion = result;
                    if (!inline) scheduleRegistration(state);
                }
            }
        }
        if (previous != null) previous.completeExceptionally(new CancellationException("Registration superseded"));
        retired.forEach(future -> future.complete(RecoveryResult.closed()));
        if (ready) result.complete(null);
        else if (inline) {
            INTERNAL_WORKER.set(true);
            try { runRegistration(state); } finally { INTERNAL_WORKER.remove(); }
        }
        return result.minimalCompletionStage();
    }

    // One queued/running registration owner per cache, even during replacement churn.
    private void scheduleRegistration(CacheState state) {
        if (closed || state.retired || state.registrationQueued || state.registrationRunning
                || state.waitingFence || state.registrationCompletion == null || state.registrationCompletion.isDone()) return;
        state.registrationQueued = true;
        workers().execute(() -> {
            INTERNAL_WORKER.set(true);
            try { runRegistration(state); }
            finally { INTERNAL_WORKER.remove(); }
        });
    }

    private void runRegistration(CacheState state) {
        long registration;
        InvalidationTarget target;
        CompletableFuture<Void> result;
        synchronized (state) {
            state.registrationQueued = false;
            if (closed || state.retired || state.registrationRunning || state.registrationCompletion == null) return;
            state.registrationRunning = true;
            registration = state.registration;
            target = state.target;
            result = state.registrationCompletion;
        }
        AutoCloseable subscription = null, oldSubscription = null;
        InvalidationDeliveryFence release = null;
        Throwable failure = null;
        boolean completed = false;
        try {
            if (!ensureFence(state)) return;
            String baseline = journal == null ? "0-0" : journal.endCursor(state.cache);
            if (baseline == null) throw new IllegalStateException("Registration journal baseline unavailable for " + state.cache);
            synchronized (state) {
                if (closed || state.retired || state.registration != registration) return;
                if (transport.requiresRegistrationReset()) {
                    long next = target.resetRecovery(target.recoveryGeneration());
                    if (next < 0 || target.recoveryGeneration() != next) throw new IllegalStateException("Registration reset superseded");
                }
                state.cursor = baseline;
                state.safeTargetGeneration = target.recoveryGeneration();
                state.safeResult = journal == null ? null : new RecoveryResult(RecoveryResult.Status.RESET_SAFE, baseline, state.generation);
                state.applied.clear(); state.confirmed.clear();
                state.ready = true;
            }
            subscription = transport.subscribe(state.cache, message -> onMessage(message, registration));
            synchronized (state) {
                // A subscribe may itself replace the transport lane. Never reuse its old fence.
                release = state.fence; state.fence = null;
                if (!closed && !state.retired && state.registration == registration) {
                    oldSubscription = state.subscription;
                    state.subscription = subscription;
                    subscription = null;
                    completed = true;
                }
            }
            if (completed) registerRecoveryMetric(state);
        } catch (Throwable error) {
            failure = error;
            synchronized (state) {
                if (state.registration == registration) {
                    state.ready = false;
                    state.registrationCompletion = null;
                }
            }
        } finally {
            closeQuietly(subscription); closeQuietly(oldSubscription);
            if (release != null) release.release();
            synchronized (state) {
                state.registrationRunning = false;
                if (state.registration != registration || (!completed && failure == null && state.fence != null)) {
                    scheduleRegistration(state);
                }
                if (state.ready && state.pending) schedule(state);
            }
            if (failure != null) result.completeExceptionally(failure);
            else if (completed) result.complete(null);
            synchronized (state) {
                if (state.registrationCompletion == result && result.isDone()) state.registrationCompletion = null;
            }
        }
    }

    private void registerRecoveryMetric(CacheState state) {
        synchronized (state) {
            if (state.metricClaimed || closed) return;
            state.metricClaimed = true;
        }
        AutoCloseable gauge = null;
        try { gauge = metrics.registerRecovery(state.cache, () -> state.pending); }
        catch (Throwable e) { log.warn("Recovery metric registration failed ({})", e.getClass().getSimpleName()); }
        boolean discard;
        synchronized (state) { discard = closed; if (!discard) state.gauge = gauge; }
        if (discard) closeQuietly(gauge);
    }

    /** External fence callbacks retain neither target nor service strongly. */
    private record FenceCompletion(java.lang.ref.WeakReference<InvalidationService> owner,
                                   String cache, long sequence) {
        void complete(InvalidationDeliveryFence fence, Throwable error) {
            InvalidationService service = owner.get();
            if (service != null) service.fenceCompleted(cache, sequence, fence, error);
        }
    }

    private boolean ensureFence(CacheState state) {
        long sequence;
        synchronized (state) {
            if (state.fence != null) return true;
            if (closed || state.retired || state.waitingFence) return false;
            state.waitingFence = true;
            sequence = ++state.fenceSequence;
        }
        var completion = new FenceCompletion(new java.lang.ref.WeakReference<>(this), state.cache, sequence);
        try { transport.fenceDelivery(state.cache).whenComplete(completion::complete); }
        catch (Throwable error) { completion.complete(null, error); }
        synchronized (state) { return state.fence != null; }
    }

    private void fenceCompleted(String cache, long sequence, InvalidationDeliveryFence fence, Throwable error) {
        CacheState state = states.get(cache);
        if (state == null) return;
        CompletableFuture<Void> failed = null;
        synchronized (state) {
            if (closed || state.retired || state.fenceSequence != sequence) return;
            state.waitingFence = false;
            if (error == null && fence != null) state.fence = fence;
            else if (!state.ready && state.registrationCompletion != null) {
                failed = state.registrationCompletion; state.registrationCompletion = null;
            } else {
                state.reset = true; state.pending = true;
                state.retryAt = clock.getAsLong() + TimeUnit.SECONDS.toNanos(1);
            }
            if (!state.ready) scheduleRegistration(state);
            else schedule(state);
        }
        if (failed != null) failed.completeExceptionally(error == null
                ? new IllegalStateException("Missing delivery fence") : error);
    }

    @Override
    public void onLocalWrite(String cache, Object key, Version version, InvalidationMessage.Type type) {
        if (closed) return;
        InvalidationMessage message = new InvalidationMessage(cache, key, version, originInstanceId, type);
        PublicationObserver.Attempt attempt = publications.admit(cache);
        if (attempt == null) return;
        if (type == InvalidationMessage.Type.EVICT_ALL) {
            CacheState state = states.get(cache);
            if (state != null) synchronized (state) {
                state.generation++;
                if (state.pending) { state.replay = true; state.lastResult = null; schedule(state); }
            }
        }
        publish(message, attempt);
    }

    @Override
    public void onLocalUpdate(String cache, Object key, Object value, Version version) {
        if (closed) return;
        InvalidationMessage message = new InvalidationMessage(cache, key, version, originInstanceId,
                InvalidationMessage.Type.UPDATE, value);
        PublicationObserver.Attempt attempt = publications.admit(cache);
        if (attempt == null) return;
        publish(message, attempt);
    }

    private void publish(InvalidationMessage message, PublicationObserver.Attempt attempt) {
        observe(() -> metrics.onInvalidation(message.cache(), CacheMetricsListener.Direction.SENT));
        try {
            transport.publishAsync(message).whenComplete(attempt::complete);
        } catch (Throwable failure) {
            // Publication follows the data commit; a submission/serialization error
            // must not turn that successful write into a reported business failure.
            attempt.complete(null, failure);
        }
    }

    private void onMessage(InvalidationMessage message, long registration) {
        boolean nested = INTERNAL_WORKER.get();
        INTERNAL_WORKER.set(true);
        try { applyMessage(message, registration); }
        finally {
            if (nested) INTERNAL_WORKER.set(true);
            else INTERNAL_WORKER.remove();
        }
    }

    private void applyMessage(InvalidationMessage message, long registration) {
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
                if (++state.deliveries % JournalProtocol.CURSOR_CADENCE == 0) state.tick = true;
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

    private CompletionStage<RecoveryResult> localGapAsync(String cache) {
        CacheState state = states.get(cache);
        if (state == null || closed) return CompletableFuture.completedFuture(RecoveryResult.closed());
        synchronized (state) {
            if (closed || !state.ready) return CompletableFuture.completedFuture(RecoveryResult.closed());
            if (state.localCompletion != null) return state.localCompletion.minimalCompletionStage();
            state.localCompletion = new CompletableFuture<>();
            state.generation++;
            state.lastResult = state.localProof = null;
            state.replay = state.pending = state.resetOnFailure = true;
            schedule(state);
            return state.localCompletion.minimalCompletionStage();
        }
    }

    // Caller owns the state gate. A running pass absorbs triggers; it alone
    // schedules its continuation. Thus at most one token exists per cache.
    private void schedule(CacheState state) {
        if (closed || state.retired || !state.ready || state.running || state.registrationRunning || state.waitingFence || state.scheduled != null) return;
        long delay = state.retryAt == 0 ? 0 : Math.max(0, state.retryAt - clock.getAsLong());
        try { state.scheduled = workers().schedule(() -> {
            INTERNAL_WORKER.set(true);
            try { run(state); } finally { INTERNAL_WORKER.remove(); }
        }, delay, TimeUnit.NANOSECONDS); }
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
        CompletableFuture<RecoveryResult> resetWaiter = null, localWaiter = null;
        InvalidationDeliveryFence releaseFence = null;
        RecoveryResult completion = null;
        synchronized (state) {
            state.running = false;
            if (closed || state.retired) return;
            if (result.kind != Kind.OBSOLETE && (state.generation != result.generation
                    || (result.kind == Kind.RESET_SAFE && state.target.recoveryGeneration() != result.targetGeneration))) {
                result = obsolete(state);
            }
            if (result.kind == Kind.WAITING) {
                state.reset = true;
            } else if (result.kind == Kind.WAITING_REPLAY) {
                state.replay = true;
            } else if (result.kind == Kind.FAILED) {
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
                localWaiter = state.localCompletion;
                state.localCompletion = null;
                if (completion.succeeded()) {
                    state.localProof = completion;
                    state.localTargetGeneration = state.target.recoveryGeneration();
                    releaseFence = state.fence;
                    state.fence = null;
                }
            } else completion = null;
            state.pending = state.replay || state.tick || state.reset || state.resyncRequired;
            if (state.pending) schedule(state);
        }
        if (releaseFence != null) releaseFence.release();
        if (resetWaiter != null) resetWaiter.complete(completion);
        if (localWaiter != null) localWaiter.complete(completion);
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
            if (replay && range.startIntact()
                    && rowsAfterCursor(range, snapshot.cursor).stream().anyMatch(row -> row.message().type() == InvalidationMessage.Type.EVICT_ALL)
                    && !ensureFence(state)) {
                return new Pass(Kind.WAITING_REPLAY, null, expectedGeneration);
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
        if (!ensureFence(state)) return new Pass(Kind.WAITING, null, expectedGeneration);
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
        List<CompletableFuture<Void>> registrations = new ArrayList<>();
        CompletableFuture<Boolean> recovery;
        synchronized (lifecycle) {
            if (closed) return;
            closed = true;
            publications.stopAdmission();
            recovery = aggregate;
            aggregate = null;
            aggregateTargets = Set.of();
            for (CacheState state : states.values()) synchronized (state) {
                state.retired = true;
                state.generation++;
                if (state.scheduled != null) state.scheduled.cancel(false);
                state.scheduled = null;
                if (state.resetCompletion != null) completions.add(state.resetCompletion);
                if (state.localCompletion != null) completions.add(state.localCompletion);
                if (state.registrationCompletion != null) registrations.add(state.registrationCompletion);
                resources.add(state.subscription);
                resources.add(state.gauge);
            }
            states.clear();
        }
        registrations.forEach(future -> future.completeExceptionally(new CancellationException("Invalidation service closed")));
        if (recovery != null) recovery.complete(false);
        for (CompletableFuture<RecoveryResult> completion : completions) completion.complete(RecoveryResult.closed());
        resources.forEach(InvalidationService::closeQuietly);
        ScheduledExecutorService owned;
        synchronized (executorGate) { owned = ownsExecutor ? executor : null; }
        if (owned != null) owned.shutdownNow();
        publications.close();
        closeQuietly(transport);
    }
}
