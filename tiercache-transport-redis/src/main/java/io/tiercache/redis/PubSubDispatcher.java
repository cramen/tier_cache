package io.tiercache.redis;

import io.tiercache.spi.*;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Bounded payload ownership and ordered, fairly scheduled registration lanes. */
final class PubSubDispatcher implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(PubSubDispatcher.class);
    private static final int QUANTUM = 32;
    private static final long RETRY_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private final Object gate = new Object();
    private final PubSubDispatchOptions options;
    private final Map<String, Lane> lanes = new LinkedHashMap<>();
    private final ArrayDeque<Lane> ready = new ArrayDeque<>();
    private final Set<String> activeCaches = new HashSet<>();
    private final List<Thread> workers = new ArrayList<>();
    private final AtomicInteger controls = new AtomicInteger();
    private long sequence, retainedBytes;
    private int retainedMessages;
    private boolean closed, diagnostics;
    private long nextDiagnostics;
    private volatile InvalidationGapHandler recovery;
    private volatile CacheMetricsListener metrics = CacheMetricsListener.NOOP;
    private AutoCloseable gauge;

    private static final class Lane {
        final String cache;
        final long generation;
        final Consumer<byte[]> handler;
        final ArrayDeque<byte[]> queue = new ArrayDeque<>();
        boolean running, scheduled, retired, pending, repairing, fenceOnly;
        long lossEpoch, retryAt, fenceEpoch;
        Control control;
        CompletableFuture<InvalidationDeliveryFence> fence;
        boolean fenceGranted, fenceNotifying;
        RecoveryResult result;
        AutoCloseable gauge;
        final long[] rejected = new long[CacheMetricsListener.DispatchReason.values().length];
        long nextLog;
        String reason = "delivery-gap";
        long repairEpoch;
        Lane(String cache, long generation, Consumer<byte[]> handler) {
            this.cache = cache; this.generation = generation; this.handler = handler;
        }
    }

    /** External callbacks retain only this credit and a weak dispatcher token. */
    private static final class Control {
        final AtomicInteger budget;
        int owners = 1; // lane ownership; each actual external attachment adds one
        Control(AtomicInteger budget) { this.budget = budget; }
        synchronized void retain() { owners++; }
        synchronized void retire() { if (--owners == 0) budget.decrementAndGet(); }
    }

    private record Token(WeakReference<PubSubDispatcher> dispatcher, String cache, long generation) {
        void completed(RecoveryResult result, Throwable error) {
            PubSubDispatcher owner = dispatcher.get();
            if (owner != null) owner.repairCompleted(this, error == null ? result : RecoveryResult.failed());
        }
        void releaseFence(long epoch) {
            PubSubDispatcher owner = dispatcher.get();
            if (owner != null) owner.releaseFence(this, epoch);
        }
    }

    PubSubDispatcher(PubSubDispatchOptions options) {
        this.options = options;
        for (int i = 0; i < options.dispatchThreads(); i++) {
            Thread worker = new Thread(this::work, "tiercache-invalidation-" + i);
            worker.setDaemon(true); workers.add(worker); worker.start();
        }
    }

    boolean isDeliveryThread() { return workers.contains(Thread.currentThread()); }

    void metrics(CacheMetricsListener listener) {
        metrics = listener;
        AutoCloseable registration = null;
        try { registration = ownedMetric(listener.registerDispatch(() -> retainedMessages(), this::retainedBytes)); }
        catch (Throwable ignored) { }
        AutoCloseable old;
        synchronized (gate) { old = gauge; gauge = closed ? null : registration; }
        closeMetric(old);
        if (closed) closeMetric(registration);
    }

    void recovery(InvalidationGapHandler handler) { recovery = handler; }

    AutoCloseable register(String cache, Consumer<byte[]> handler) {
        Lane old, lane;
        synchronized (gate) {
            if (closed) throw new IllegalStateException("Pub/Sub dispatcher closed");
            lane = new Lane(cache, ++sequence, handler);
            old = lanes.put(cache, lane);
            if (old != null) retire(old);
        }
        cancelFence(old);
        if (old != null) observeLosses(old);
        closeMetric(old == null ? null : old.gauge);
        AutoCloseable registration = null;
        try { registration = ownedMetric(metrics.registerDispatchPending(cache, () -> pending(cache))); }
        catch (Throwable ignored) { }
        boolean discard;
        synchronized (gate) { discard = lane.retired || closed; if (!discard) lane.gauge = registration; }
        if (discard) closeMetric(registration);
        return () -> {
            synchronized (gate) {
                if (lanes.remove(cache, lane)) retire(lane);
            }
            cancelFence(lane); closeMetric(lane.gauge);
        };
    }

    void accept(String cache, byte[] encoded) {
        synchronized (gate) {
            Lane lane = lanes.get(cache);
            if (closed || lane == null) return;
            if (lane.pending || retainedMessages >= options.maxPendingMessages()
                    || encoded.length > options.maxPendingBytes() - retainedBytes) {
                CacheMetricsListener.DispatchReason reason = lane.pending ? CacheMetricsListener.DispatchReason.PENDING
                        : encoded.length > options.maxPendingBytes() - retainedBytes ? CacheMetricsListener.DispatchReason.BYTES : CacheMetricsListener.DispatchReason.COUNT;
                lane.rejected[reason.ordinal()]++;
                if (!lane.pending) lane.reason = reason.name().toLowerCase(java.util.Locale.ROOT);
                gap(lane); return;
            }
            retainedMessages++; retainedBytes += encoded.length;
            lane.queue.addLast(encoded);
            enqueue(lane);
        }
    }

    void gap(String cache) {
        synchronized (gate) {
            if (closed) return;
            if (cache == null) lanes.values().forEach(lane -> {
                lane.rejected[CacheMetricsListener.DispatchReason.ROUTING.ordinal()]++; lane.reason = "routing"; gap(lane);
            });
            else { Lane lane = lanes.get(cache); if (lane != null) gap(lane); }
        }
    }

    private void gap(Lane lane) {
        lane.pending = true; lane.lossEpoch++;
        diagnostics = true; gate.notifyAll();
        purge(lane); enqueue(lane);
    }

    CompletionStage<InvalidationDeliveryFence> fence(String cache) {
        CompletableFuture<InvalidationDeliveryFence> result;
        synchronized (gate) {
            Lane lane = lanes.get(cache);
            if (closed) return CompletableFuture.failedFuture(new CancellationException("Dispatcher closed"));
            if (lane == null) {
                if (!activeCaches.contains(cache)) return CompletableFuture.completedFuture(InvalidationDeliveryFence.NOOP);
                // Unsubscribe retires queued work, not an already admitted handler.
                // Keep a temporary fence lane until that old owner relinquishes the cache.
                lane = new Lane(cache, ++sequence, bytes -> { });
                lane.fenceOnly = true;
                if (!control(lane)) return CompletableFuture.failedFuture(
                        new RejectedExecutionException("Pub/Sub control capacity exhausted"));
                lanes.put(cache, lane);
            }
            if (lane.fence == null) {
                if (!control(lane)) {
                    lane.reason = "control-capacity"; gap(lane);
                    return CompletableFuture.failedFuture(new RejectedExecutionException("Pub/Sub control capacity exhausted"));
                }
                lane.fence = new CompletableFuture<>();
                lane.fenceGranted = false;
                lane.reason = "recovery-fence";
                gap(lane);
                lane.fenceEpoch = lane.lossEpoch;
            }
            result = lane.fence;
            enqueue(lane);
        }
        return result;
    }

    static boolean due(long now, long deadline) {
        return deadline == 0 || now - deadline >= 0;
    }

    private boolean control(Lane lane) {
        if (lane.control != null) return true;
        if (controls.get() >= options.maxPendingMessages()) return false;
        controls.incrementAndGet(); lane.control = new Control(controls); return true;
    }

    private void enqueue(Lane lane) {
        if (lane.retired || lane.running || lane.scheduled || activeCaches.contains(lane.cache)) return;
        lane.scheduled = true; ready.addLast(lane); gate.notifyAll();
    }

    private void work() {
        try {
            for (;;) {
                Lane lane;
                synchronized (gate) {
                    for (;;) {
                        if (closed) return;
                        long now = System.nanoTime();
                        if (diagnostics && (nextDiagnostics == 0 || now - nextDiagnostics >= 0)) {
                            diagnostics = false; nextDiagnostics = now + RETRY_NANOS; lane = null; break;
                        }
                        for (Lane candidate : lanes.values()) {
                            if (candidate.pending && !candidate.repairing && due(now, candidate.retryAt)
                                    && (candidate.fence == null || !candidate.fenceGranted)) enqueue(candidate);
                        }
                        lane = ready.pollFirst();
                        if (lane != null) {
                            lane.scheduled = false;
                            if (activeCaches.contains(lane.cache)) { gate.wait(100); continue; }
                            activeCaches.add(lane.cache); lane.running = true; break;
                        }
                        gate.wait(100);
                    }
                }
                if (lane == null) {
                    List<Lane> observed;
                    synchronized (gate) { observed = new ArrayList<>(lanes.values()); }
                    observed.forEach(this::observeLosses);
                    continue;
                }
                try { dispatch(lane); }
                catch (Throwable failure) {
                    synchronized (gate) {
                        if (!lane.retired) { lane.retryAt = System.nanoTime() + RETRY_NANOS; gap(lane); }
                    }
                }
                finally {
                    synchronized (gate) {
                        lane.running = false;
                        activeCaches.remove(lane.cache);
                        if (!lane.retired && ((!lane.pending && !lane.queue.isEmpty())
                                || (lane.fence != null && !lane.fenceGranted
                                    && due(System.nanoTime(), lane.retryAt)))) enqueue(lane);
                        Lane current = lanes.get(lane.cache);
                        if (current != null && current != lane) enqueue(current);
                    }
                }
            }
        } catch (InterruptedException stopped) { Thread.currentThread().interrupt(); }
    }

    private void dispatch(Lane lane) {
        for (int processed = 0; processed < QUANTUM; processed++) {
            byte[] bytes;
            synchronized (gate) {
                if (lane.retired || closed) return;
                if (lane.pending) { bytes = null; }
                else bytes = lane.queue.pollFirst();
            }
            if (bytes == null) { repair(lane); return; }
            try { lane.handler.accept(bytes); }
            catch (Throwable failure) {
                synchronized (gate) {
                    if (!lane.retired) {
                        lane.reason = failure instanceof DecodeFailure ? "decode" : "apply";
                        lane.rejected[(failure instanceof DecodeFailure ? CacheMetricsListener.DispatchReason.DECODE
                                : CacheMetricsListener.DispatchReason.APPLY).ordinal()]++;
                        gap(lane);
                    }
                }
            }
            finally { synchronized (gate) { release(bytes); } }
        }
    }

    private void repair(Lane lane) {
        observeLosses(lane);
        CompletableFuture<InvalidationDeliveryFence> fence = null;
        InvalidationDeliveryFence permit = null;
        Control notification = null;
        RecoveryResult result;
        InvalidationGapHandler handler = recovery;
        synchronized (gate) {
            if (!lane.pending || lane.retired || closed) return;
            if (!control(lane)) { lane.reason = "control-capacity"; lane.retryAt = System.nanoTime() + RETRY_NANOS; return; }
            if (lane.fence != null && !lane.fenceGranted) {
                lane.fenceGranted = true;
                lane.fenceNotifying = true;
                notification = lane.control; notification.retain();
                fence = lane.fence;
                long epoch = lane.fenceEpoch;
                Token token = token(lane);
                permit = () -> token.releaseFence(epoch);
            }
            result = lane.result;
            lane.result = null;
        }
        if (fence != null) {
            try { fence.complete(permit); }
            finally {
                synchronized (gate) {
                    lane.fenceNotifying = false;
                    if (!lane.pending && !lane.repairing && !lane.retired) releaseControl(lane);
                }
                notification.retire();
            }
            return;
        }
        if (result != null) {
            observe(() -> metrics.onDispatchRepair(lane.cache, result.status()));
            boolean valid = handler != null && result.succeeded()
                    && handler.isLocalRecoveryCurrent(lane.cache, result);
            synchronized (gate) {
                if (lane.retired || closed) return;
                if (valid && lane.repairEpoch == lane.lossEpoch && lane.fence == null) {
                    lane.pending = false;
                    releaseControl(lane);
                    return;
                }
                lane.retryAt = System.nanoTime() + RETRY_NANOS;
            }
            return;
        }
        Control credit;
        Token token;
        synchronized (gate) {
            if (lane.retired || lane.repairing || lane.fence != null || closed) return;
            if (handler == null || !due(System.nanoTime(), lane.retryAt)) {
                if (handler == null) lane.reason = "no-recovery-handler";
                lane.retryAt = System.nanoTime() + RETRY_NANOS; return;
            }
            lane.repairing = true; lane.repairEpoch = lane.lossEpoch;
            credit = lane.control; credit.retain(); token = token(lane);
        }
        CompletionStage<RecoveryResult> stage;
        try { stage = Objects.requireNonNull(handler.recoverLocalGap(lane.cache)); }
        catch (Throwable error) {
            try { token.completed(RecoveryResult.failed(), error); }
            finally { credit.retire(); }
            return;
        }
        var finished = new java.util.concurrent.atomic.AtomicBoolean();
        try {
            stage.whenComplete((repaired, error) -> {
                if (!finished.compareAndSet(false, true)) return;
                try { token.completed(repaired, error); }
                finally { credit.retire(); }
            });
        } catch (Throwable registrationFailure) {
            // A custom stage may have retained the callback before throwing.
            // Keep its ownership until that callback actually runs; never
            // attach an unbounded series of retries to an unknowable stage.
            observe(() -> metrics.onDispatchRepair(lane.cache, RecoveryResult.Status.FAILED));
        }
    }

    private Token token(Lane lane) { return new Token(new WeakReference<>(this), lane.cache, lane.generation); }

    private void repairCompleted(Token token, RecoveryResult result) {
        synchronized (gate) {
            Lane lane = lanes.get(token.cache);
            if (closed || lane == null || lane.generation != token.generation) return;
            lane.repairing = false; lane.result = result == null ? RecoveryResult.failed() : result;
            enqueue(lane);
        }
    }

    private void releaseFence(Token token, long epoch) {
        synchronized (gate) {
            Lane lane = lanes.get(token.cache);
            if (closed || lane == null || lane.generation != token.generation) return;
            if (lane.fence == null || lane.fenceEpoch != epoch) return;
            lane.fence = null; lane.fenceGranted = false;
            if (lane.fenceOnly) {
                lanes.remove(lane.cache, lane);
                retire(lane);
                releaseControl(lane);
                return;
            }
            if (!lane.repairing && lane.lossEpoch == epoch) {
                lane.pending = false;
                if (!lane.fenceNotifying) releaseControl(lane);
            }
            enqueue(lane);
        }
    }

    private void releaseControl(Lane lane) {
        if (lane.control != null) { lane.control.retire(); lane.control = null; }
    }
    private void release(byte[] bytes) { retainedMessages--; retainedBytes -= bytes.length; }
    private void purge(Lane lane) { byte[] bytes; while ((bytes = lane.queue.pollFirst()) != null) release(bytes); }
    private void retire(Lane lane) {
        lane.retired = true; ready.remove(lane); lane.scheduled = false; purge(lane);
    }
    private void cancelFence(Lane lane) {
        if (lane == null) return;
        Control notification;
        CompletableFuture<InvalidationDeliveryFence> fence;
        synchronized (gate) {
            notification = lane.control; fence = lane.fence;
            if (notification != null) notification.retain();
        }
        try {
            if (fence != null) fence.completeExceptionally(new CancellationException("Registration retired"));
        } finally {
            synchronized (gate) { releaseControl(lane); }
            if (notification != null) notification.retire();
        }
    }
    int retainedMessages() { synchronized (gate) { return retainedMessages; } }
    long retainedBytes() { synchronized (gate) { return retainedBytes; } }
    int controlGroups() { return controls.get(); }
    boolean pending(String cache) { synchronized (gate) { Lane lane = lanes.get(cache); return lane != null && lane.pending; } }

    static final class DecodeFailure extends RuntimeException {
        DecodeFailure(Throwable cause) { super("Invalid Pub/Sub frame", cause); }
    }
    private static void observe(Runnable callback) { try { callback.run(); } catch (Throwable ignored) { } }
    private static AutoCloseable ownedMetric(AutoCloseable handle) {
        var released = new java.util.concurrent.atomic.AtomicBoolean();
        return () -> { if (handle != null && released.compareAndSet(false, true)) handle.close(); };
    }
    private static void closeMetric(AutoCloseable handle) {
        if (handle != null) { try { handle.close(); } catch (Throwable ignored) { } }
    }
    private void observeLosses(Lane lane) {
        long[] lost;
        boolean log;
        String diagnosticReason;
        synchronized (gate) {
            lost = lane.rejected.clone(); Arrays.fill(lane.rejected, 0);
            diagnosticReason = lane.reason;
            long now = System.nanoTime();
            log = lane.pending && !lane.retired && (lane.nextLog == 0 || now - lane.nextLog >= 0);
            if (log) lane.nextLog = now + TimeUnit.SECONDS.toNanos(30);
        }
        for (var reason : CacheMetricsListener.DispatchReason.values()) {
            long count = lost[reason.ordinal()];
            if (count != 0) observe(() -> metrics.onDispatchRejected(reason, count));
        }
        if (log) observe(() -> LOG.warn("Pub/Sub delivery pending repair for cache '{}': {}; retained messages={}, encoded bytes={}",
                lane.cache, diagnosticReason, retainedMessages(), retainedBytes()));
    }

    @Override public void close() {
        List<Lane> retired;
        synchronized (gate) {
            if (closed) return;
            closed = true; retired = new ArrayList<>(lanes.values());
            retired.forEach(this::retire); lanes.clear(); gate.notifyAll();
        }
        retired.forEach(this::cancelFence);
        retired.forEach(lane -> { observeLosses(lane); closeMetric(lane.gauge); });
        closeMetric(gauge);
        workers.forEach(Thread::interrupt);
    }
}
