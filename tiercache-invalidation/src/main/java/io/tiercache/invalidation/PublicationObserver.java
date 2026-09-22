package io.tiercache.invalidation;

import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.PublicationOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/** Bounded terminal accounting and coalesced publication diagnostics. */
final class PublicationObserver implements AutoCloseable {
    static final String OVERFLOW_CACHE = "__tiercache_unregistered__";
    private static final Logger log = LoggerFactory.getLogger(PublicationObserver.class);
    private static final long WARNING_INTERVAL = TimeUnit.SECONDS.toNanos(30);
    private static final PublicationOutcome[] OUTCOMES = PublicationOutcome.values();
    private final CacheMetricsListener metrics;
    private final LongSupplier clock;
    private final ReentrantLock gate = new ReentrantLock();
    private final Map<String, Bucket> caches = new HashMap<>();
    private final Bucket overflow = new Bucket(OVERFLOW_CACHE);
    private final ArrayDeque<Bucket> ready = new ArrayDeque<>();
    private final ThreadPoolExecutor worker;
    private volatile Thread workerThread;
    private boolean accepting = true, scheduling = true, notifying = true, scheduled, closing;

    private static final class Bucket {
        final String cache;
        final AtomicLongArray totals = new AtomicLongArray(OUTCOMES.length);
        final long[] exported = new long[OUTCOMES.length]; // worker-owned observation cursors
        final AtomicReference<String> failure = new AtomicReference<>();
        boolean queued;
        volatile long nextWarning;
        Bucket(String cache) { this.cache = cache; }
    }

    static final class Attempt {
        private final PublicationObserver owner;
        private final Bucket bucket;
        private final AtomicBoolean completed = new AtomicBoolean();
        Attempt(PublicationObserver owner, Bucket bucket) { this.owner = owner; this.bucket = bucket; }
        void complete(PublicationOutcome outcome, Throwable error) {
            if (!completed.compareAndSet(false, true)) return;
            PublicationOutcome terminal = error != null || outcome == null ? PublicationOutcome.FAILED : outcome;
            bucket.totals.incrementAndGet(terminal.ordinal());
            if (terminal == PublicationOutcome.FAILED) {
                Throwable cause = error;
                for (int depth = 0; depth < 8 && (cause instanceof CompletionException || cause instanceof ExecutionException)
                        && cause.getCause() != null; depth++) cause = cause.getCause();
                String failure = cause == null ? "PublicationFailure" : cause.getClass().getSimpleName();
                bucket.failure.set(failure.substring(0, Math.min(128, failure.length())));
            }
            owner.enqueue(bucket);
        }
        long count(PublicationOutcome outcome) { return bucket.totals.get(outcome.ordinal()); }
    }

    PublicationObserver(CacheMetricsListener metrics, LongSupplier clock) {
        this.metrics = metrics;
        this.clock = clock;
        worker = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1), runnable -> {
                    Thread thread = new Thread(runnable, "tiercache-publication-observer");
                    thread.setDaemon(true);
                    workerThread = thread;
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    void register(String cache) {
        gate.lock();
        try { if (accepting) caches.computeIfAbsent(cache, Bucket::new); }
        finally { gate.unlock(); }
    }

    Attempt admit(String cache) {
        gate.lock();
        try { return accepting ? new Attempt(this, caches.getOrDefault(cache, overflow)) : null; }
        finally { gate.unlock(); }
    }

    void stopAdmission() {
        gate.lock();
        try { accepting = false; }
        finally { gate.unlock(); }
    }

    private void enqueue(Bucket bucket) {
        gate.lock();
        try {
            if (!scheduling) return;
            if (!bucket.queued) { bucket.queued = true; ready.add(bucket); }
            if (!scheduled) {
                scheduled = true;
                worker.execute(this::drain);
            }
        } finally { gate.unlock(); }
    }

    private void drain() {
        while (true) {
            Bucket bucket;
            gate.lock();
            try {
                bucket = ready.poll();
                if (bucket == null || !notifying) { scheduled = false; return; }
                // Completion after this dequeue can enqueue exactly one next token.
                bucket.queued = false;
            } finally { gate.unlock(); }
            String failure = bucket.failure.getAndSet(null);
            if (failure != null && notificationAdmitted()) warn(bucket, failure);
            for (PublicationOutcome outcome : OUTCOMES) {
                int index = outcome.ordinal();
                long total = bucket.totals.get(index);
                long count = total - bucket.exported[index];
                bucket.exported[index] = total;
                if (count != 0 && notificationAdmitted()) {
                    try { metrics.onPublication(bucket.cache, outcome, count); }
                    catch (Throwable error) { warn(bucket, "Observer:" + error.getClass().getSimpleName()); }
                }
            }
        }
    }

    private boolean notificationAdmitted() {
        gate.lock();
        try { return notifying; }
        finally { gate.unlock(); }
    }

    private void warn(Bucket bucket, String failure) {
        long now = clock.getAsLong();
        if (bucket.nextWarning != 0 && now - bucket.nextWarning < 0) return;
        bucket.nextWarning = now + WARNING_INTERVAL;
        try { log.warn("Invalidation publication diagnostic: cache={}, failure={}", bucket.cache, failure); }
        catch (Throwable ignored) { /* Diagnostics cannot terminate the worker. */ }
    }

    /** Diagnostic seam: pending tokens cannot exceed registered caches plus one. */
    int pendingTokens() {
        gate.lock();
        try { return ready.size(); }
        finally { gate.unlock(); }
    }
    int bucketCount() {
        gate.lock();
        try { return caches.size() + 1; }
        finally { gate.unlock(); }
    }

    @Override public void close() {
        gate.lock();
        try {
            if (closing) return;
            closing = true;
            accepting = false;
            scheduling = false;
            worker.shutdown();
        } finally { gate.unlock(); }
        if (Thread.currentThread() != workerThread) {
            try { worker.awaitTermination(1, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        gate.lock();
        try { notifying = false; ready.clear(); }
        finally { gate.unlock(); }
        worker.shutdownNow();
    }
}
