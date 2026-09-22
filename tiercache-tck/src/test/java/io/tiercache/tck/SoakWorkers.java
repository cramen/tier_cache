package io.tiercache.tck;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Observes FutureTask failures as well as actual work and termination. */
final class SoakWorkers implements AutoCloseable {
    interface Work { void run(Worker worker) throws Exception; }
    final class Worker {
        final int id;
        final AtomicLong operations = new AtomicLong();
        volatile boolean started;
        volatile long startedNanos, finishedNanos;
        volatile String state = "NOT_STARTED";
        volatile Throwable failure, futureFailure;
        volatile boolean observed;
        Future<?> future;
        Worker(int id) { this.id = id; }
        boolean keepRunning() { return clock.getAsLong() - deadline < 0 && !Thread.currentThread().isInterrupted(); }
        void succeeded() { operations.incrementAndGet(); }
    }
    private final List<Worker> workers = new ArrayList<>();
    private final ExecutorService pool;
    private final LongSupplier clock;
    private final long deadline;
    SoakWorkers(int count, long deadline, LongSupplier clock, Work work) {
        this.deadline = deadline;
        this.clock = clock;
        pool = Executors.newFixedThreadPool(count, runnable -> {
            Thread thread = new Thread(runnable, "tiercache-soak-worker");
            thread.setDaemon(true); // an uninterruptible failed worker cannot prevent the test JVM exiting
            return thread;
        });
        try {
            for (int i = 0; i < count; i++) {
                Worker worker = new Worker(i);
                workers.add(worker);
                worker.future = pool.submit(() -> {
                    worker.started = true;
                    worker.startedNanos = clock.getAsLong();
                    worker.state = "RUNNING";
                    try {
                        work.run(worker);
                        if (Thread.currentThread().isInterrupted()) throw new CancellationException("Worker interrupted before normal completion");
                        if (clock.getAsLong() - deadline < 0) throw new AssertionError("Worker ended prematurely before the workload deadline");
                        if (worker.operations.get() == 0) throw new AssertionError("Worker completed no successful operations");
                        worker.state = "COMPLETED";
                    } catch (Throwable failure) {
                        worker.failure = failure;
                        worker.state = "FAILED";
                        if (failure instanceof Error error) throw error;
                        if (failure instanceof Exception exception) throw exception;
                        throw new AssertionError(failure);
                    } finally { worker.finishedNanos = clock.getAsLong(); }
                    return null;
                });
            }
            pool.shutdown();
        } catch (RuntimeException | Error failure) { close(); throw failure; }
    }
    void checkProgress() {
        List<Throwable> failures = new ArrayList<>();
        for (Worker worker : workers) if (worker.future.isDone()) inspect(worker, failures);
        requireHealthy(failures);
    }
    void awaitHealthy(Duration timeout) throws InterruptedException {
        List<Throwable> failures = new ArrayList<>();
        if (!pool.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
            failures.add(new TimeoutException("Soak workers did not terminate within " + timeout));
            for (Worker worker : workers) if (!worker.future.isDone()) worker.future.cancel(true);
            pool.shutdownNow();
        }
        // Includes cancelled futures; none may disappear merely because the pool terminated.
        for (Worker worker : workers) inspect(worker, failures);
        requireHealthy(failures);
    }
    private void inspect(Worker worker, List<Throwable> failures) {
        try {
            worker.future.get();
            if (!worker.started || worker.operations.get() == 0 || !"COMPLETED".equals(worker.state)) {
                failures.add(new AssertionError("Incomplete soak worker " + worker.id + ": " + worker.state));
            }
        } catch (ExecutionException e) { worker.futureFailure = e.getCause(); failures.add(e.getCause()); }
        catch (CancellationException e) { worker.futureFailure = e; failures.add(e); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); failures.add(e); }
        finally { worker.observed = true; }
    }
    private static void requireHealthy(List<Throwable> failures) {
        if (failures.isEmpty()) return;
        AssertionError failure = new AssertionError("Incomplete or failed soak workload", failures.get(0));
        for (int i = 1; i < failures.size(); i++) failure.addSuppressed(failures.get(i));
        throw failure;
    }
    long operations() { return workers.stream().mapToLong(worker -> worker.operations.get()).sum(); }
    List<Map<String, Object>> snapshots() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Worker worker : workers) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", worker.id); row.put("started", worker.started); row.put("operations", worker.operations.get());
            row.put("state", worker.state); row.put("startedNanos", worker.startedNanos); row.put("finishedNanos", worker.finishedNanos);
            row.put("futureDone", worker.future != null && worker.future.isDone());
            row.put("futureCancelled", worker.future != null && worker.future.isCancelled());
            row.put("futureObserved", worker.observed);
            Throwable failure = worker.failure != null ? worker.failure : worker.futureFailure;
            row.put("failure", failure == null ? null : failure.toString());
            result.add(row);
        }
        return result;
    }
    // Test seam for an externally cancelled workload.
    void cancel(int worker) { workers.get(worker).future.cancel(true); }
    boolean terminated() { return pool.isTerminated(); }
    @Override public void close() {
        for (Worker worker : workers) if (worker.future != null && !worker.future.isDone()) worker.future.cancel(true);
        pool.shutdownNow();
        try { pool.awaitTermination(1, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        List<Throwable> observed = new ArrayList<>();
        for (Worker worker : workers) if (worker.future != null) inspect(worker, observed);
    }
}
