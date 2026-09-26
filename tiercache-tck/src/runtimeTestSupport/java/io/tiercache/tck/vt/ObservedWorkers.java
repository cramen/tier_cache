package io.tiercache.tck.vt;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

/** One deadline, checked futures and bounded shutdown. Never relies on executor.close(). */
public final class ObservedWorkers {
    public static void run(ExecutorService executor, int count, IntConsumer work,
            Duration deadline, Duration cleanup, Map<String,Object> report) throws Throwable {
        var futures=new ArrayList<Future<?>>(); var completed=new AtomicInteger(); Throwable failure=null;
        long start=System.nanoTime();
        try {
            if(count<=0 || deadline.isZero() || deadline.isNegative() || cleanup.isZero() || cleanup.isNegative())
                throw new IllegalArgumentException("Positive worker count and finite deadlines required");
            long budget=deadline.toNanos();
            for(int i=0;i<count;i++) {
                if(System.nanoTime()-start>=budget) throw new TimeoutException("Submission deadline");
                int id=i; futures.add(executor.submit(() -> { work.accept(id); completed.incrementAndGet(); }));
            }
            for(var future:futures) {
                long remaining=budget-(System.nanoTime()-start);
                if(remaining<=0) throw new TimeoutException("Worker deadline");
                try { future.get(remaining,TimeUnit.NANOSECONDS); }
                catch(ExecutionException e) { throw e.getCause(); }
            }
            if(completed.get()!=count) throw new AssertionError("Incomplete worker count");
        } catch(Throwable error) { failure=error; }
        finally {
            for(var future:futures) if(!future.isDone()) future.cancel(true);
            executor.shutdownNow();
            try {
                if(!executor.awaitTermination(cleanup.toNanos(),TimeUnit.NANOSECONDS))
                    throw new TimeoutException("Worker cleanup deadline");
            } catch(Throwable error) { if(failure==null) failure=error; else failure.addSuppressed(error); }
            // Even on failure, observe all completed futures rather than
            // silently losing Errors from workers later in submission order.
            int cancelled=0, failed=0;
            if(failure!=null) for(var future:futures) if(future.isDone()) {
                try { future.get(); }
                catch(CancellationException e) { cancelled++; }
                catch(ExecutionException e) {
                    failed++;
                    if(e.getCause()!=failure && failed<=20) failure.addSuppressed(e.getCause());
                } catch(InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            report.put("cancelled",cancelled); report.put("failedWorkers",failed);
            report.put("submitted",futures.size()); report.put("completed",completed.get());
            report.put("terminated",executor.isTerminated()); report.put("elapsedNanos",System.nanoTime()-start);
        }
        if(failure instanceof InterruptedException) Thread.currentThread().interrupt();
        if(failure!=null) throw failure;
    }
    private ObservedWorkers() { }
}
