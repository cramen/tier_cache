package io.tiercache.jmh;

import io.tiercache.internal.CircuitBreaker;
import org.openjdk.jmh.annotations.*;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/** One factory-wide breaker, including serialized once-only completion. */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(3)
public class ClosedBreakerBenchmark {
    @Param({"healthy", "mixed", "open", "saturated"})
    public String workload;
    private CircuitBreaker breaker;
    private CircuitBreaker.Permit held;
    @State(Scope.Thread) public static class Caller { int sequence; }
    @Setup(Level.Trial) public void setup() {
        breaker = new CircuitBreaker(new CircuitBreaker.Config(128, 1, 128,
                workload.equals("saturated") ? Duration.ZERO : Duration.ofDays(1), 1),
                new CircuitBreaker.Listener() { public void onOpen() { } public void onClose() { } });
        if (workload.equals("open") || workload.equals("saturated")) {
            for (int i = 0; i < 128; i++) breaker.onFailure();
            if (workload.equals("saturated")) held = breaker.tryAcquirePermit();
        }
    }
    @TearDown(Level.Trial) public void cleanup() {
        if ((workload.equals("healthy") || workload.equals("mixed")) && breaker.state() != io.tiercache.BreakerState.CLOSED)
            throw new AssertionError("Healthy/mixed workload unexpectedly stopped admission");
        if (held != null) held.cancel();
    }
    @Benchmark public boolean guardedAttempt(Caller caller) {
        if (breaker.isOpen()) return false;
        var permit = breaker.tryAcquirePermit();
        if (permit == null) return false;
        if (workload.equals("mixed") && (++caller.sequence & 7) == 0) permit.failure();
        else permit.success();
        return true;
    }
}
