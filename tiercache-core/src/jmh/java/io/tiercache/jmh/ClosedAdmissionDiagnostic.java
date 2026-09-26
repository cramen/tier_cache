package io.tiercache.jmh;
import io.tiercache.internal.CircuitBreaker;
import org.openjdk.jmh.annotations.*;
import java.util.concurrent.TimeUnit;
/** Diagnostic compatibility admission only: CLOSED creates no reservation to clean up. */
@State(Scope.Benchmark) @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations=3,time=2) @Measurement(iterations=3,time=2) @Fork(3)
public class ClosedAdmissionDiagnostic {
    private CircuitBreaker breaker;
    @Setup public void setup() {
        breaker=new CircuitBreaker(CircuitBreaker.Config.defaults(),new CircuitBreaker.Listener(){public void onOpen(){}public void onClose(){}});
    }
    @Benchmark public boolean admit() { return breaker.tryAcquire(); }
    @TearDown public void verify() { if(breaker.state()!=io.tiercache.BreakerState.CLOSED)throw new AssertionError("diagnostic left CLOSED"); }
}
