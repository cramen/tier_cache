package io.tiercache.jmh;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.infra.*;
import org.openjdk.jmh.results.*;
import java.lang.management.ManagementFactory;
import java.util.*;
/** Whole-process CPU, including harness overhead; never a request-latency metric. */
public final class ProcessCpuProfiler implements InternalProfiler {
    private final com.sun.management.OperatingSystemMXBean os=(com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
    private long before;
    public String getDescription(){return "Whole-process CPU nanoseconds per measured operation";}
    public void beforeIteration(BenchmarkParams benchmark,IterationParams iteration){before=os.getProcessCpuTime();}
    public Collection<? extends Result> afterIteration(BenchmarkParams benchmark,IterationParams iteration,IterationResult result){
        long elapsed=os.getProcessCpuTime()-before, operations=result.getMetadata().getAllOps();
        if(before<0 || elapsed<0 || operations<=0)throw new IllegalStateException("CPU measurement unavailable");
        return List.of(new ScalarResult("processCpu",(double)elapsed/operations,"ns/op",AggregationPolicy.AVG));
    }
}
