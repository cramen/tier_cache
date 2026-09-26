import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
/** Streaming attribution; sums of blocked waits are not request latency. */
class BreakerJfrSummary {
    public static void main(String[] args) throws Exception {
        long waits=0,totalWait=0,maxWait=0,samples=0,breakerSamples=0,accountingSamples=0;
        try(var file=new RecordingFile(Path.of(args[0]))) {
            while(file.hasMoreEvents()) {
                var event=file.readEvent();String name=event.getEventType().getName();
                if(name.equals("jdk.JavaMonitorEnter") && event.getClass("monitorClass").getName().replace('/','.').equals("io.tiercache.internal.CircuitBreaker")) {
                    long duration=event.getDuration().toNanos();waits++;totalWait+=duration;maxWait=Math.max(maxWait,duration);
                }
                if(name.equals("jdk.ExecutionSample")) {
                    samples++;var stack=event.getStackTrace();if(stack==null)continue;
                    boolean breaker=false,accounting=false;
                    for(var frame:stack.getFrames()) {
                        String type=frame.getMethod().getType().getName().replace('/','.'),method=frame.getMethod().getName();
                        if(type.startsWith("io.tiercache.internal.CircuitBreaker")) {
                            breaker=true;
                            if(Set.of("complete","record","successLocked","failureLocked").contains(method))accounting=true;
                        }
                    }
                    if(breaker)breakerSamples++;if(accounting)accountingSamples++;
                }
            }
        }
        System.out.printf(Locale.ROOT,"{\"monitorEntries\":%d,\"aggregateWaitNanosAcrossThreads\":%d,\"maxWaitNanos\":%d,\"executionSamples\":%d,\"breakerFrameSamples\":%d,\"accountingFrameSamples\":%d}%n",
                waits,totalWait,maxWait,samples,breakerSamples,accountingSamples);
    }
}
