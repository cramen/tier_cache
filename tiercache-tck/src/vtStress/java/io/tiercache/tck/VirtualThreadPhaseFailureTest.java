package io.tiercache.tck;
import io.tiercache.tck.vt.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
class VirtualThreadPhaseFailureTest {
    @TempDir Path output;
    @Test void workloadFailureRetainsRecordingAndOriginalCause() throws Exception {
        String old=System.getProperty("tiercache.vt.output");System.setProperty("tiercache.vt.output",output.toString());
        try {
            for(boolean cold:new boolean[]{true,false}) {
                var failure=new AssertionError("injected worker failure");
                assertSame(failure,assertThrows(AssertionError.class,()->VirtualThreadPhase.run(cold,()->
                    ObservedWorkers.run(Executors.newVirtualThreadPerTaskExecutor(),1,id->{throw failure;},Duration.ofSeconds(2),Duration.ofSeconds(1),new HashMap<>()))));
            }
            try(var paths=Files.walk(output)) {
                var summaries=paths.filter(p->p.getFileName().toString().equals("summary.json")).toList();assertEquals(2,summaries.size());
                for(var path:summaries) { assertTrue(Files.readString(path).contains("\"status\":\"FAILED\""));assertTrue(Files.size(path.resolveSibling("recording.jfr"))>0); }
            }
        } finally { if(old==null)System.clearProperty("tiercache.vt.output");else System.setProperty("tiercache.vt.output",old); }
    }
    @Test void runtimeMismatchCannotProducePassOrFakeRecording() throws Exception {
        String oldOutput=System.getProperty("tiercache.vt.output"),oldJdk=System.getProperty("tiercache.vt.requestedJdk");
        System.setProperty("tiercache.vt.output",output.toString());System.setProperty("tiercache.vt.requestedJdk","999");
        try {
            assertThrows(IllegalStateException.class,()->VirtualThreadPhase.run(true));
            try(var paths=Files.walk(output)) {
                var summary=paths.filter(p->p.getFileName().toString().equals("summary.json")).findFirst().orElseThrow();
                assertTrue(Files.readString(summary).contains("FAILED"));assertFalse(Files.exists(summary.resolveSibling("recording.jfr")));
            }
        } finally {
            if(oldOutput==null)System.clearProperty("tiercache.vt.output");else System.setProperty("tiercache.vt.output",oldOutput);
            if(oldJdk==null)System.clearProperty("tiercache.vt.requestedJdk");else System.setProperty("tiercache.vt.requestedJdk",oldJdk);
        }
    }
}
