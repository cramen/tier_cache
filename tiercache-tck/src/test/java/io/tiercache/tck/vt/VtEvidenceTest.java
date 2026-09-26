package io.tiercache.tck.vt;
import jdk.jfr.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class VtEvidenceTest {
    @Name("tiercache.FixturePin") static class Pin extends Event { }
    @Name("tiercache.FixtureReasonPin") static class ReasonPin extends Event {
        public String pinnedReason="initialization";
        public String blockingOperation="monitor enter";
    }
    @TempDir Path dir;
    @Test void attributionAndPolicyDoNotWhitelistReasons() {
        assertEquals("product",VtEvidence.category(List.of("io.tiercache.internal.DefaultTierCache.get")));
        assertEquals("product",VtEvidence.category(List.of("io/tiercache/internal/caffeine/cache/X.get")));
        assertEquals("testOnly",VtEvidence.category(List.of("io.tiercache.tck.Work.run","io.tiercache.testkit.Remote.get")));
        assertEquals("product",VtEvidence.category(List.of("io.tiercache.tck.Work.run","io.tiercache.redis.X.get")));
        assertEquals("jdkOrOther",VtEvidence.category(List.of("java.lang.Thread.run")));
        assertThrows(IllegalStateException.class,()->VtEvidence.category(List.of()));
        assertThrows(IllegalStateException.class,()->VtEvidence.category(null));
        var positive=Map.<String,Object>of("product",1L,"reason","class initialization","duration",0);
        assertEquals("DIAGNOSTIC_COMPLETE",VtEvidence.outcome(true,positive));
        assertThrows(AssertionError.class,()->VtEvidence.outcome(false,positive));
        assertEquals("PASS",VtEvidence.outcome(false,Map.of("product",0)));
    }
    @Test void settingsMustBeEnabledZeroThresholdAndStackBearing() {
        var valid=Map.of("pin#enabled","true","pin#threshold","0 ns","pin#stackTrace","true");
        VtEvidence.settings(valid,"pin");
        for(String key:valid.keySet()) {
            var broken=new HashMap<>(valid);broken.remove(key);
            assertThrows(IllegalStateException.class,()->VtEvidence.settings(broken,"pin"));
        }
        for(String[] wrong:new String[][]{{"enabled","false"},{"threshold","20 ms"},{"stackTrace","false"}}) {
            var broken=new HashMap<>(valid);broken.put("pin#"+wrong[0],wrong[1]);
            assertThrows(IllegalStateException.class,()->VtEvidence.settings(broken,"pin"));
        }
    }
    Path record(String run, boolean marker, boolean stacks, boolean event) throws Exception {
        Path path=dir.resolve(UUID.randomUUID()+".jfr");
        try(var recording=new Recording()) {
            recording.enable(Pin.class).withThreshold(Duration.ZERO).with("stackTrace",Boolean.toString(stacks));
            recording.enable(VtEvidence.Proof.class);recording.setDestination(path);recording.start();
            if(event) { var pin=new Pin();pin.begin();pin.end();pin.commit(); }
            if(marker) { var proof=new VtEvidence.Proof();proof.runId=run;proof.commit(); }
            recording.stop();
        }
        return path;
    }
    @Test void realRecordingRetainsPositiveEventsAndUnavailableOptionalFields() throws Exception {
        var evidence=VtEvidence.read(record("r",true,true,true),"r","tiercache.FixturePin");
        assertEquals(1L,evidence.get("total"));assertEquals(1L,evidence.get("testOnly"));
        assertEquals(Map.of("unavailable",1L),evidence.get("reasons"));
        assertEquals(Map.of("pinnedReason","unavailable","blockingOperation","unavailable"),evidence.get("optionalFields"));
        assertEquals(0L,VtEvidence.read(record("r",true,true,false),"r","tiercache.FixturePin").get("total"));
    }
    @Test void runtimeReasonFieldsAreReadAsStrings() throws Exception {
        Path path=dir.resolve("reason.jfr");
        try(var recording=new Recording()) {
            recording.enable(ReasonPin.class).withStackTrace(); recording.enable(VtEvidence.Proof.class);
            recording.setDestination(path); recording.start(); new ReasonPin().commit();
            var proof=new VtEvidence.Proof();proof.runId="r";proof.commit(); recording.stop();
        }
        var evidence=VtEvidence.read(path,"r","tiercache.FixtureReasonPin");
        assertEquals(Map.of("initialization",1L),evidence.get("reasons"));
        assertEquals(Map.of("monitor enter",1L),evidence.get("blockingOperations"));
        assertEquals(Map.of("pinnedReason","available","blockingOperation","available"),evidence.get("optionalFields"));
    }
    @Test void invalidEvidenceCannotPassAsZero() throws Exception {
        assertThrows(Exception.class,()->VtEvidence.read(dir.resolve("missing"),"r","pin"));
        var corrupt=dir.resolve("corrupt.jfr");Files.writeString(corrupt,"not a recording");
        assertThrows(Exception.class,()->VtEvidence.read(corrupt,"r","pin"));
        var valid=record("r",true,true,true);
        assertThrows(IllegalStateException.class,()->VtEvidence.read(valid,"other","tiercache.FixturePin"));
        assertThrows(IllegalStateException.class,()->VtEvidence.read(valid,"r","missing.Type"));
        assertThrows(IllegalStateException.class,()->VtEvidence.read(record("r",false,true,true),"r","tiercache.FixturePin"));
        assertThrows(IllegalStateException.class,()->VtEvidence.read(record("r",true,false,true),"r","tiercache.FixturePin"));
    }
    @Test void portableJsonRetainsFailureAndEscapesControlCharacters() throws Exception {
        var file=dir.resolve("nested/summary.json");
        VtEvidence.write(file,Map.of("status","FAILED","failure","bad\n\"value\"","completed",0));
        String json=Files.readString(file);assertTrue(json.contains("FAILED"));assertTrue(json.contains("\\u000a"));
        assertTrue(json.contains("\\\"value\\\""));
    }
}
