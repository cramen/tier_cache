package io.tiercache.tck;
import org.junit.jupiter.api.Test;
class VirtualThreadColdStartTest {
    @Test void coldStartRetainsDiagnosticEvidence() throws Throwable { VirtualThreadPhase.run(true); }
}
