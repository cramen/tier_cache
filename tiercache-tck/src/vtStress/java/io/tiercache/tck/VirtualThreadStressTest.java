package io.tiercache.tck;
import org.junit.jupiter.api.Test;
class VirtualThreadStressTest {
    @Test void steadyStateHasNoProductPinning() throws Throwable { VirtualThreadPhase.run(false); }
}
