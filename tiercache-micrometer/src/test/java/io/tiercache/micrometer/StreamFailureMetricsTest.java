package io.tiercache.micrometer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.tiercache.spi.CacheMetricsListener.StreamResult;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
@org.junit.jupiter.api.parallel.ResourceLock("java.util.Locale.default")
class StreamFailureMetricsTest {
    @Test void streamFailuresUseFixedLocaleIndependentLowCardinalityLabels() {
        Locale previous = Locale.getDefault(); var registry = new SimpleMeterRegistry();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR")); var metrics = new MicrometerCacheMetrics(registry);
            for (var result : StreamResult.values()) {
                metrics.onStreamFailure("c", result);
                var counter = registry.get("tiercache.invalidation.stream").tags("cache","c","result",result.name().toLowerCase(Locale.ROOT)).counter();
                assertEquals(1,counter.count()); assertEquals(2,counter.getId().getTags().size());
            }
        } finally { Locale.setDefault(previous); registry.close(); }
    }
}
