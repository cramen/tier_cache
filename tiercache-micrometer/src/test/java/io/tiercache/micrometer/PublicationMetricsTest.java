package io.tiercache.micrometer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.tiercache.spi.PublicationOutcome;
import org.junit.jupiter.api.Test;
import java.util.Locale;
import static org.junit.jupiter.api.Assertions.*;

class PublicationMetricsTest {
    @Test void batchesHaveFixedAsciiLabelsEvenUnderTurkishLocale() {
        Locale previous=Locale.getDefault();Locale.setDefault(Locale.forLanguageTag("tr-TR"));
        var registry=new SimpleMeterRegistry();
        try {
            var metrics=new MicrometerCacheMetrics(registry);
            for(var outcome:PublicationOutcome.values()) {
                metrics.onPublication("c",outcome,7);metrics.onPublication("c",outcome,3);
                var meter=registry.find("tiercache.invalidation.publish").tags("cache","c","outcome",outcome.name().toLowerCase(Locale.ROOT)).counter();
                assertNotNull(meter);assertEquals(10,meter.count());assertEquals(2,meter.getId().getTags().size());
            }
            assertEquals(4,registry.getMeters().size());
        } finally {registry.close();Locale.setDefault(previous);}
    }
}
