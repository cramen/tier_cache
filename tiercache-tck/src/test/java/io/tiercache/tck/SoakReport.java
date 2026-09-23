package io.tiercache.tck;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Small dependency-free JSON report, including failed/inconclusive runs. */
final class SoakReport {
    static void write(Path path, Map<String, Object> report) throws IOException {
        Path target = path.toAbsolutePath();
        Files.createDirectories(target.getParent());
        Path temporary = Files.createTempFile(target.getParent(), "soak-", ".json.tmp");
        try {
            Files.writeString(temporary, json(report) + "\n");
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
        } finally { Files.deleteIfExists(temporary); }
    }
    static Map<String, Object> sample(SoakGate.Sample sample) {
        return Map.of("elapsedNanos", sample.elapsedNanos(), "postGcHeapBytes", sample.heapBytes(),
                "rssBytes", sample.rssBytes(), "journalRows", sample.journalRows(),
                "completedOperations", sample.operations(), "explicitGcCompletions", sample.explicitGcCompletions(), "workers", sample.workers());
    }
    static Map<String, Object> assessment(SoakGate.Assessment result) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("warmupSamplesDiscarded", result.discarded()); values.put("steadySamples", result.steadySamples());
        values.put("heapBaselineBytes", result.heapBaseline()); values.put("heapPeakBytes", result.heapPeak());
        values.put("heapGrowthFraction", result.heapGrowth()); values.put("rssBaselineBytes", result.rssBaseline());
        values.put("rssPeakBytes", result.rssPeak()); values.put("rssGrowthFraction", result.rssGrowth());
        values.put("journalPeakRows", result.journalPeak()); values.put("journalFirstHalfMean", result.journalFirstMean());
        values.put("journalSecondHalfMean", result.journalSecondMean()); values.put("violations", result.violations());
        return values;
    }
    static String json(Object value) {
        if (value == null) return "null";
        if (value instanceof String text) {
            StringBuilder out = new StringBuilder("\"");
            for (char c : text.toCharArray()) {
                if (c == '"' || c == '\\') out.append('\\').append(c);
                else if (c < 32) out.append(String.format(Locale.ROOT, "\\u%04x", (int)c));
                else out.append(c);
            }
            return out.append('"').toString();
        }
        if (value instanceof Number number) {
            if (!Double.isFinite(number.doubleValue())) throw new IllegalArgumentException("Non-finite report number");
            return number.toString();
        }
        if (value instanceof Boolean bool) return bool.toString();
        if (value instanceof Map<?, ?> map) {
            var out = new StringJoiner(",", "{", "}");
            map.forEach((key, entry) -> out.add(json(key.toString()) + ":" + json(entry)));
            return out.toString();
        }
        if (value instanceof Iterable<?> values) {
            var out = new StringJoiner(",", "[", "]");
            values.forEach(entry -> out.add(json(entry)));
            return out.toString();
        }
        throw new IllegalArgumentException("Unsupported report type: " + value.getClass());
    }
}
