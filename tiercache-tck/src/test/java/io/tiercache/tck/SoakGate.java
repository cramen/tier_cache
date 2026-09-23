package io.tiercache.tck;

import java.time.Duration;
import java.util.*;

/** Fixed-baseline assessment; memory series are never added or compared step-to-step. */
final class SoakGate {
    static final double MAX_GROWTH = 0.05;
    static final int WARMUP_PERCENT = 20;
    record Sample(long elapsedNanos, long heapBytes, long rssBytes, long journalRows,
                  long operations, long explicitGcCompletions, List<Map<String, Object>> workers) { }
    record Assessment(int discarded, int steadySamples, long heapBaseline, long heapPeak,
                      double heapGrowth, long rssBaseline, long rssPeak, double rssGrowth,
                      long journalPeak, double journalFirstMean, double journalSecondMean,
                      List<String> violations) {
        void requirePass() {
            if (!violations.isEmpty()) throw new AssertionError(String.join("; ", violations));
        }
    }
    static void validateDuration(Duration duration, Duration interval) {
        if (interval.isZero() || interval.isNegative() || duration.compareTo(interval.multipliedBy(3)) < 0) {
            throw new IllegalArgumentException("Soak duration must cover at least three sample intervals (minimum "
                    + interval.multipliedBy(3) + ") for three steady-state samples after warm-up");
        }
        duration.toNanos();
    }
    static Assessment assess(List<Sample> samples, int capacity) {
        int discard = (int) Math.ceil(samples.size() * WARMUP_PERCENT / 100.0);
        if (samples.size() - discard < 3) throw new AssertionError("Need at least three usable steady-state samples after 20% warm-up; got " + (samples.size() - discard));
        long previous = -1, previousGc = 0;
        for (var sample : samples) {
            if (sample.elapsedNanos() < 0 || sample.elapsedNanos() <= previous || sample.heapBytes() <= 0
                    || sample.rssBytes() <= 0 || sample.journalRows() < 0 || sample.explicitGcCompletions() <= previousGc) throw new AssertionError("Invalid memory/journal/time sample: " + sample);
            previous = sample.elapsedNanos();
            previousGc = sample.explicitGcCompletions();
        }
        var steady = samples.subList(discard, samples.size());
        long heapBase = steady.get(0).heapBytes(), rssBase = steady.get(0).rssBytes();
        long heapPeak = steady.stream().mapToLong(Sample::heapBytes).max().orElseThrow();
        long rssPeak = steady.stream().mapToLong(Sample::rssBytes).max().orElseThrow();
        double heapGrowth = (heapPeak - heapBase) / (double) heapBase, rssGrowth = (rssPeak - rssBase) / (double) rssBase;
        long journalPeak = steady.stream().mapToLong(Sample::journalRows).max().orElseThrow();
        int half = steady.size() / 2;
        double first = steady.subList(0, half).stream().mapToLong(Sample::journalRows).average().orElseThrow();
        double second = steady.subList(half, steady.size()).stream().mapToLong(Sample::journalRows).average().orElseThrow();
        List<String> failures = new ArrayList<>();
        if (heapGrowth > MAX_GROWTH) failures.add("Post-GC heap peak exceeds fixed baseline by " + heapGrowth * 100 + "% (limit 5%)");
        if (rssGrowth > MAX_GROWTH) failures.add("RSS peak exceeds fixed baseline by " + rssGrowth * 100 + "% (limit 5%)");
        if (journalPeak > capacity * 2L) failures.add("Journal peak exceeds " + capacity * 2L + " rows");
        if (second > first + capacity * 0.25) failures.add("Journal second-half mean exceeds first-half mean plus " + capacity * 0.25 + " rows");
        return new Assessment(discard, steady.size(), heapBase, heapPeak, heapGrowth, rssBase, rssPeak, rssGrowth,
                journalPeak, first, second, List.copyOf(failures));
    }
}
