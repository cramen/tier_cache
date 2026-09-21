package io.tiercache.internal;

import io.tiercache.spi.StoredEntry;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * One local execution owner, shared by foreground loads and background refreshes.
 * Only demand/skip arbitration is synchronized; I/O, waiting, and future
 * completion always happen outside that short transition.
 */
final class LoadClaim<K, V> {

    record Demand<K, V>(Function<? super K, ? extends V> loader, long deadlineNanos) {
    }

    enum Kind {
        RESULT, SKIPPED_REFRESH
    }

    record Outcome<V>(Kind kind, StoredEntry<V> entry) {
        static <V> Outcome<V> result(StoredEntry<V> entry) {
            return new Outcome<>(Kind.RESULT, entry);
        }

        static <V> Outcome<V> skippedRefresh() {
            return new Outcome<>(Kind.SKIPPED_REFRESH, null);
        }

        boolean isSkipped() {
            return kind == Kind.SKIPPED_REFRESH;
        }

        StoredEntry<V> resultEntry() {
            if (isSkipped()) {
                throw new IllegalStateException("A skipped refresh is not a cache result");
            }
            return entry;
        }
    }

    final CompletableFuture<Outcome<V>> result = new CompletableFuture<>();
    private Demand<K, V> foreground;
    private boolean skipped;

    LoadClaim(Demand<K, V> foreground) {
        this.foreground = foreground;
    }

    /** False only when the refresh already committed its skipped outcome. */
    synchronized boolean requireResult(Demand<K, V> demand) {
        if (skipped) {
            return false;
        }
        if (foreground == null) {
            foreground = demand;
        }
        return true;
    }

    /** Commit a skip, or return the first foreground demand that prevents it. */
    synchronized Demand<K, V> skipOrForeground() {
        if (foreground == null) {
            skipped = true;
        }
        return foreground;
    }
}
