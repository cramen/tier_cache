package io.tiercache.invalidation;

import io.tiercache.InvalidationMessage;
import io.tiercache.Version;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.CheckedRange;
import io.tiercache.spi.InvalidationHandler;
import io.tiercache.spi.InvalidationEventListener;
import io.tiercache.spi.InvalidationJournal;
import io.tiercache.spi.InvalidationListener;
import io.tiercache.spi.InvalidationTarget;
import io.tiercache.spi.InvalidationTransport;
import io.tiercache.spi.JournalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The invalidation engine: publishes local writes, applies inbound events
 * to registered caches (last-write-wins), and heals missed events after a
 * transport reconnect by replaying the journal — with a controlled full L1
 * flush when the journal window was exceeded or a read's integrity cannot
 * be confirmed.
 *
 * <p>The replay cursor always marks the end of the <b>contiguous applied
 * prefix</b> of the journal: live deliveries are tracked in a bounded
 * applied-version window, the cursor advances on a bounded cadence over
 * exactly the contiguous rows already applied (never past an unconsumed
 * row, never to the stream end), and every cursor read is one atomic
 * {@link InvalidationJournal#checkedRead} — integrity proof and range from
 * the same response. A window overflow behind a delayed row falls back to
 * journal-driven catch-up (the journal is the source of truth).
 *
 * <p>Created per factory via {@code TierCacheFactory.Builder.invalidation(...)}:
 * <pre>{@code
 * .invalidation(versions -> new InvalidationService(transport, journal,
 *         versions.instanceId(), listener))
 * }</pre>
 *
 * <p><b>Internal — not part of the supported API.</b> The default invalidation
 * engine behind the {@code io.tiercache.spi} interfaces; may change in any
 * release without notice.
 *
 * @since 0.1.0
 */
public final class InvalidationService implements InvalidationHandler {

    private static final Logger log = LoggerFactory.getLogger(InvalidationService.class);

    /** Live deliveries between cursor ticks (bounded-cadence tracking). */
    private static final long CURSOR_TICK_EVERY = 64L;
    /** Versions tracked per cache since the confirmed cursor. */
    private static final int APPLIED_WINDOW_CAPACITY = 128;
    /** Rows read per checked read on the tick and catch-up paths. */
    private static final int READ_BATCH = 256;
    /** Catch-up batches per trigger (progress is guaranteed; the rest continues on the next trigger). */
    private static final int MAX_CATCHUP_BATCHES = 16;

    private final InvalidationTransport transport;
    private final InvalidationJournal journal; // null = no replay capability
    private final UUID originInstanceId;
    private final InvalidationListener listener;
    private final Map<String, InvalidationTarget> targets = new ConcurrentHashMap<>();
    private final Map<String, AutoCloseable> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, String> cursors = new ConcurrentHashMap<>();
    private final Map<String, Set<Version>> appliedWindows = new ConcurrentHashMap<>();
    private final Map<String, Long> deliveriesSinceCursorTick = new ConcurrentHashMap<>();
    private final Map<String, Object> cacheLocks = new ConcurrentHashMap<>();

    private final CacheMetricsListener metrics;
    private volatile InvalidationEventListener eventListener = InvalidationEventListener.NOOP;

    /**
     * Creates the service without metrics reporting.
     *
     * @param transport        the invalidation transport; its reconnect
     *                         listener is taken over by this service
     * @param journal          the invalidation journal used to heal missed
     *                         events after a reconnect, or {@code null} to
     *                         disable replay (reconnect then flushes L1
     *                         entirely)
     * @param originInstanceId ID of this instance; own writes are skipped on
     *                         receipt
     * @param listener         lifecycle listener, or {@code null} for no
     *                         callbacks
     * @since 0.1.0
     */
    public InvalidationService(InvalidationTransport transport, InvalidationJournal journal,
            UUID originInstanceId, InvalidationListener listener) {
        this(transport, journal, originInstanceId, listener, CacheMetricsListener.NOOP);
    }

    /**
     * Creates the service.
     *
     * @param transport        the invalidation transport; its reconnect
     *                         listener is taken over by this service
     * @param journal          the invalidation journal used to heal missed
     *                         events after a reconnect, or {@code null} to
     *                         disable replay (reconnect then flushes L1
     *                         entirely)
     * @param originInstanceId ID of this instance; own writes are skipped on
     *                         receipt
     * @param listener         lifecycle listener, or {@code null} for no
     *                         callbacks
     * @param metrics          metrics listener for invalidation traffic
     * @since 0.1.0
     */
    public InvalidationService(InvalidationTransport transport, InvalidationJournal journal,
            UUID originInstanceId, InvalidationListener listener, CacheMetricsListener metrics) {
        this.transport = transport;
        this.journal = journal;
        this.originInstanceId = originInstanceId;
        this.listener = listener != null ? listener : InvalidationListener.NOOP;
        this.metrics = metrics;
        transport.setReconnectListener(this::onReconnect);
    }

    @Override
    public void onLocalWrite(String cache, Object key, io.tiercache.Version version,
            InvalidationMessage.Type type) {
        transport.publish(new InvalidationMessage(cache, key, version, originInstanceId, type));
        metrics.onInvalidation(cache, CacheMetricsListener.Direction.SENT);
    }

    @Override
    public void onLocalUpdate(String cache, Object key, Object value, io.tiercache.Version version) {
        transport.publish(new InvalidationMessage(cache, key, version, originInstanceId,
                InvalidationMessage.Type.UPDATE, value));
        metrics.onInvalidation(cache, CacheMetricsListener.Direction.SENT);
    }

    @Override
    public void registerTarget(String cache, InvalidationTarget target) {
        targets.put(cache, target);
        subscriptions.computeIfAbsent(cache, c -> {
            // First subscribe: start from the journal's end — a fresh target
            // has an empty L1, so replaying history would only evict fresh
            // entries by stale version order.
            if (journal != null) {
                cursors.put(c, journal.endCursor(c));
            }
            return transport.subscribe(c, this::onMessage);
        });
    }

    private void onMessage(InvalidationMessage message) {
        if (message.originInstanceId().equals(originInstanceId)) {
            return; // own write: our L1 is already correct
        }
        InvalidationTarget target = targets.get(message.cache());
        if (target == null) {
            return;
        }
        applyInbound(target, message);
        recordDelivery(message.cache(), target, message.version());
    }

    /** Applies an inbound event to the target (live, replayed or caught-up). */
    private void applyInbound(InvalidationTarget target, InvalidationMessage message) {
        Object span = metrics.onInvalidationStart(message.cache());
        metrics.onInvalidation(message.cache(), CacheMetricsListener.Direction.RECEIVED);
        try {
            switch (message.type()) {
                case INVALIDATE -> target.evictL1IfNewer(message.key(), message.version());
                case UPDATE -> target.applyUpdateL1(message.key(), message.payload(),
                        message.version());
                case EVICT_ALL -> target.evictAllL1();
            }
            eventListener.onEvent(message.cache(), message);
        } finally {
            metrics.onInvalidationEnd(message.cache(), span);
        }
    }

    /**
     * Tracks a live-applied event for cursor advancement: the version joins
     * the bounded applied window, an overflow falls back to journal-driven
     * catch-up, and every {@link #CURSOR_TICK_EVERY}th delivery advances the
     * cursor over the contiguous applied prefix.
     */
    private void recordDelivery(String cache, InvalidationTarget target, Version version) {
        if (journal == null) {
            return;
        }
        Object lock = cacheLocks.computeIfAbsent(cache, c -> new Object());
        synchronized (lock) {
            Set<Version> window = appliedWindows.computeIfAbsent(cache, c -> new HashSet<>());
            window.add(version);
            if (window.size() > APPLIED_WINDOW_CAPACITY) {
                catchUpFromJournal(cache, target);
            }
            long deliveries = deliveriesSinceCursorTick.merge(cache, 1L, Long::sum);
            if (deliveries % CURSOR_TICK_EVERY == 0) {
                advanceCursor(cache, target);
            }
        }
    }

    /**
     * Cadence tick: advances the cursor over exactly the contiguous rows
     * from one checked read whose versions are already accounted for (in
     * the applied window, or own writes — our L1 holds them by definition),
     * stopping at the first unconsumed row. Never advances to the stream
     * end and never past an unconsumed row.
     */
    private void advanceCursor(String cache, InvalidationTarget target) {
        String cursor = cursors.get(cache);
        if (cursor == null) {
            return;
        }
        CheckedRange range;
        try {
            range = journal.checkedRead(cache, cursor, READ_BATCH);
        } catch (RuntimeException e) {
            // A failed tick read proves nothing either way; the cursor stays
            // put and the next tick retries (a later replay may widen).
            log.debug("Cursor tick read failed for cache '{}'; will retry on a later tick.",
                    cache, e);
            return;
        }
        if (!range.startIntact()) {
            flushL1(cache, target,
                    "the replay cursor row was trimmed; prefix integrity is unconfirmable");
            return;
        }
        Set<Version> window = appliedWindows.get(cache);
        String confirmed = cursor;
        for (JournalRow row : rowsAfterCursor(range, cursor)) {
            if (row.message().originInstanceId().equals(originInstanceId)
                    || (window != null && window.remove(row.message().version()))) {
                confirmed = row.cursor();
            } else {
                break; // the first unconsumed row: never advance past it
            }
        }
        cursors.put(cache, confirmed);
    }

    /**
     * Window overflow behind a delayed row: catches up directly from the
     * journal (the source of truth), applying rows in order and advancing
     * the cursor over every row read — applied or stale-dropped, both are
     * accounted. Bounded work per trigger; progress is guaranteed.
     */
    private void catchUpFromJournal(String cache, InvalidationTarget target) {
        String cursor = cursors.get(cache);
        if (cursor == null) {
            return;
        }
        Set<Version> window = appliedWindows.get(cache);
        for (int batch = 0; batch < MAX_CATCHUP_BATCHES; batch++) {
            CheckedRange range;
            try {
                range = journal.checkedRead(cache, cursor, READ_BATCH);
            } catch (RuntimeException e) {
                log.debug("Catch-up read failed for cache '{}'; will retry on the next trigger.",
                        cache, e);
                return;
            }
            if (!range.startIntact()) {
                flushL1(cache, target,
                        "the replay cursor row was trimmed; prefix integrity is unconfirmable");
                return;
            }
            List<JournalRow> rows = rowsAfterCursor(range, cursor);
            if (rows.isEmpty()) {
                return; // caught up to the journal end
            }
            for (JournalRow row : rows) {
                if (!row.message().originInstanceId().equals(originInstanceId)) {
                    applyInbound(target, row.message());
                }
                if (window != null) {
                    window.remove(row.message().version());
                }
                cursor = row.cursor();
            }
            cursors.put(cache, cursor);
        }
    }

    @Override
    public void setEventListener(InvalidationEventListener listener) {
        this.eventListener = listener != null ? listener : InvalidationEventListener.NOOP;
    }

    @Override
    public void onL2Recovery() {
        onReconnect();
    }

    private void onReconnect() {
        if (journal == null) {
            // No replay capability: the honest fallback is a full flush.
            log.warn("Invalidation transport reconnected without a journal; "
                    + "flushing L1 for all caches to avoid stale entries.");
            targets.forEach((cache, target) -> target.evictAllL1());
            return;
        }
        targets.forEach((cache, target) -> {
            Object lock = cacheLocks.computeIfAbsent(cache, c -> new Object());
            synchronized (lock) {
                String cursor = cursors.get(cache);
                if (cursor == null) {
                    return;
                }
                try {
                    while (true) {
                        CheckedRange range = journal.checkedRead(cache, cursor, READ_BATCH);
                        if (!range.startIntact()) {
                            flushL1(cache, target,
                                    "the journal window was exceeded during the disconnect");
                            return;
                        }
                        List<JournalRow> rows = rowsAfterCursor(range, cursor);
                        if (rows.isEmpty()) {
                            return; // nothing missed
                        }
                        Set<Version> window = appliedWindows.get(cache);
                        for (JournalRow row : rows) {
                            if (!row.message().originInstanceId().equals(originInstanceId)) {
                                applyInbound(target, row.message());
                                metrics.onInvalidation(cache,
                                        CacheMetricsListener.Direction.REPLAYED);
                            }
                            if (window != null) {
                                window.remove(row.message().version());
                            }
                            // The cursor records the ID of the last row
                            // actually read — never the stream end: a row
                            // written after this read sits past the cursor
                            // and is picked up next time.
                            cursor = row.cursor();
                        }
                        cursors.put(cache, cursor);
                    }
                } catch (RuntimeException e) {
                    // A failed replay read (e.g. the trim counter is
                    // unreadable): integrity is unconfirmable — take the
                    // flush path, never assume "no loss".
                    flushL1(cache, target, "the journal replay read failed", e);
                }
            }
        });
    }

    /**
     * The flush path: L1 is dropped for the cache, the flush is signaled
     * (log + listener + dropped metric), and the cursor re-baselines at the
     * journal's current end.
     */
    private void flushL1(String cache, InvalidationTarget target, String reason) {
        log.warn("Invalidation journal cannot confirm contiguous history for cache '{}' "
                + "({}); flushing L1 entirely.", cache, reason);
        target.evictAllL1();
        listener.onJournalOverflow(cache);
        metrics.onInvalidation(cache, CacheMetricsListener.Direction.DROPPED);
        cursors.put(cache, journal.endCursor(cache));
        Set<Version> window = appliedWindows.get(cache);
        if (window != null) {
            window.clear();
        }
    }

    private void flushL1(String cache, InvalidationTarget target, String reason, Exception e) {
        log.warn("Invalidation journal cannot confirm contiguous history for cache '{}' "
                + "({}); flushing L1 entirely.", cache, reason, e);
        target.evictAllL1();
        listener.onJournalOverflow(cache);
        metrics.onInvalidation(cache, CacheMetricsListener.Direction.DROPPED);
        cursors.put(cache, journal.endCursor(cache));
        Set<Version> window = appliedWindows.get(cache);
        if (window != null) {
            window.clear();
        }
    }

    /**
     * The rows to process from a checked read: all of them for a beginning
     * cursor, everything after the cursor's own row otherwise (an intact
     * non-beginning read starts AT the cursor row, which is already
     * accounted for).
     */
    private static List<JournalRow> rowsAfterCursor(CheckedRange range, String cursor) {
        List<JournalRow> rows = range.rows();
        if (!rows.isEmpty() && rows.get(0).cursor().equals(cursor)) {
            return rows.subList(1, rows.size());
        }
        return rows;
    }

    @Override
    public void close() {
        subscriptions.values().forEach(subscription -> {
            try {
                subscription.close();
            } catch (Exception e) {
                log.warn("Failed to close invalidation subscription", e);
            }
        });
        subscriptions.clear();
        targets.clear();
        transport.close();
    }
}
