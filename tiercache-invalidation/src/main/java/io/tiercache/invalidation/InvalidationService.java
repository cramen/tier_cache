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
import java.util.LinkedHashSet;
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
    /** Versions tracked per cache since the confirmed cursor (nominal trigger). */
    private static final int APPLIED_WINDOW_CAPACITY = 128;
    /** Hard cap for the applied window; a breach after an unfinished catch-up forces resync-required. */
    private static final int APPLIED_WINDOW_HARD_CAP = 512;
    /** Recently confirmed versions per cache (duplicates of them are not re-tracked). */
    private static final int CONFIRMED_SET_CAPACITY = 256;
    /** Minimum interval between resync attempts for one cache in resync-required state. */
    private static final long RESYNC_MIN_INTERVAL_NANOS = 1_000_000_000L;
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
    /** Recently confirmed versions per cache (bounded; duplicates are not re-tracked). */
    private final Map<String, LinkedHashSet<Version>> confirmedVersions = new ConcurrentHashMap<>();
    /** Caches whose version tracking is unconfirmable; resync retries are throttled and lazy. */
    private final Map<String, Boolean> resyncRequired = new ConcurrentHashMap<>();
    private final Map<String, Long> lastResyncAttemptNanos = new ConcurrentHashMap<>();
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
     * Tracks a live-applied event for cursor advancement. A version the
     * cursor has already passed (bounded confirmed-set) is a late duplicate:
     * applied to L1 but NOT re-tracked. Window overflow falls back to
     * journal-driven catch-up; a catch-up that reached the journal end
     * accounts for everything left in the window (a live event's row is
     * journaled before its publish, so unseen versions sit at or before the
     * cursor). In resync-required state tracking is skipped entirely and
     * resync retries on a bounded time throttle.
     */
    private void recordDelivery(String cache, InvalidationTarget target, Version version) {
        if (journal == null) {
            return;
        }
        Object lock = cacheLocks.computeIfAbsent(cache, c -> new Object());
        synchronized (lock) {
            if (resyncRequired.containsKey(cache)) {
                long now = System.nanoTime();
                Long last = lastResyncAttemptNanos.get(cache);
                if (last == null || now - last >= RESYNC_MIN_INTERVAL_NANOS) {
                    lastResyncAttemptNanos.put(cache, now);
                    if (catchUpFromJournal(cache, target) == CatchUpOutcome.CAUGHT_UP) {
                        resyncRequired.remove(cache);
                    }
                }
                return; // degraded: no tracking until the resync completes
            }
            LinkedHashSet<Version> confirmed = confirmedVersions.get(cache);
            if (confirmed != null && confirmed.contains(version)) {
                return; // late duplicate of an already-accounted row
            }
            Set<Version> window = appliedWindows.computeIfAbsent(cache, c -> new HashSet<>());
            window.add(version);
            if (window.size() > APPLIED_WINDOW_CAPACITY) {
                CatchUpOutcome outcome = catchUpFromJournal(cache, target);
                if (outcome == CatchUpOutcome.CAUGHT_UP) {
                    window.clear();
                } else if (outcome == CatchUpOutcome.FAILED
                        || window.size() > APPLIED_WINDOW_HARD_CAP) {
                    enterResync(cache);
                }
            }
            long deliveries = deliveriesSinceCursorTick.merge(cache, 1L, Long::sum);
            if (deliveries % CURSOR_TICK_EVERY == 0) {
                advanceCursor(cache, target);
            }
        }
    }

    /** Marks a version as cursor-confirmed (bounded per cache; insertion-ordered eviction). */
    private void confirm(String cache, Version version) {
        LinkedHashSet<Version> confirmed =
                confirmedVersions.computeIfAbsent(cache, c -> new LinkedHashSet<>());
        confirmed.add(version);
        while (confirmed.size() > CONFIRMED_SET_CAPACITY) {
            confirmed.remove(confirmed.iterator().next());
        }
    }

    /**
     * Enters the resync-required state: tracking memory is freed and the
     * cursor stays where it is. While set, recovery is lazy and throttled;
     * missed rows are applied at the next recovery trigger (a delivery
     * after reads heal, or a reconnect replay) — until then L1 may serve
     * stale data.
     */
    private void enterResync(String cache) {
        Set<Version> window = appliedWindows.get(cache);
        if (window != null) {
            window.clear();
        }
        resyncRequired.put(cache, Boolean.TRUE);
        lastResyncAttemptNanos.put(cache, System.nanoTime());
        log.warn("Version tracking for cache '{}' is unconfirmable (catch-up failed or the "
                + "hard cap was exceeded); entering resync-required state. Missed rows are "
                + "applied at the next recovery trigger; L1 may serve stale data until then.",
                cache);
    }

    /** The outcome of one catch-up pass. */
    private enum CatchUpOutcome {
        /** The read reached the current journal end: everything left in the window is accounted. */
        CAUGHT_UP,
        /** The batch budget was exhausted before the journal end; progress is kept. */
        MORE_WORK,
        /** A read failed before the journal end. */
        FAILED
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
                    "the replay cursor row was trimmed; prefix integrity is unconfirmable", null);
            return;
        }
        Set<Version> window = appliedWindows.get(cache);
        String confirmed = cursor;
        for (JournalRow row : rowsAfterCursor(range, cursor)) {
            if (row.message().originInstanceId().equals(originInstanceId)
                    || (window != null && window.remove(row.message().version()))) {
                confirm(cache, row.message().version());
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
     * accounted. Returns the tri-state outcome; only {@link
     * CatchUpOutcome#CAUGHT_UP} proves the window fully accounted.
     */
    private CatchUpOutcome catchUpFromJournal(String cache, InvalidationTarget target) {
        String cursor = cursors.get(cache);
        if (cursor == null) {
            return CatchUpOutcome.CAUGHT_UP; // no cursor: nothing to account against
        }
        Set<Version> window = appliedWindows.get(cache);
        for (int batch = 0; batch < MAX_CATCHUP_BATCHES; batch++) {
            CheckedRange range;
            try {
                range = journal.checkedRead(cache, cursor, READ_BATCH);
            } catch (RuntimeException e) {
                log.debug("Catch-up read failed for cache '{}'; will retry on the next trigger.",
                        cache, e);
                return CatchUpOutcome.FAILED;
            }
            if (!range.startIntact()) {
                flushL1(cache, target,
                        "the replay cursor row was trimmed; prefix integrity is unconfirmable", null);
                return CatchUpOutcome.CAUGHT_UP; // the flush settles the state completely
            }
            List<JournalRow> rows = rowsAfterCursor(range, cursor);
            if (rows.isEmpty()) {
                return CatchUpOutcome.CAUGHT_UP;
            }
            for (JournalRow row : rows) {
                if (!row.message().originInstanceId().equals(originInstanceId)) {
                    applyInbound(target, row.message());
                }
                if (window != null) {
                    window.remove(row.message().version());
                }
                confirm(cache, row.message().version());
                cursor = row.cursor();
            }
            cursors.put(cache, cursor);
        }
        return CatchUpOutcome.MORE_WORK;
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
                                    "the journal window was exceeded during the disconnect", null);
                            return;
                        }
                        List<JournalRow> rows = rowsAfterCursor(range, cursor);
                        if (rows.isEmpty()) {
                            // Replay reached the journal end: this is a full
                            // journal-driven recovery — normal tracking resumes.
                            resyncRequired.remove(cache);
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
                            confirm(cache, row.message().version());
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
     * (log + listener + dropped metric), and the cursor re-baselines.
     *
     * <p>Order matters: the baseline is captured BEFORE L1 is cleared —
     * rows journaled up to it are covered by the clear (a re-warm reads
     * current L2, which includes them), while rows journaled after the
     * clear stay ahead of the stored cursor and are applied by the next
     * replay or catch-up. If the baseline read fails, the previous
     * confirmed cursor is kept (never advance past unread rows). Observers
     * (log, listener, metrics) fire last, after the state is settled, so an
     * observer failure cannot cancel the clear.
     */
    private void flushL1(String cache, InvalidationTarget target, String reason, Throwable cause) {
        String baseline;
        try {
            baseline = journal.endCursor(cache);
        } catch (RuntimeException e) {
            baseline = cursors.get(cache);
            log.warn("Journal baseline read failed for cache '{}'; keeping the previous "
                    + "confirmed cursor for the flush.", cache, e);
        }
        target.evictAllL1();
        cursors.put(cache, baseline);
        resyncRequired.remove(cache); // the flush settles the state completely
        lastResyncAttemptNanos.remove(cache);
        Set<Version> window = appliedWindows.get(cache);
        if (window != null) {
            window.clear();
        }
        if (cause == null) {
            log.warn("Invalidation journal cannot confirm contiguous history for cache '{}' "
                    + "({}); flushing L1 entirely.", cache, reason);
        } else {
            log.warn("Invalidation journal cannot confirm contiguous history for cache '{}' "
                    + "({}); flushing L1 entirely.", cache, reason, cause);
        }
        listener.onJournalOverflow(cache);
        metrics.onInvalidation(cache, CacheMetricsListener.Direction.DROPPED);
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
