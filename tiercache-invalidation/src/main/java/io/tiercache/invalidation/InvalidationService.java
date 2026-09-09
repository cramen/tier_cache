package io.tiercache.invalidation;

import io.tiercache.InvalidationMessage;
import io.tiercache.spi.CacheMetricsListener;
import io.tiercache.spi.InvalidationHandler;
import io.tiercache.spi.InvalidationEventListener;
import io.tiercache.spi.InvalidationJournal;
import io.tiercache.spi.InvalidationListener;
import io.tiercache.spi.InvalidationTarget;
import io.tiercache.spi.InvalidationTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The invalidation engine: publishes local writes, applies inbound events
 * to registered caches (last-write-wins), and heals missed events after a
 * transport reconnect by replaying the journal — with a controlled full L1
 * flush when the journal window was exceeded.
 *
 * <p>Created per factory via {@code TierCacheFactory.Builder.invalidation(...)}:
 * <pre>{@code
 * .invalidation(versions -> new InvalidationService(transport, journal,
 *         versions.instanceId(), listener))
 * }</pre>
 *
 * <p><b>Incubating:</b> 0.x API, may change before 1.0.
 */
public final class InvalidationService implements InvalidationHandler {

    private static final Logger log = LoggerFactory.getLogger(InvalidationService.class);

    private final InvalidationTransport transport;
    private final InvalidationJournal journal; // null = no replay capability
    private final UUID originInstanceId;
    private final InvalidationListener listener;
    private final Map<String, InvalidationTarget> targets = new ConcurrentHashMap<>();
    private final Map<String, AutoCloseable> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, String> cursors = new ConcurrentHashMap<>();
    private final CacheMetricsListener metrics;
    private volatile InvalidationEventListener eventListener = InvalidationEventListener.NOOP;

    public InvalidationService(InvalidationTransport transport, InvalidationJournal journal,
            UUID originInstanceId, InvalidationListener listener) {
        this(transport, journal, originInstanceId, listener, CacheMetricsListener.NOOP);
    }

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
            String cursor = cursors.get(cache);
            if (cursor == null) {
                return;
            }
            if (journal.isTrimmed(cache, cursor)) {
                log.warn("Invalidation journal window exceeded for cache '{}' during "
                        + "disconnect; flushing L1 entirely.", cache);
                target.evictAllL1();
                listener.onJournalOverflow(cache);
                metrics.onInvalidation(cache, CacheMetricsListener.Direction.DROPPED);
                cursors.put(cache, journal.endCursor(cache));
                return;
            }
            List<InvalidationMessage> missed = journal.readRange(cache, cursor);
            missed.forEach(this::onMessage);
            missed.forEach(m -> metrics.onInvalidation(cache, CacheMetricsListener.Direction.REPLAYED));
            if (!missed.isEmpty()) {
                // readRange is ordered; the last entry's cursor is the new mark.
                cursors.put(cache, journal.endCursor(cache));
            }
        });
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
