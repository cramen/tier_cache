package io.tiercache.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.tiercache.TierCacheFactory;
import io.tiercache.spi.InvalidationJournal;

import javax.management.MBeanServer;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMX registration for {@link TiercacheInspectionMXBean}. One instance per
 * factory; call {@link #register()} to expose and {@link #close()} to
 * unregister.
 *
 * @since 0.1.0
 */
public final class TiercacheInspection implements TiercacheInspectionMXBean, AutoCloseable {

    private final MeterRegistry registry;
    private final TierCacheFactory factory;
    private final InvalidationJournal journal;
    private final List<String> cacheNames;
    private static final Logger log = LoggerFactory.getLogger(TiercacheInspection.class);
    private final ReentrantLock lifecycle = new ReentrantLock();
    private final Supplier<MBeanServer> serverSupplier;
    private ObjectName objectName;
    private MBeanServer ownerServer;

    /**
     * Creates an inspection MBean reading counters from the given registry
     * and state from the given factory.
     *
     * @param registry the registry holding the {@code tiercache.requests}
     *     counters the hit ratios are computed from
     * @param factory the factory whose circuit-breaker state is reported
     * @param journal the invalidation journal backing
     *     {@link #getJournalSize(String)}; may be {@code null}, in which case
     *     the journal size is reported as {@code -1}
     * @param cacheNames the named caches exposed by {@link #getCacheNames()}
     * @since 0.1.0
     */
    public TiercacheInspection(MeterRegistry registry, TierCacheFactory factory,
            InvalidationJournal journal, List<String> cacheNames) {
        this(registry, factory, journal, cacheNames, ManagementFactory::getPlatformMBeanServer);
    }

    /** Package-local seam for deterministic registration failure and lifecycle races. */
    TiercacheInspection(MeterRegistry registry, TierCacheFactory factory,
            InvalidationJournal journal, List<String> cacheNames, Supplier<MBeanServer> serverSupplier) {
        this.serverSupplier = serverSupplier;
        this.registry = registry;
        this.factory = factory;
        this.journal = journal;
        this.cacheNames = cacheNames;
    }

    /**
     * Registers this MBean with the platform MBean server under
     * {@code io.tiercache:type=Inspection}. A second call while the name is
     * already registered is a no-op.
     *
     * @throws IllegalStateException if JMX registration fails
     * @since 0.1.0
     */
    public void register() {
        lifecycle.lock();
        try {
            if (objectName != null) return; // Preserve this ownership period on repeated registration.
            ObjectName name = new ObjectName("io.tiercache:type=Inspection");
            MBeanServer server = serverSupplier.get();
            try {
                server.registerMBean(this, name);
            } catch (InstanceAlreadyExistsException occupied) {
                return; // A foreign or competing registration grants no ownership.
            }
            ownerServer = server;
            objectName = name;
        } catch (Exception e) {
            throw new IllegalStateException("JMX registration failed", e);
        } finally {
            lifecycle.unlock();
        }
    }

    @Override
    public String[] getCacheNames() {
        return cacheNames.toArray(new String[0]);
    }

    @Override
    public double getL1HitRatio(String cache) {
        double l1 = counter(cache, "l1_hit");
        double l2 = counter(cache, "l2_hit");
        double miss = counter(cache, "miss") + counter(cache, "load");
        double total = l1 + l2 + miss;
        return total == 0 ? 0 : l1 / total;
    }

    @Override
    public double getL2HitRatio(String cache) {
        double l1 = counter(cache, "l1_hit");
        double l2 = counter(cache, "l2_hit");
        double miss = counter(cache, "miss") + counter(cache, "load");
        double total = l1 + l2 + miss;
        return total == 0 ? 0 : l2 / total;
    }

    @Override
    public String getBreakerState() {
        return factory.breakerState().name().toLowerCase(Locale.ROOT);
    }

    @Override
    public long getJournalSize(String cache) {
        return journal != null ? journal.size(cache) : -1;
    }

    private double counter(String cache, String result) {
        try {
            Counter c = registry.get("tiercache.requests")
                    .tags("cache", cache, "result", result).counter();
            return c.count();
        } catch (MeterNotFoundException e) {
            return 0;
        }
    }

    /**
     * Relinquishes this successful registration once. Skipped registrations and
     * repeated closes are harmless; a later register may start a new ownership period.
     */
    @Override
    public void close() {
        lifecycle.lock();
        try {
            ObjectName owned = objectName;
            MBeanServer server = ownerServer;
            objectName = null;
            ownerServer = null;
            if (owned == null) return;
            try {
                server.unregisterMBean(owned);
            } catch (InstanceNotFoundException absent) {
                // External removal is benign; this ownership period is already consumed.
            } catch (Exception failure) {
                log.warn("JMX inspection unregister failed ({})", failure.getClass().getSimpleName());
            }
        } finally {
            lifecycle.unlock();
        }
    }
}
