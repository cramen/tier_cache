package io.tiercache.micronaut;

import io.micronaut.context.annotation.ConfigurationInject;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.Nullable;

import java.time.Duration;

/**
 * Per-cache override bound from {@code tiercache.caches.<name>.*}: one bean
 * per configured cache name (Micronaut's {@code @EachProperty} idiom — the
 * property keys are identical to the Spring starter's). Fields left unset
 * inherit the global {@code tiercache.defaults.*} level.
 *
 * <p>This class is part of the supported public API: applications configure
 * per-cache overrides exclusively through these properties.
 *
 * @since 1.1.0
 */
@EachProperty("tiercache.caches")
public class TiercacheCacheProperties extends TiercacheProperties.CacheProps {

    private final String name;

    /**
     * Creates the override set for one named cache; see
     * {@link TiercacheProperties.CacheProps} for the field semantics.
     *
     * @param name                the cache name (the
     *                            {@code tiercache.caches.<name>} key)
     * @param l1MaxSize           the L1 size bound
     * @param l1ExpireAfterWrite  the L1 expire-after-write TTL
     * @param l1ExpireAfterAccess the L1 expire-after-access TTL
     * @param l2Ttl               the L2 entry TTL
     * @param jitterAmplitude     the TTL jitter amplitude as a fraction
     * @param nullPolicy          the null-caching policy
     * @param nullMarkerTtl       the null-marker TTL
     * @param invalidationMode    the invalidation event mode
     * @param payloadCapBytes     the UPDATE-mode payload cap in bytes
     * @param staleTtl            the stale-while-revalidate window
     * @param xfetchEnabled       the XFetch early-refresh switch
     * @param xfetchBeta          the XFetch beta tuning factor
     * @since 1.1.0
     */
    @ConfigurationInject
    public TiercacheCacheProperties(@Parameter String name,
            @Nullable Long l1MaxSize,
            @Nullable Duration l1ExpireAfterWrite,
            @Nullable Duration l1ExpireAfterAccess,
            @Nullable Duration l2Ttl,
            @Nullable Double jitterAmplitude,
            @Nullable Kind nullPolicy,
            @Nullable Duration nullMarkerTtl,
            @Nullable io.tiercache.InvalidationMode invalidationMode,
            @Nullable Long payloadCapBytes,
            @Nullable Duration staleTtl,
            @Nullable Boolean xfetchEnabled,
            @Nullable Duration xfetchBeta,
            @Nullable Duration degradationStaleTtl) {
        super(l1MaxSize, l1ExpireAfterWrite, l1ExpireAfterAccess, l2Ttl, jitterAmplitude,
                nullPolicy, nullMarkerTtl, invalidationMode, payloadCapBytes, staleTtl,
                xfetchEnabled, xfetchBeta, degradationStaleTtl);
        this.name = name;
    }

    /**
     * Returns the cache name this override applies to.
     *
     * @return the {@code tiercache.caches.<name>} key
     * @since 1.1.0
     */
    public String getName() {
        return name;
    }
}
