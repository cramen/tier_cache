package io.tiercache.micronaut;

import io.micronaut.cache.CacheManager;
import io.micronaut.cache.SyncCache;
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Micronaut {@link CacheManager} over {@link TierCacheFactory}. Named caches
 * are created on demand: a name present in {@code tiercache.caches.*} gets
 * its configured overrides, any other name gets the global defaults.
 *
 * <p><strong>Internal:</strong> not part of the supported public API. The
 * module registers this manager as the application {@code CacheManager}
 * (replacing {@link io.micronaut.cache.DefaultCacheManager}); applications
 * interact with it through Micronaut's {@link io.micronaut.cache.SyncCache}
 * abstraction. It may change in any release without notice.
 *
 * @since 1.1.0
 */
public class TierCacheMicronautManager implements CacheManager<TierCache<Object, Object>> {

    private final TierCacheFactory factory;
    private final Map<String, TierCacheMicronautCache> caches = new ConcurrentHashMap<>();

    /**
     * Creates a manager backed by the given factory.
     *
     * @param factory the two-level cache factory named caches are obtained from
     */
    public TierCacheMicronautManager(TierCacheFactory factory) {
        this.factory = factory;
    }

    /**
     * Returns the names of the caches created so far; every other name is
     * created lazily by {@link #getCache(String)}.
     *
     * @return the known cache names
     */
    @Override
    public Set<String> getCacheNames() {
        return caches.keySet();
    }

    /**
     * Returns the two-level cache for the name, creating it on first use:
     * the per-cache override from {@code tiercache.caches.<name>.*} applies
     * when present, the global defaults otherwise. The same adapter instance
     * is returned for repeated calls with the same name.
     *
     * @param name the cache name requested through Micronaut's cache
     *             abstraction
     * @return a {@link SyncCache} backed by the two-level cache, never
     *         {@code null}
     */
    @Override
    public SyncCache<TierCache<Object, Object>> getCache(String name) {
        return caches.computeIfAbsent(name, n ->
                new TierCacheMicronautCache(n, factory.getCache(n), factory.asyncCache(n)));
    }
}
