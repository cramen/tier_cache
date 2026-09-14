package io.tiercache.spring;

import io.tiercache.TierCacheFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractCacheManager;

import java.util.Collection;

/**
 * Spring {@code CacheManager} over {@link TierCacheFactory}. Named caches
 * not present in {@code tiercache.caches.*} are created on demand with the
 * global defaults.
 *
 * <p><strong>Internal:</strong> not part of the supported public API. The
 * starter registers this manager as the application {@code CacheManager};
 * applications interact with it through Spring's
 * {@link org.springframework.cache.Cache} abstraction. It may change in any
 * release without notice.
 *
 * @since 0.1.0
 */
public class TierCacheManager extends AbstractCacheManager {

    private final TierCacheFactory factory;

    /**
     * Creates a manager backed by the given factory.
     *
     * @param factory the two-level cache factory named caches are obtained from
     */
    public TierCacheManager(TierCacheFactory factory) {
        this.factory = factory;
    }

    /**
     * Loads no predefined caches: every named cache is created lazily by
     * {@link #getMissingCache(String)}.
     *
     * @return an empty collection
     */
    @Override
    protected Collection<? extends Cache> loadCaches() {
        return java.util.List.of();
    }

    /**
     * Creates a two-level cache for a name not seen before, applying the
     * per-cache override from {@code tiercache.caches.<name>.*} when present
     * and the global defaults otherwise.
     *
     * @param name the cache name requested through Spring's cache abstraction
     * @return a new {@link Cache} backed by the two-level cache, never {@code null}
     */
    @Override
    protected Cache getMissingCache(String name) {
        return new TierCacheSpringCache(name, factory.getCache(name));
    }
}
