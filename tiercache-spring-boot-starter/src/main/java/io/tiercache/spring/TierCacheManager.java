package io.tiercache.spring;

import io.tiercache.TierCacheFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractCacheManager;

import java.util.Collection;

/**
 * Spring {@code CacheManager} over {@link TierCacheFactory}. Named caches
 * not present in {@code tiercache.caches.*} are created on demand with the
 * global defaults.
 */
public class TierCacheManager extends AbstractCacheManager {

    private final TierCacheFactory factory;

    public TierCacheManager(TierCacheFactory factory) {
        this.factory = factory;
    }

    @Override
    protected Collection<? extends Cache> loadCaches() {
        return java.util.List.of();
    }

    @Override
    protected Cache getMissingCache(String name) {
        return new TierCacheSpringCache(name, factory.getCache(name));
    }
}
