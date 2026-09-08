package io.tiercache;

import io.tiercache.internal.CaffeineLocalCache;
import io.tiercache.testkit.LocalCacheContractTest;

class CaffeineLocalCacheContractTest extends LocalCacheContractTest {

    @Override
    protected io.tiercache.spi.LocalCache<String, String> newCache() {
        return new CaffeineLocalCache<>(CacheSettings.defaults());
    }
}
