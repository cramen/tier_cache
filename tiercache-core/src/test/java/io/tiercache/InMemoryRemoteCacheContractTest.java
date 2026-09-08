package io.tiercache;

import io.tiercache.testkit.InMemoryRemoteCache;
import io.tiercache.testkit.RemoteCacheContractTest;

class InMemoryRemoteCacheContractTest extends RemoteCacheContractTest {

    @Override
    protected InMemoryRemoteCache<String, String> newCache() {
        return new InMemoryRemoteCache<>();
    }
}
