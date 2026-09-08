package io.tiercache.spi;

/**
 * Implemented by {@link RemoteCache} implementations that can also provide
 * distributed rebuild locks (F-21). Lets the factory derive the lock
 * provider automatically from the configured L2 transport.
 *
 * <p><b>Incubating:</b> 0.x API, may change until CP-0.
 */
public interface LockProviderSource {

    DistributedLockProvider lockProvider();
}
