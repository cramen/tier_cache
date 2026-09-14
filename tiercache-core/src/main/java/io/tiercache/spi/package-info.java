/**
 * Service provider interfaces for plugging in alternative cache levels
 * ({@link io.tiercache.spi.LocalCache}, {@link io.tiercache.spi.RemoteCache}),
 * distributed lock providers, invalidation transports and journals, and
 * metrics or lifecycle listeners. Implemented by the TierCache transport and
 * observability modules; custom implementations plug in through
 * {@code TierCacheFactory.Builder}.
 *
 * <p><b>Internal — not part of the supported API.</b> These interfaces are
 * extension points for cache/transport implementors and may change in any
 * release without notice.
 *
 * @since 0.1.0
 */
package io.tiercache.spi;
