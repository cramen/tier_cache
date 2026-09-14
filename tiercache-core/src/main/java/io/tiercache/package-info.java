/**
 * Public API of the TierCache two-level cache: {@link io.tiercache.TierCache}
 * (synchronous), {@link io.tiercache.AsyncTierCache} (non-blocking view), and
 * {@link io.tiercache.TierCacheFactory} (configuration and lifecycle), plus
 * the supporting configuration, result, and versioning types they expose.
 *
 * <p>Caches built here are eventually consistent by design; no
 * strong-consistency guarantees are given or implied.
 *
 * @since 0.1.0
 */
package io.tiercache;
