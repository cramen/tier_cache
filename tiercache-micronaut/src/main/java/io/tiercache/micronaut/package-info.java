/**
 * Micronaut integration for the Tiercache two-level cache: the cache
 * manager replacing Micronaut's default, the {@code SyncCache} and
 * {@code AsyncCache} adapters, the {@code @Factory} wiring, and the
 * {@code tiercache.*} configuration properties.
 *
 * <p>Only the configuration types {@link io.tiercache.micronaut.TiercacheProperties}
 * and {@link io.tiercache.micronaut.TiercacheCacheProperties} (the
 * {@code tiercache.*} properties) are part of the supported public API. The
 * remaining types in this package are adapter internals: Micronaut loads and
 * wires them automatically, and they may change in any release without
 * notice.
 *
 * @since 1.1.0
 */
package io.tiercache.micronaut;
