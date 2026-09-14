/**
 * Spring Boot integration for the Tiercache two-level cache: the
 * auto-configuration that replaces the standard cache manager, the Spring
 * Cache SPI adapters, and the {@code tiercache.*} configuration properties.
 *
 * <p>Only {@link io.tiercache.spring.TiercacheProperties} (the
 * {@code tiercache.*} properties) is part of the supported public API. The
 * remaining types in this package are starter internals: Spring Boot loads
 * and wires them automatically, and they may change in any release without
 * notice.
 *
 * @since 0.1.0
 */
package io.tiercache.spring;
