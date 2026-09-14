/**
 * Observability bindings for Tiercache:
 * {@link io.tiercache.micrometer.MicrometerCacheMetrics} publishes the core's
 * metrics events (requests, latency, invalidations, degradation, null
 * entries, stale-while-revalidate activity) as Micrometer meters;
 * {@link io.tiercache.micrometer.TiercacheTracing} turns L2 operations and
 * inbound invalidation processing into OpenTelemetry spans; and
 * {@link io.tiercache.micrometer.TiercacheInspection} exposes cache state
 * (hit ratios, breaker state, journal size) over JMX.
 *
 * <p>These classes implement the {@code io.tiercache.spi} listener
 * interfaces and are wired by the Spring Boot starter's auto-configuration,
 * or registered programmatically with
 * {@code TierCacheFactory.Builder.metricsListener(...)}.
 *
 * @since 0.1.0
 */
package io.tiercache.micrometer;
