# TierCache documentation

Start with the [quick start](../README.md#quick-start). These guides
cover configuring, integrating and operating TierCache in an application.

## Configure and integrate

- [Configuration reference](configuration.md): properties, defaults, validation,
  serialization, async execution and invalidation profiles.
- [Sizing and TTL](sizing-and-ttl.md): choose capacity, freshness and journal retention.
- [Supported platforms](compatibility.md): Java, Spring Boot, Redis/Valkey and Sentinel scope.
- [Resource ownership and shutdown](resource-lifecycle.md): what the factory owns,
  custom resources, async followers and closing an application.

## Migrate and upgrade

- [Spring Cache](migration-from-spring-cache.md)
- [Redisson](migration-from-redisson.md)
- [JetCache](migration-from-jetcache.md)
- [Upgrade policy and behavior changes](../UPGRADING.md)
- [Redis keyspace and coordinated migration](redis-keyspace-v2.md)

## Operate and troubleshoot

- [Observability](observability.md): metrics, tracing, JMX and publication outcomes.
- [Grafana dashboard](grafana/tiercache-dashboard.json) and [alert rules](grafana/alerts.yml).
- [Invalidation recovery](recovery.md): reconnects, HALF_OPEN, replay and safe fallback.
- [Streams recovery](streams-recovery.md): pending entries, acknowledgements and reset limits.
- [Troubleshooting](troubleshooting.md): symptoms, signals and operational checks.

## Verify an integration or release

- [TCK compliance suite](tck.md): consume the published tests and understand their scope.
- [Release verification](release-evidence.md): inspect provenance, signatures and dependency evidence.
- [Security policy](../SECURITY.md)

Repository development and release procedures are maintained separately in
[maintenance](../maintenance/README.md).
