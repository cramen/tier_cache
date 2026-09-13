# Tiercache

[![CI](https://github.com/cramen/tier_cache/actions/workflows/ci.yml/badge.svg)](https://github.com/cramen/tier_cache/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**Tiercache** is a family of JVM libraries providing a correct-out-of-the-box **two-level cache**: L1 in-process (Caffeine) + L2 Redis/Valkey, with cross-instance invalidation, built-in protection against the classic high-load failure modes (stampede, avalanche, penetration, L2 degradation), and first-class observability. A framework-independent core with thin adapters — pull in exactly one starter module and keep your existing cache code.

Status: 0.x, incubating API (may change before 1.0). The cache is eventually consistent by design; no strong-consistency guarantees are given or implied.

## Features

- **Two-level read cascade** — L1 (shaded Caffeine, zero-allocation hit path) → L2 (Redis/Valkey) → your loader. An L2 hit always warms L1, so the next read of the same key is served in-process.
- **Correct by default** — singleflight per instance plus cluster-wide rebuild coordination (distributed lock with watchdog lease extension and mandatory double-check), TTL jitter, fail-fast TTL-ordering validation, null caching with a tri-state `lookup`, atomic `putIfAbsent` — all on without configuration; disabling requires an explicit opt-in and is logged as a risk.
- **Cross-instance invalidation** — versioned events with last-write-wins ordering, a bounded journal with replay on reconnect, and two transport profiles: lightweight Pub/Sub or durable Redis Streams.
- **Honest degradation** — a circuit breaker switches the cache to L1-only when Redis fails; business code never sees infrastructure exceptions. Recovery replays the missed invalidations and never flushes L1 (no healing-partition stampede).
- **Stale serving** — stale-while-revalidate and XFetch early refresh keep hot keys fast while values refresh in the background.
- **Observability as a feature** — Micrometer metrics for every failure mode, OpenTelemetry tracing, JMX inspection, and a reference Grafana dashboard with alert rules in [`docs/grafana/`](docs/grafana/).
- **Kotlin coroutines** — `suspend` API, invalidation `Flow`, and a `tierCache { }` config DSL in `tiercache-kotlin`.
- **GraalVM Native Image** — reachability metadata ships inside the published jars; the demo application compiles natively in CI.

## Quick start

### Spring Boot

```kotlin
// build.gradle.kts
implementation("io.tiercache:tiercache-spring-boot-starter:0.1.0-SNAPSHOT")
```

```yaml
# application.yml
tiercache:
  enabled: true
  redis-uri: redis://localhost:6379
```

The starter replaces the standard cache manager: `@Cacheable` / `@CachePut` / `@CacheEvict` code works unchanged, backed by the two-level cache. Per-cache overrides live under `tiercache.caches.<name>.*`; invalid configuration (e.g. L1 TTL > L2 TTL) aborts startup with an actionable error. A runnable demo lives in [`examples/demo-spring`](examples/demo-spring/) (docker-compose included).

### Programmatic

```kotlin
// build.gradle.kts
implementation("io.tiercache:tiercache-core:0.1.0-SNAPSHOT")
implementation("io.tiercache:tiercache-transport-redis:0.1.0-SNAPSHOT")
```

```java
import io.tiercache.TierCache;
import io.tiercache.TierCacheFactory;
import io.tiercache.redis.LettuceRemoteCache;

try (LettuceRemoteCache<String, String> l2 = LettuceRemoteCache
        .<String, String>builder("redis://localhost:6379")
        .cacheName("users")
        .build();
     TierCacheFactory factory = TierCacheFactory.builder()
        .remoteCache(l2)
        .build()) {

    TierCache<String, String> cache = factory.getCache("users");
    String name = cache.getOrCompute("user:42", key -> loadFromDatabase(key));
}
```

Reads cascade L1 → L2 → loader; concurrent loads of the same key share one loader execution, per instance and cluster-wide. All protections (singleflight, rebuild coordination, TTL jitter, circuit breaker) are active in this snippet with no extra configuration.

## Modules

| Module | What it gives you |
|---|---|
| `tiercache-spring-boot-starter` | Spring Boot 3 auto-configuration — the one dependency most Spring apps need |
| `tiercache-core` | The framework-independent cache engine: cascade, singleflight, rebuild coordination, degradation |
| `tiercache-transport-redis` | Lettuce-backed Redis/Valkey L2 and invalidation transport |
| `tiercache-kotlin` | Coroutines API: suspend facade, invalidation Flow, config DSL |
| `tiercache-micrometer` | Micrometer metrics, OpenTelemetry tracing, JMX inspection |
| `tiercache-invalidation` | Invalidation protocol internals (pulled in transitively by the transport) |
| `tiercache-tck` | Public chaos-test suite and benchmarks |

## Requirements

- Java 17 or newer
- Redis 6.2+ or Valkey (for L2 and cross-instance features)
- Docker, to run the integration tests and TCK chaos suite locally

## Documentation

- [Configuration reference](docs/configuration.md)
- [Migration from Spring Cache](docs/migration-from-spring-cache.md)
- [Observability: metrics, tracing, dashboards](docs/observability.md)
- [Running the TCK chaos suite](docs/tck.md)
- [Grafana dashboard and alert rules](docs/grafana/)
- [Security policy and supply-chain verification](SECURITY.md)
- [Demo application](examples/demo-spring/)

## License

[Apache License 2.0](LICENSE)
