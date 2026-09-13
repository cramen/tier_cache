# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

First milestone: the complete two-level cache stack — core engine, Redis transport, invalidation protocol, degradation handling, observability, Kotlin and Spring Boot integrations, GraalVM support, and the quality-gate infrastructure.

### Added

#### Core two-level cache

- L1 → L2 → loader cascade read path; an L2 hit always warms L1.
- Singleflight: concurrent loads of one key share a single loader execution per instance.
- Distributed rebuild coordination: cluster-wide rebuild lock with watchdog lease extension and mandatory double-check after lock acquisition.
- TTL jitter (5–10%) against synchronized expiry, and fail-fast startup validation of TTL ordering (`TTL_L1_effective ≤ TTL_L2`) and other invalid configurations.
- Atomic `putIfAbsent` backed by the L2 `SET NX PX` primitive.
- Null caching with per-cache policy and a tri-state `lookup` (miss / cached-null / hit).
- Shaded Caffeine L1 (relocated under `io.tiercache.internal.caffeine`); L1/L2/lock SPI for alternative implementations.
- Lettuce-backed Redis/Valkey L2 transport and lock provider.

#### Cross-instance invalidation

- Versioned invalidation events with last-write-wins ordering and an instance-identity version generator.
- Bounded journal with replay of missed invalidations after reconnect.
- Two transport profiles: lightweight Pub/Sub and durable Redis Streams.
- UPDATE mode (payload-bearing events), tag-based and batch eviction.

#### Degradation handling

- Circuit breaker on L2 failure: the cache switches to L1-only mode and no infrastructure exceptions escape into business code.
- Honest degraded mode: narrowed cross-instance atomicity is surfaced via metric and log.
- Controlled recovery: journal replay before the breaker closes; L1 is never flushed on reconnect.

#### Observability

- Micrometer metrics for every failure mode: request outcomes per level, L2 latency, invalidation flow, journal size, degraded state, breaker state, last load age, null entries.
- OpenTelemetry tracing of L2 operations and invalidation processing.
- JMX inspection.
- Reference Grafana dashboard and alert rules (`docs/grafana/`).

#### Stale serving

- Stale-while-revalidate and XFetch early refresh for hot keys, on a daemon revalidation executor.

#### Kotlin coroutines API

- `KTierCache` suspend facade, invalidation `Flow`, and a `tierCache { }` configuration DSL (`tiercache-kotlin`).

#### Spring Boot starter

- Spring Boot 3.5.x auto-configuration replacing the standard cache manager — existing `@Cacheable` code works unchanged.
- Spring Cache SPI adapters, relaxed properties binding (`tiercache.*`) with fail-fast validation, per-cache overrides.
- Demo application in `examples/demo-spring`.

#### GraalVM Native Image

- Reachability metadata shipped in the published jars (Caffeine cache-implementation family in core; Netty jctools queues, `ResourceLeakDetector.addExclusions`, and JDK string serialization in the transport).
- Native smoke CI job compiling the demo application on a GraalVM 25 toolchain.

#### Quality gates

- Public TCK chaos suite (Testcontainers): stampede (single- and multi-instance), avalanche, penetration, Pub/Sub loss, invalidation/write race, Redis degradation, reconnect storm, and Spring Cache migration.
- Churn soak gate (bounded memory growth and journal size) and a virtual-thread pinning gate (zero `jdk.VirtualThreadPinned` on library frames).
- PIT mutation gate (≥75% kill score on core internals and invalidation) and a ≥90% branch-coverage gate on `tiercache-core`.
- JMH budgets: L1-hit overhead and zero-allocation path, two-level throughput, invalidation propagation latency (p99 ≤ 5 ms).
- Dependency audit asserting the core runtime classpath exposes only the SLF4J API.

#### CI and supply chain

- GitHub Actions CI: JDK 17/21/25 build matrix, nightly gates (soak, PIT, reproducible build, native smoke, informational benchmarks and CVE scan).
- CycloneDX SBOM published per release; Sigstore keyless signing and SLSA build provenance for release artifacts.
- Reproducible-build gate for the core shaded jar.
