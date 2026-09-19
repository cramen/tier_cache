# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `tiercache.async-executor-threads` / `TierCacheFactory.Builder.asyncExecutorThreads(int)` (also exposed on the Kotlin DSL, `KTierCacheFactory.Builder`, and `ReactorCacheFactory.Builder`): thread cap for the bounded async executor, default `max(4, availableProcessors)`.
- `docs/sizing-and-ttl.md`: journal-capacity sizing guidance (disconnect window x invalidation rate, and what happens on overflow).

### Changed

- Docs accuracy: the recovery description no longer claims L1 is "never" flushed — it is replay-based within the journal window and flushes for affected caches on window overflow or when no journal is wired (signalled via log, the `tiercache.invalidation{direction="dropped"}` metric, and a listener callback). README now states that `@Cacheable` load coalescing requires `sync = true` and that null caching is opt-in (`null-policy: allow`).
- The async executor is now bounded with a bounded queue (10,000): previously `AsyncTierCache` operations ran on an unbounded cached thread pool shared with background revalidation. Under saturation, submissions fail their `CompletionStage` with `RejectedExecutionException` instead of growing threads without bound. Background SWR/XFetch revalidation runs on its own small bounded pool and no longer competes with latency-sensitive async work. See `UPGRADING.md` for the saturation behavior change.

### Fixed

- Cross-instance write ordering: versions were per-instance counters starting at 1, so a freshly started instance's writes (and evicts) could be rejected as stale against a long-running instance's earlier writes on the same key — newer changes silently lost with no concurrency involved. Versions are now time-ordered (wall-clock hybrid sequence: `max(epochMillis x 1000 + per-millis counter, previous + 1)`), so writes order by real time across instances; the wire format and Redis scripts are unchanged. See `UPGRADING.md` for the rolling-upgrade note and the clock-sync expectation.
- Reconnect replay for Spring/Micronaut-wired caches: the invalidation journal was written under the namespaced cache name (`spring:users` / `micronaut:users`) while the recovery path replayed the logical name (`users`), so missed invalidations were never replayed and stale L1 entries survived until TTL. The journal now follows the logical cache name in all wirings (`LettuceRemoteCache.Builder.journalName`, defaulting to `cacheName`; programmatic prefix-free wiring unchanged).

## [1.1.0] - 2026-09-18

### Added

- `tiercache-micronaut`: Micronaut integration — a `CacheManager` replacing Micronaut's `DefaultCacheManager`, `SyncCache`/`AsyncCache` adapters over the core engine, `@ConfigurationProperties("tiercache")` with the same keys and defaults as the Spring Boot starter, default wiring parity (shared Redis client, per-cache L2 namespacing, Pub/Sub invalidation profile, conditional Micrometer metrics). Micronaut `@Cacheable`/`@CachePut`/`@CacheInvalidate` code works unchanged.
- `docs/configuration.md`: "Micronaut binding notes" section (constructor binding, `@EachProperty` per-cache beans, metrics conditional).
- `docs/migration-from-redisson.md`: migration guide from Redisson `RLocalCachedMap` (API mapping, reconnection semantics, feature parity and gaps).
- `docs/migration-from-jetcache.md`: migration guide from JetCache (annotation and config mapping, before/after examples).
- `docs/sizing-and-ttl.md`: practical guide to L1 sizing, L1:L2 TTL ratios, jitter, per-cache overrides, and the metrics to watch while iterating.
- README section "When Tiercache is not the right tool": honest anti-adoption guidance (strong consistency, single instance, tiny working sets, write-mostly workloads).

### Changed

- CI workflows moved to current action majors: `actions/checkout` v7, `actions/upload-artifact` v7, `actions/setup-java` v6, `gradle/actions/setup-gradle` v6.3.0 (Node 20 deprecation cleanup).

### Removed

- The soak gate is no longer part of CI: hosted runners kill long jobs before a meaningful soak completes. Run it locally or on your own hardware via `./gradlew :tiercache-tck:soakTest` (default PT10M, `-Dtiercache.soak.duration=PT24H` for the full profile).

## [1.0.0] - 2026-09-14

GA release. No runtime behavior changes versus 0.4.0.

### Added

- Complete Javadoc with `@since` tags across all published modules, plus package-level documentation.
- `UPGRADING.md` with per-release upgrade notes.
- "Compatibility and versioning" policy documented in the README: semantic versioning, the definition of the public API, and deprecation/removal rules.

### Changed

- CI workflows moved to `actions/setup-java` v5.

## [0.4.0] - 2026-09-13

### Changed

- The absolute two-level throughput budget (1M ops/s per instance) is dropped from the requirements: absolute throughput depends on the hardware and network environment the code runs on, so it is not a fair library contract. The throughput benchmarks stay as reproducible trend/regression measurements; budgets, if any, are defined per reference environment.


### Added

- `tiercache-reactor`: Reactor bridge over the async view — `ReactorTierCache` Mono facade (all operations; sync and Mono loader forms of `getOrCompute`, coalescing stays in the engine's singleflight) and `ReactorCacheFactory` exposing inbound invalidation events as a cold `Flux` with bounded per-subscriber buffering and a drop signal.

## [0.3.0] - 2026-09-13

### Added

- `AsyncTierCache`: a non-blocking `CompletionStage` view of the cache (all operations, plus sync and async loader forms for `getOrCompute`), obtained via `TierCacheFactory.asyncCache(name)`; coalescing stays in the engine's singleflight.
- The Spring Boot starter now registers the factory-level gauges (`tiercache.degraded`, `tiercache.breaker.state`, `tiercache.journal.size`, `tiercache.last.load.age`) eagerly — they were previously never registered in the Spring path.

### Changed

- `KTierCache` suspend operations now await the core's async view instead of offloading blocking calls to `Dispatchers.IO`; loader failures reach coalesced callers directly (unwrapped) instead of `CompletionException`-wrapped. Public Kotlin API is unchanged.
- The shaded core's Gradle `.module` metadata now matches its POM (shaded jar as the default variant, no Caffeine leak; previously Gradle-metadata consumers got the unshaded jar).


## [0.2.0] - 2026-09-13

Second release: multi-instance hardening. Two significant defects found and fixed by Docker cluster testing behind a load balancer (3-8 application instances + Redis + nginx), plus per-cache L2 isolation.

### Added

- `TierCacheFactory.Builder.remoteCacheFactory(...)`: provide the L2 per cache name (memoized) for full key-space isolation between named caches.
- TCK: two-instance per-cache isolation suite over real Redis and Valkey (same key in two caches is independent data; per-cache `evict`/`evictAll` is cache-scoped).
- `examples/demo-spring/cluster/`: multi-instance test harness (Dockerfile, nginx balancer, compose, load driver) used for the cluster verification.

### Fixed

- The Spring Boot starter created the shared Redis client with Lettuce defaults (60 s command timeout), so an L2 outage hung business requests instead of tripping the circuit breaker; the client now carries the documented fast timeouts (100 ms connect, 250 ms command). Found by the cluster test.

### Changed

- Spring Boot starter users: the L2 key layout is now namespaced per cache — each named cache gets its own Redis key prefix, fixing cross-cache key collisions and making per-cache `evictAll` correct. Entries written by older versions become unreachable after the upgrade (a cold-start effect: one reload per key, not corruption); acceptable pre-1.0.
- The demo's expensive-computation endpoint uses `@Cacheable(sync = true)` so concurrent misses coalesce through the value-loader path; the default `sync = false` get/put flow bypasses stampede protection (documented behavior of Spring Cache).


## [0.1.0] - 2026-09-13

First milestone: the complete two-level cache stack — core engine, Redis transport, invalidation protocol, degradation handling, observability, Kotlin and Spring Boot integrations, GraalVM support, and the quality-gate infrastructure.

Published to Maven Central under the `io.github.cramen` group ID (Java packages remain `io.tiercache`).

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
