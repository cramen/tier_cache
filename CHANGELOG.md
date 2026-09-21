# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.0] - 2026-09-21

### Added

- Degradation stale window (`degradationStaleTtl` / `tiercache.degradation-stale-ttl`, per cache, default off): during an L2 outage, physically retained but logically expired L1 entries can be served stale without hitting the source — the failure mode that overloaded origins in load testing. Freshness is tracked by engine-stamped per-entry deadlines (never the L2 write timestamp), stale serving is breaker-gated (OPEN, or HALF_OPEN without a probe permit — probes always flow so the breaker can close), fresh accesses slide the horizon without shortening the store-time retention floor, stale accesses never extend it, and every stale serve is counted (`tiercache.requests{result="stale_degraded"}`). `CacheSettings` gained the component with a compatible old-arity constructor; `CacheOverride`, both starter property bindings, and the Kotlin DSL are covered. See `docs/configuration.md` for the memory trade-off and the residual contract for full-outage writes (not healed by replay; bounded by the stale L2 copy's remaining TTL, not one L1 TTL).

### Fixed

- The Spring Boot starter and the Micronaut integration now build the invalidation engine with the application's configured `CacheMetricsListener` instead of silently selecting the no-op listener, so invalidation traffic (sent, received, replayed, dropped) is observable like every other metric family; without a listener the wiring falls back to NOOP explicitly.
- An in-flight load can no longer overwrite a concurrent write or invalidation: the store version is minted at claim time (before the loader runs), so a write or versioned eviction landing during the load wins the conditional store — previously a slow loader's stale snapshot won last-write-wins and every node served stale data until TTL. A load losing to an eviction performs at most one bounded reload instead of returning a false "not found" (at most two loader executions per request). Every L1 state change now goes through one atomic per-key commit over a unified holder (entry + version + invalidation barrier): invalidations are barriered even for absent keys, equal versions are admitted (an UPDATE lifts the barrier and installs its payload in one step), local puts/evicts participate in the same commit, and a per-cache generation guard refuses stale L1 writes when protective state is evicted mid-flight — the L1 fill alone is refused, never converted into an artificial miss.
- A rebuild lock can no longer be orphaned by an ambiguous acquire: when the acquire command's outcome is unknown (client-side timeout while the server may have executed it), the provider schedules a best-effort token-checked compensating release, retried until it deletes or the compensation window (`max(2 × lease, 30 s)`) expires — previously the orphan sat for the full lease and blocked rebuilds (a bench read waited 10.57 s). The token check makes the compensation safe against every outcome; retries are bounded per provider (small worker pool, pending cap with drop-on-overflow), and the documented residual — a SET executing only after the window — self-expires within one lease. Lock providers now have defined ownership: library-created providers are closed by the creating factory or as managed beans (destroy callback), wrappers forward `close()`, and provider connections open lazily so wiring never fails on an unreachable server at startup.

## [1.3.0] - 2026-09-20

### Fixed

- Closing the factory now fails outstanding async operations instead of abandoning them: every stage handed out through `AsyncTierCache` views is tracked and completed with `CancellationException` on `TierCacheFactory.close()`, so callers waiting on queued (never-started) operations unblock with a visible failure instead of hanging forever.
- The async close protocol is now race-free: submission (state check + registry + executor hand-off) and the close drain share a per-view lifecycle lock, and view creation (`asyncCache`) and the factory's CLOSED transition share a factory-level lock — a close landing anywhere in the submit path either rejects the submission or drains the registered stage, and no view can be published after the drain snapshot. Two visible consequences: a submission after close now fails its stage with `CancellationException` (previously `RejectedExecutionException`), and `asyncCache(name)` on a closed factory throws `IllegalStateException` (previously returned a view nothing would ever drain).
- Tag indexes are now bounded under every workload: the per-key reverse index keeps the data entry's exact TTL, each per-tag member set's TTL is extend-only and assigned atomically with the member add (one Lua call) — a shorter-lived entry can no longer shrink the shared index (mixed TTLs), and sets of never-touched-again tags expire within the longest member TTL. Dead members are reclaimed atomically (janitor sampling on writes plus read-time pruning, both check-and-remove in the same Lua call): tag lookups never return phantom entries, a concurrent rewrite can never lose its membership, and steady-state set size stays within the stated statistical bound under uninterrupted writes. See UPGRADING.md for the transition notes (pre-existing shrunken set TTLs do not self-repair; mixed-rollout caveats).
- Replay cursor correctness: the replay position now marks the end of the contiguous applied prefix of the journal — live deliveries are tracked in a bounded (128-version) applied window, and the cursor advances on a bounded cadence over exactly the rows already applied, never to the stream end and never past an unconsumed row (previously, a writer that journaled a row but delayed its publish could have that row skipped after a reconnect; a row written mid-replay could likewise be skipped). Every cursor read is one atomic checked read (integrity proof and range from the same response): if the cursor's own row was trimmed, the receiver takes the flush path with the flush signal instead of mistaking surviving rows for a contiguous prefix; a window overflow behind a delayed row falls back to journal-driven catch-up. Trim detection for a beginning cursor (`0-0`) is now exact — counted atomically on every journal-appending path — including the boundary where the stream length equals the capacity exactly (previously misread as "no loss"). A trim-counter read failure takes the flush path with a warning, never "no loss". Operational note: the journal capacity must exceed the cursor cadence (64 events); below that, integrity is unconfirmable on every tick and the flush path fires by design (see docs/sizing-and-ttl.md).
- The L1 flush path can no longer swallow an invalidation: the journal baseline is now captured BEFORE the local cache is cleared, so a write journaled between the clear and the baseline establishment stays ahead of the cursor and is replayed instead of being silently accounted (previously the stale re-warmed entry survived). The order is baseline → clear → settle state → notify observers, so a listener/metrics failure cannot cancel the mandatory clear; if the baseline read itself fails, the previous confirmed cursor is kept — the cursor never jumps past unread rows.
- Applied-version tracking is now hard-bounded (at most 768 versions per cache: 512 window hard cap + 256 confirmed, plus one bounded read batch): versions the cursor already passed are recorded in a bounded confirmed-set and late duplicates are applied to L1 without being re-tracked (previously 1,000 re-delivered events were retained forever at a nominal cap of 128); every catch-up pass ends in an explicit CAUGHT_UP / MORE_WORK / FAILED outcome — only a pass that reached the journal end clears the window; and on read failures the service enters a resync-required state (tracking memory freed, retries throttled to one per second, cursor never advanced without confirmed rows). Recovery is deliberately lazy: without new deliveries or a reconnect it stays incomplete and L1 may serve stale data until the next trigger, which then applies the missed rows.
- `evictAll` is now journaled and covered by reconnect replay: the `RemoteCache` SPI gained a versioned `clear(Version)` (defaulting to `clear()`, so custom transports keep their current behavior), the engine routes `evictAll` through it, and the Redis transport appends the EVICT_ALL journal row atomically with the namespace clear. Previously a receiver that missed the live EVICT_ALL notification kept serving stale L1 entries until TTL.
- UPDATE-mode journal replay now deserializes the payload with the value serializer before applying it to L1, matching the live Pub/Sub path; previously a replayed UPDATE stored the raw serialized bytes as the value (typed reads could hit `ClassCastException`).
- Direct journal appends now serialize the UPDATE payload with the value serializer (previously the key serializer), staying symmetric with replay deserialization when key and value serializers differ. The standard write path (payload framed by the L2 scripts) is byte-identical before and after. Rows misencoded before this fix are not repaired retroactively — they age out within the journal window.

### Changed

- Versions moved from millisecond to microsecond resolution (`max(epochMicros, previous + 1)`), shrinking the cross-instance simultaneity window a thousandfold: two instances writing within one millisecond now order by real time. Same-microsecond writes remain ordered by the instance-ID tiebreak — the documented last-write-wins trade-off; a strict "later always wins" guarantee would require per-write coordination and stays out of scope by design. Wire format and Redis scripts are unchanged; on platforms with millisecond-granular clocks the scheme degrades to the 1.2.0 behavior.

## [1.2.1] - 2026-09-20

### Fixed

- A revalidation rejected by the saturated background pool no longer hangs readers: the pool switched from `DiscardPolicy` to `AbortPolicy`, and the rejection path now completes the in-flight claim exceptionally before releasing it, so waiters fail fast and later reads retry instead of joining a never-completing future. Introduced in 1.2.0 with the bounded pool; reproduced and reported externally.

## [1.2.0] - 2026-09-20

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
