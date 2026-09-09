# AGENTS.md — Tiercache

## What this project is

**Tiercache** is a family of JVM libraries providing a correct-out-of-the-box **two-level cache**: L1 = in-process (Caffeine), L2 = Redis/Valkey, with cross-instance invalidation, built-in protection against high-load failure modes (stampede, avalanche, penetration, L2 degradation), and first-class observability.

Positioning: an enterprise-grade infrastructure library in the spirit of SLF4J / Caffeine / Resilience4j — a framework-independent core with thin adapters, a predictable release cycle, and supply-chain guarantees.

The full rationale, pain catalog, and normative requirements live in `research/` (Russian source documents; the project itself is English-only):

- `research/Исследование_пробелов_JVM_библиотек_микросервисы.md` — market/gap analysis: why this niche is open.
- `research/Двухуровневый_кэш_JVM_анализ_боли_и_требования.md` — pain catalog (8 pains) and derived requirements.
- `research/ТЗ_двухуровневый_кэш_enterprise.md` — the authoritative technical specification (v1.1) and the 13-month roadmap.

When requirements conflict or are ambiguous, the TZ (`ТЗ_...enterprise.md`) wins. TZ requirement IDs are internal to the local `research/` and `openspec/` directories — they must NOT appear in repo files or commit messages; describe the behavior in plain English instead.

## Language policy

Everything in this repository is **English only**: code, comments, commit messages, README, docs, ADRs. The `research/` folder is the sole exception (read-only Russian source material; do not translate or modify it).

## Repository state

Gradle (Kotlin DSL) multi-module build, Java 17 toolchain. Modules present: `tiercache-core` (cascade read path, singleflight, cluster-wide rebuild coordination, null caching, config validation, stale-while-revalidate/XFetch, shaded Caffeine L1, L1/L2/lock SPI), `tiercache-invalidation` (invalidation protocol: versioned messages, journal, replay, last-write-wins), `tiercache-transport-redis` (Lettuce-backed L2 + lock provider, Pub/Sub and Streams invalidation profiles, Redis 6.2+/Valkey contract-tested), `tiercache-spring-boot-starter` (Spring Boot 3.5.x auto-config, Cache SPI adapters, Spring Cache migration gate), `tiercache-micrometer` (Micrometer metrics + OTel tracing, JMX inspection), `tiercache-kotlin` (Kotlin coroutines API: `KTierCache` suspend facade, invalidation `Flow`, `tierCache { }` config DSL), `tiercache-tck` (chaos harness: stampede full form, avalanche, penetration, degradation, reconnect storm), `examples/demo-spring` (quick-start demo).

## Build & test commands

- `./gradlew build` — compile, unit tests, TCK tests, dependency audit, coverage gate (≥90% branch on `tiercache-core`).
- `./gradlew :tiercache-core:test` — core unit/contract tests.
- `./gradlew :tiercache-tck:test` — TCK chaos tests (Testcontainers: stampede single- and multi-instance over real Redis).
- `./gradlew :tiercache-tck:soakTest` — churn soak gate (memory ≤5% growth, bounded journal; default PT10M, override with `-Dtiercache.soak.duration=PT24H` for the full CI profile). Excluded from `check`.
- `./gradlew :tiercache-tck:vtStressTest` — virtual-thread pinning gate (100k VTs, zero `jdk.VirtualThreadPinned` on library frames). Requires a JDK 21+ toolchain; skipped loudly otherwise. Excluded from `check`.
- `./gradlew :tiercache-transport-redis:test` — transport contract suite against Redis 6.2 and Valkey containers (needs Docker).
- `./gradlew :tiercache-spring-boot-starter:test` — Spring adapter, auto-config, and Spring Cache migration tests (no Docker needed).
- `./gradlew :tiercache-kotlin:test` — Kotlin coroutines API tests (no Docker needed).
- `./gradlew :examples:demo-spring:test` — demo smoke test (Docker; not part of `check`).
- `./gradlew :tiercache-core:shadowJar` — shaded artifact: Caffeine relocated under `io.tiercache.internal.caffeine`; the shaded jar is the main artifact, the plain jar keeps the `unshaded` classifier.
- `./gradlew :tiercache-core:dependencyAudit` — asserts the runtime classpath exposes only SLF4J API.
- `./gradlew :tiercache-core:jmh` — JMH baseline for the L1-hit hot path (gc profiler: overhead and zero-allocation budgets); results in `tiercache-core/build/results/jmh/results.txt`.
- `./gradlew :tiercache-core:pitest :tiercache-invalidation:pitest` — PIT mutation gate (≥75% kill score; core targets `io.tiercache.internal.*`, invalidation targets `io.tiercache.invalidation.*`). On demand only — deliberately NOT wired into `check`; belongs to CI/nightly.

## Module structure (target)

| Module | Purpose | Dependency rules |
|---|---|---|
| `tiercache-core` | L1→L2→loader cascade, singleflight, TTL jitter, SWR/XFetch, null-cache, circuit breaker, configuration | **Zero mandatory deps except SLF4J API**; Caffeine shaded/optional; framework-agnostic |
| `tiercache-invalidation` | Invalidation protocol: versioned messages, journal, replay, last-write-wins | depends on `core` only |
| `tiercache-transport-redis` | L2 + invalidation transport over Lettuce (default) / Redisson (optional); Pub/Sub and Redis Streams profiles | depends on `invalidation` + chosen client |
| `tiercache-spring-boot-starter` | Auto-configuration, Spring Cache SPI (`CacheManager`/`Cache`), multilevel `Cache.retrieve(...)`, properties binding with validation | Spring Boot 3.x/4.x |
| `tiercache-kotlin` | `suspend` API, `Flow` invalidations, config DSL, coroutine-native coalescing (no thread blocking) | kotlinx-coroutines |
| `tiercache-micrometer` | Micrometer metrics + OTel tracing, JMX/REST inspection | SPI on `core` |
| `tiercache-tck` | Public chaos-test suite (Testcontainers) + JMH benchmarks | all modules |

A user pulls in exactly **one starter module**. Never let framework or client dependencies leak into `core` (enforced by dependency audit in CI).

## Non-negotiable design principles

These are the product's identity. Violating them is a bug, not a trade-off:

1. **Correct by default.** All protections — singleflight, distributed rebuild coordination with double-check and watchdog lease, TTL jitter 5–10%, TTL ordering `TTL_L1_effective ≤ TTL_L2` with fail-fast startup validation on violation, null-caching policy, L2 timeouts below business timeout — are ON without configuration. Disabling requires explicit opt-in and is logged as a risk. Known DIY traps (missing double-check after lock acquisition, explicit `leaseTime` killing the watchdog, L1 never warmed from L2) must be impossible **by API construction**, not by documentation.
2. **Never claim strong consistency.** The cache is eventually consistent by design. All artifacts — docs, logs, exceptions, marketing text — describe a bounded, measurable staleness window only.
3. **Observability as a feature.** Every failure mode has a metric (`tiercache.requests{result=...}`, `tiercache.latency{level=...}`, `tiercache.invalidation{direction=...}`, `tiercache.journal.size`, `tiercache.degraded`, `tiercache.breaker.state`, ...). Every chaos test must be diagnosable from metrics alone.
4. **Degradation is honest.** On L2 failure: circuit breaker → L1-only mode, zero infrastructure exceptions escaping into business code; loss of cross-instance `putIfAbsent` atomicity is surfaced via `tiercache.degraded=1` metric + log + docs. Recovery: journal replay → rate-limited warm-up → breaker close; **instant full L1 flush on reconnect is forbidden**.
5. **Zero migration threshold.** Migration from standard Spring Cache = swap the starter + one config line; existing `@Cacheable` code unchanged.

## Explicit non-goals (fixed scope boundary)

Do not implement, and reject proposals for:

- Own Redis client (transport builds on Lettuce/Redisson)
- Strong consistency
- L1 persistence to disk
- Write-behind / write-through to a database
- Service discovery

## Coding conventions

- **Java 17 baseline**; no language features beyond 17 in `core`.
- **Virtual-thread safety:** no `synchronized` on I/O paths (pinning); verified by the virtual-thread stress test (zero `jdk.VirtualThreadPinned` JFR events on library paths under 100k virtual threads).
- **Zero allocations** on steady-state L1-hit path (verified by JMH gc profiler).
- Core depends on **SLF4J API only** for logging.
- Secrets (e.g. Redis passwords) are never logged at any level, including diagnostic mode.
- Async surface: `CompletionStage` in core, `Mono` via reactor bridge, `suspend`/`Flow` in the Kotlin module; coroutine code must not block threads.
- Configuration model: global defaults + per-cache overrides, relaxed binding, **fail-fast startup validation** with actionable error messages.

## Testing & quality gates

Testing culture is TDD-adjacent and chaos-first. Every pain in the catalog has a paired chaos test in `tiercache-tck`: stampede, avalanche, Pub/Sub loss, invalidation/write race, Redis degradation, reconnect storm, 24h soak, penetration, virtual-thread stress, and Spring Cache migration. Tests must be reproducible locally via Testcontainers.

Quality gates (enforced in CI once set up):

- Branch coverage of `core` ≥ 90%; PIT mutation score ≥ 75% on invalidation/degradation paths.
- JMH benchmarks as regression gates: L1-hit overhead ≤ +50% over raw Caffeine, ≥ 1M ops/s/instance two-level reads, zero steady-state allocations; > 10% regression blocks merge.
- All observability metrics asserted by tests.
- Test matrix: Redis 6.2+ and Valkey, standalone/Sentinel/Cluster, JDK 17/21/25.

## Performance targets (for orientation)

- L1 hit ≈ 0.0012 ms vs ~0.45 ms Redis (~95% latency reduction on hot reads).
- Two-level throughput target ≥ 1M ops/s per instance (reference benchmark: 1.8M ops/s from `caffeinated-redis`).
- Invalidation propagation p99 ≤ 5 ms within one AZ (Pub/Sub profile).

## Roadmap (13 months to GA)

| Phase | Months | Content | Checkpoint |
|---|---|---|---|
| 0. Architecture | M0–1 | ADRs (consistency model, invalidation protocol, SPI), API spec | Public API + consistency model freeze |
| 1. MVP | M1–3 | `core` + `spring-boot-starter` + Pub/Sub transport; two-level cascade, singleflight, rebuild coordination, TTL jitter, null caching; basic metrics | Stampede, avalanche, and Spring Cache migration tests green |
| 2. Invalidation | M3–6 | Journal + replay + Streams profile, last-write-wins, UPDATE mode | Pub/Sub loss and invalidation/write-race tests green on Redis/Valkey |
| 3. Degradation & observability | M6–8 | Circuit breaker, metrics/tracing, Grafana dashboard | Redis degradation and reconnect-storm tests green |
| 4. Expansion & hardening | M8–11 | Kotlin module, GraalVM metadata, SWR/XFetch, soak + mutation gates | Degradation, reconnect, soak, penetration, and virtual-thread tests green; performance budgets met |
| 5. Enterprise GA | M11–13 | SBOM/signing/SLSA, offline delivery, docs, TCK publication | GA: full TCK green, release 1.0 |

Work should land in roadmap order — do not build phase 2+ features before the phase 1 core exists.

## Enterprise / supply-chain commitments (for release tooling)

- SBOM (CycloneDX) published per release; artifact signing (Sigstore/cosign + PGP); reproducible builds for `core`.
- Public SECURITY.md; fix SLAs: critical 7 days, high 30 days; target SLSA Level 3.
- Dependencies with known CVEs block release; daily scanning.
- Support last two LTS JDK lines and last two major Spring Boot lines; 12-month security backports.
- Offline delivery: build must work in an isolated network, no external CDN calls.

## Competitive context (why decisions look this way)

- **Spring Cache** has no multi-level support — `CompositeCacheManager` never warms L1 on L2 hit; this defect must be impossible by construction here.
- **JetCache** is frozen; **Redisson** paywalls near-cache eviction and per-entry TTL (PRO); **Hazelcast** is a platform, not a library over existing Redis; **caffeinated-redis** lacks stampede protection, degradation handling, and null semantics. Our differentiators: completeness of failure-mode protection, observability, zero-config migration, and infrastructural reliability.
- Framework market is fragmented (Spring Boot ~42%, Micronaut ~39%) — hence framework-independent core + thin adapters. Post-GA candidates: Micronaut/Quarkus modules via the same SPI, without bloating core.

# Coding guide

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
