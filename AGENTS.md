# AGENTS.md — Tiercache

## What this project is

**Tiercache** is a family of JVM libraries providing a correct-out-of-the-box **two-level cache**: L1 = in-process (Caffeine), L2 = Redis/Valkey, with cross-instance invalidation, built-in protection against high-load failure modes (stampede, avalanche, penetration, L2 degradation), and first-class observability.

Positioning: an enterprise-grade infrastructure library in the spirit of SLF4J / Caffeine / Resilience4j — a framework-independent core with thin adapters, a predictable release cycle, and supply-chain guarantees.

The full rationale, pain catalog, and normative requirements live in `research/` (Russian source documents; the project itself is English-only):

- `research/Исследование_пробелов_JVM_библиотек_микросервисы.md` — market/gap analysis: why this niche is open.
- `research/Двухуровневый_кэш_JVM_анализ_боли_и_требования.md` — pain catalog (8 pains) and derived requirements.
- `research/ТЗ_двухуровневый_кэш_enterprise.md` — the authoritative technical specification (v1.1): requirements F-01..F-54, NFR N-01..N-10, security commitments S-01..S-06, TCK tests T-01..T-10, and the 13-month roadmap.

When requirements conflict or are ambiguous, the TZ (`ТЗ_...enterprise.md`) wins. Requirement IDs (F-xx, N-xx, S-xx, T-xx) are stable — reference them in design docs, tests, and commit messages.

## Language policy

Everything in this repository is **English only**: code, comments, commit messages, README, docs, ADRs. The `research/` folder is the sole exception (read-only Russian source material; do not translate or modify it).

## Repository state

Gradle (Kotlin DSL) multi-module build, Java 17 toolchain. Modules present: `tiercache-core` (cascade read path, singleflight, config validation, shaded Caffeine L1, L1/L2 SPI) and `tiercache-tck` (stampede harness seed). Redis transport, invalidation, Spring starter, and observability modules are not yet created — follow the roadmap below.

## Build & test commands

- `./gradlew build` — compile, unit tests, TCK tests, dependency audit, coverage gate (≥90% branch on `tiercache-core`).
- `./gradlew :tiercache-core:test` — core unit/contract tests.
- `./gradlew :tiercache-tck:test` — TCK chaos tests (Testcontainers; the stampede seed currently runs on the in-memory L2).
- `./gradlew :tiercache-core:shadowJar` — shaded artifact: Caffeine relocated under `io.tiercache.internal.caffeine`; the shaded jar is the main artifact, the plain jar keeps the `unshaded` classifier.
- `./gradlew :tiercache-core:dependencyAudit` — asserts the runtime classpath exposes only SLF4J API (N-06).
- `./gradlew :tiercache-core:jmh` — JMH baseline for the L1-hit hot path (gc profiler; N-01/N-03); results in `tiercache-core/build/results/jmh/results.txt`.
- PIT mutation testing: to be added with the invalidation/degradation phases (gate ≥75% on F-10..F-32 paths).

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

A user pulls in exactly **one starter module**. Never let framework or client dependencies leak into `core` (enforced by dependency audit in CI, NFR-5/N-06).

## Non-negotiable design principles

These are the product's identity. Violating them is a bug, not a trade-off:

1. **Correct by default.** All protections — singleflight (F-20), distributed rebuild coordination with double-check and watchdog lease (F-21), TTL jitter 5–10% (F-24), TTL ordering `TTL_L1_effective ≤ TTL_L2` (F-05, fail-fast startup validation on violation, F-04), null-caching policy (F-25), L2 timeouts below business timeout (F-33) — are ON without configuration. Disabling requires explicit opt-in and is logged as a risk. Known DIY traps (missing double-check after lock acquisition, explicit `leaseTime` killing the watchdog, L1 never warmed from L2) must be impossible **by API construction**, not by documentation.
2. **Never claim strong consistency.** The cache is eventually consistent by design (F-15). All artifacts — docs, logs, exceptions, marketing text — describe a bounded, measurable staleness window only.
3. **Observability as a feature.** Every failure mode has a metric (F-40: `tiercache.requests{result=...}`, `tiercache.latency{level=...}`, `tiercache.invalidation{direction=...}`, `tiercache.journal.size`, `tiercache.degraded`, `tiercache.breaker.state`, ...). Every chaos test must be diagnosable from metrics alone.
4. **Degradation is honest.** On L2 failure: circuit breaker → L1-only mode, zero infrastructure exceptions escaping into business code (F-30); loss of cross-instance `putIfAbsent` atomicity is surfaced via `tiercache.degraded=1` metric + log + docs (F-31). Recovery: journal replay → rate-limited warm-up → breaker close; **instant full L1 flush on reconnect is forbidden** (F-32).
5. **Zero migration threshold.** Migration from standard Spring Cache = swap the starter + one config line; existing `@Cacheable` code unchanged (F-50).

## Explicit non-goals (fixed scope boundary)

Do not implement, and reject proposals for:

- Own Redis client (transport builds on Lettuce/Redisson)
- Strong consistency
- L1 persistence to disk
- Write-behind / write-through to a database
- Service discovery

## Coding conventions

- **Java 17 baseline**; no language features beyond 17 in `core` (N-05).
- **Virtual-thread safety:** no `synchronized` on I/O paths (pinning); verified by T-09 (zero `jdk.VirtualThreadPinned` JFR events on library paths under 100k virtual threads).
- **Zero allocations** on steady-state L1-hit path (N-03, verified by JMH gc profiler).
- Core depends on **SLF4J API only** for logging.
- Secrets (e.g. Redis passwords) are never logged at any level, including diagnostic mode (S-06).
- Async surface: `CompletionStage` in core, `Mono` via reactor bridge, `suspend`/`Flow` in the Kotlin module; coroutine code must not block threads.
- Configuration model: global defaults + per-cache overrides, relaxed binding, **fail-fast startup validation** with actionable error messages.

## Testing & quality gates

Testing culture is TDD-adjacent and chaos-first. Every pain in the catalog has a paired chaos test in `tiercache-tck` (T-01 stampede, T-02 avalanche, T-03 Pub/Sub loss, T-04 invalidation/write race, T-05 Redis degradation, T-06 reconnect storm, T-07 24h soak, T-08 penetration, T-09 VT stress, T-10 Spring Cache migration). Tests must be reproducible locally via Testcontainers.

Quality gates (enforced in CI once set up):

- Branch coverage of `core` ≥ 90%; PIT mutation score ≥ 75% on invalidation/degradation paths (F-10..F-32).
- JMH benchmarks as regression gates: L1-hit overhead ≤ +50% over raw Caffeine (N-01), ≥ 1M ops/s/instance two-level reads (N-02), zero steady-state allocations (N-03); > 10% regression blocks merge.
- All F-40 metrics asserted by tests.
- Test matrix: Redis 6.2+ and Valkey, standalone/Sentinel/Cluster, JDK 17/21/25.

## Performance targets (for orientation)

- L1 hit ≈ 0.0012 ms vs ~0.45 ms Redis (~95% latency reduction on hot reads).
- Two-level throughput target ≥ 1M ops/s per instance (reference benchmark: 1.8M ops/s from `caffeinated-redis`).
- Invalidation propagation p99 ≤ 5 ms within one AZ (Pub/Sub profile, N-04).

## Roadmap (13 months to GA)

| Phase | Months | Content | Checkpoint |
|---|---|---|---|
| 0. Architecture | M0–1 | ADRs (consistency model, invalidation protocol, SPI), API spec | CP-0: public API + consistency model freeze |
| 1. MVP | M1–3 | `core` + `spring-boot-starter` + Pub/Sub transport; F-01..06, F-20/21/24/25; basic metrics | CP-1: T-01, T-02, T-10 green |
| 2. Invalidation | M3–6 | Journal + replay + Streams profile (F-10..15), LWW, UPDATE mode | CP-2: T-03, T-04 green on Redis/Valkey |
| 3. Degradation & observability | M6–8 | Circuit breaker (F-30..33), metrics/tracing (F-40..43), Grafana dashboard | CP-3: T-05, T-06 green |
| 4. Expansion & hardening | M8–11 | Kotlin module, GraalVM metadata, SWR/XFetch, soak + mutation gates | CP-4: T-05..T-09 green, N-01..03 in budget |
| 5. Enterprise GA | M11–13 | SBOM/signing/SLSA (S-01..05), offline delivery, docs, TCK publication | GA: full TCK green, release 1.0 |

Work should land in roadmap order — do not build phase 2+ features before the phase 1 core exists.

## Enterprise / supply-chain commitments (for release tooling)

- SBOM (CycloneDX) published per release; artifact signing (Sigstore/cosign + PGP); reproducible builds for `core` (S-01/S-02).
- Public SECURITY.md; fix SLAs: critical 7 days, high 30 days; target SLSA Level 3 (S-03).
- Dependencies with known CVEs block release; daily scanning (S-04).
- Support last two LTS JDK lines and last two major Spring Boot lines; 12-month security backports (S-05).
- Offline delivery: build must work in an isolated network, no external CDN calls (N-10).

## Competitive context (why decisions look this way)

- **Spring Cache** has no multi-level support — `CompositeCacheManager` never warms L1 on L2 hit; this defect must be impossible by construction here (F-01).
- **JetCache** is frozen; **Redisson** paywalls near-cache eviction and per-entry TTL (PRO); **Hazelcast** is a platform, not a library over existing Redis; **caffeinated-redis** lacks stampede protection, degradation handling, and null semantics. Our differentiators: completeness of failure-mode protection, observability, zero-config migration, and infrastructural reliability.
- Framework market is fragmented (Spring Boot ~42%, Micronaut ~39%) — hence framework-independent core + thin adapters. Post-GA candidates: Micronaut/Quarkus modules (F-53) via the same SPI, without bloating core.

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