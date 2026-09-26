# TierCache TCK (compliance suite)

`tiercache-tck` is the public chaos-test suite. It runs the cache against real
Redis/Valkey containers and checks the shipped Lettuce-based stack under specific failure scenarios.
It is not a generic certification suite for arbitrary custom transports or
serializers; those need their own SPI contract and integration tests. The suite classes live in the module's test source set; they ship as
a dedicated jar with the `tests` classifier:

```
io.github.cramen:tiercache-tck:<version>:tests
```

The plain `tiercache-tck-<version>.jar` contains only the harness helper
(`StampedeHarness`) and is included inside the `tests` jar, so the `tests`
artifact is self-contained at the class level. The JMH benchmark
(`jmhBenchmark`) and virtual-thread stress (`vtStressTest`) source sets are not
part of this artifact.

The suite's runtime dependencies (versions as used by the matching TierCache
release) a consumer build must add:

- `io.github.cramen:tiercache-core` (with its test-fixtures jar:
  `testFixtures("io.github.cramen:tiercache-core:<version>")` — the suite uses
  the `io.tiercache.testkit` in-memory SPI doubles)
- `io.github.cramen:tiercache-invalidation`
- `io.github.cramen:tiercache-transport-redis`
- `io.github.cramen:tiercache-micrometer`
- `io.micrometer:micrometer-core`
- JUnit Jupiter (`org.junit.jupiter:junit-jupiter`) and the JUnit Platform
  launcher at runtime
- Testcontainers (`org.testcontainers:testcontainers`,
  `org.testcontainers:junit-jupiter`)

Gradle consumer example:

```kotlin
dependencies {
    testImplementation("io.github.cramen:tiercache-tck:<version>:tests")
    testImplementation(testFixtures("io.github.cramen:tiercache-core:<version>"))
    testImplementation("io.github.cramen:tiercache-invalidation:<version>")
    testImplementation("io.github.cramen:tiercache-transport-redis:<version>")
    testImplementation("io.github.cramen:tiercache-micrometer:<version>")
    testImplementation("io.micrometer:micrometer-core:<version>")
    testImplementation(platform("org.junit:junit-bom:<version>"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.testcontainers:testcontainers:<version>")
    testImplementation("org.testcontainers:junit-jupiter:<version>")
}

tasks.test {
    useJUnitPlatform {
        excludeTags("soak") // long-running; see "Optional gates" below
    }
}
```

## Prerequisites

- Docker reachable by Testcontainers (local daemon, Colima, or a remote
  `DOCKER_HOST`).
- Image pulls: `redis:6.2-alpine` and `valkey/valkey:8.0-alpine`. The
  container-based tests run against both images.

## What the suite proves

| Test | Proves |
|---|---|
| `StampedeTest` | N concurrent readers of one missing key trigger exactly one loader execution per instance (singleflight); the harness detects the stampede when protection is disabled. |
| `MultiInstanceStampedeTest` | With distributed rebuild coordination, the loader runs exactly once cluster-wide on a shared Redis; with coordination disabled, at most once per instance (harness sensitivity). |
| `AvalancheTest` | Mass writes with one base TTL get effective L1 TTLs spread over the jitter band, so entries do not expire simultaneously. |
| `PenetrationTest` | Repeated requests for nonexistent keys are absorbed by null markers; the loader sees only a tiny fraction of the traffic. |
| `DegradationChaosTest` | Redis paused under read load: business operations continue at L1-only latency, no infrastructure exceptions escape, the degraded signal fires; verified retained history preserves unaffected L1 entries. The reconnect-storm fixture checks its configured workload, not a universal source-load multiplier. |
| `PubSubLossTest` | A disconnected receiver heals missed invalidations via journal replay on reconnect within the journal window; beyond the window it flushes L1 entirely. |
| `InvalidationRaceTest` | Concurrent put/evict races across instances converge every L1 to the L2 content (versioned writes, last-write-wins) — no resurrected or stale values after quiescence. |
| `TagAndUpdateTest` | Tag and batch invalidation across instances, UPDATE-mode cross-instance warm-up, and oversized-payload fallback on a real server. |
| `MetricsDiagnosabilityTest` | Selected scenario outcomes are asserted through published metrics; this does not cover every failure or replace diagnostic logs. |
| `SoakTest` (tag `soak`) | Sustained churn against a real L2: Independent post-GC heap and process RSS growth ≤ 5%, observed workers, bounded journal and JSON evidence. Not run by the default suite. |

## Choosing checks for your integration

Use the published suite to exercise the shipped stack with your selected runtime
and container environment. It does not certify arbitrary custom providers or
promise a universal throughput, staleness bound or absence of memory leaks.

The public `PropagationBenchmark` main class is included in the `tests` jar and
can run on the same assembled test classpath. It measures publish-to-applied
latency for its workload; compare results in your own deployment environment.
JMH and virtual-thread source sets are not included in the published suite.

Long soak runs and the source-only JVM/platform checks require a repository
checkout. Their commands, deadlines and evidence requirements are documented in
[repository quality gates](../maintenance/quality-gates.md). For the supported
server and Spring combinations and Sentinel limitations, see the
[platform matrix](compatibility.md).
