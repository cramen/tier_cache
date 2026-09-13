# TierCache TCK (compliance suite)

`tiercache-tck` is the public chaos-test suite. It runs the cache against real
Redis/Valkey containers and proves the failure-mode protections the library
promises. The suite classes live in the module's test source set; they ship as
a dedicated jar with the `tests` classifier:

```
io.tiercache:tiercache-tck:<version>:tests
```

The plain `tiercache-tck-<version>.jar` contains only the harness helper
(`StampedeHarness`) and is included inside the `tests` jar, so the `tests`
artifact is self-contained at the class level. The JMH benchmark
(`jmhBenchmark`) and virtual-thread stress (`vtStressTest`) source sets are not
part of this artifact.

No module in this build publishes a pom, so the `tests` jar carries no
transitive dependency metadata. A consumer build must add the suite's runtime
dependencies explicitly (versions as used by the matching TierCache release):

- `io.tiercache:tiercache-core` (with its test-fixtures jar:
  `testFixtures("io.tiercache:tiercache-core:<version>")` — the suite uses the
  `io.tiercache.testkit` in-memory SPI doubles)
- `io.tiercache:tiercache-invalidation`
- `io.tiercache:tiercache-transport-redis`
- `io.tiercache:tiercache-micrometer`
- `io.micrometer:micrometer-core`
- JUnit Jupiter (`org.junit.jupiter:junit-jupiter`) and the JUnit Platform
  launcher at runtime
- Testcontainers (`org.testcontainers:testcontainers`,
  `org.testcontainers:junit-jupiter`)

Gradle consumer example:

```kotlin
dependencies {
    testImplementation("io.tiercache:tiercache-tck:<version>:tests")
    testImplementation(testFixtures("io.tiercache:tiercache-core:<version>"))
    testImplementation("io.tiercache:tiercache-invalidation:<version>")
    testImplementation("io.tiercache:tiercache-transport-redis:<version>")
    testImplementation("io.tiercache:tiercache-micrometer:<version>")
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
| `DegradationChaosTest` | Redis paused under read load: business operations continue at L1-only latency, no infrastructure exceptions escape, the degraded signal fires; after recovery L1 survives (no reconnect flush) and several instances recover without a loader spike (reconnect storm). |
| `PubSubLossTest` | A disconnected receiver heals missed invalidations via journal replay on reconnect within the journal window; beyond the window it flushes L1 entirely. |
| `InvalidationRaceTest` | Concurrent put/evict races across instances converge every L1 to the L2 content (versioned writes, last-write-wins) — no resurrected or stale values after quiescence. |
| `TagAndUpdateTest` | Tag and batch invalidation across instances, UPDATE-mode cross-instance warm-up, and oversized-payload fallback on a real server. |
| `MetricsDiagnosabilityTest` | Every chaos scenario above is visible in the published metrics. |
| `SoakTest` (tag `soak`) | Sustained churn against a real L2: post-GC memory growth ≤ 5%, journal size bounded. Not run by the default suite. |

## Running the suite from the repository

```bash
./gradlew :tiercache-tck:test
```

This is part of the aggregate `./gradlew build` and requires Docker.

## Optional gates

All of the following are excluded from `check`; run them explicitly.

| Command | What it does | Budget |
|---|---|---|
| `./gradlew :tiercache-tck:soakTest` | Churn soak against a real L2 container. Default duration PT10M; override with `-Dtiercache.soak.duration=PT24H` for the full CI profile. | Memory growth ≤ 5%, journal size bounded |
| `./gradlew :tiercache-tck:vtStressTest` | 100k virtual threads over the read path; fails on any `jdk.VirtualThreadPinned` event on library frames. Requires a JDK 21+ toolchain; skipped loudly otherwise. | Zero pinning events |
| `./gradlew :tiercache-tck:jmhBenchmark` | Throughput benchmark against a Redis container: `mixedWorkload` (95% hot L1 hits / 5% cold cascade reads), `cascadeRead` (pure cascade, worst-case reference, no budget), `l1Hit` (attribution control). Results in `tiercache-tck/build/results/jmh-benchmark/results.txt`. | `mixedWorkload` ≥ 1M ops/s per instance |
| `./gradlew :tiercache-tck:propagationBenchmark` | Invalidation propagation latency harness (two Pub/Sub instances, 10k events by default; override with `-Dtiercache.propagation.events`). Results in `tiercache-tck/build/results/propagation/results.txt`. | p99 ≤ 5 ms publish-to-applied (single AZ) |

The propagation harness (`io.tiercache.tck.PropagationBenchmark`) is a plain
`main` class inside the `tests` jar, so consumers can also run it from the
artifact on a classpath assembled as shown above.
