# TierCache TCK (compliance suite)

`tiercache-tck` is the public chaos-test suite. It runs the cache against real
Redis/Valkey containers and proves the failure-mode protections the library
promises. The suite classes live in the module's test source set; they ship as
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
| `./gradlew :tiercache-tck:soakTest` | Churn soak against a real L2 container. Default duration PT10M; override with `-Dtiercache.soak.duration=PT24H` for the full profile. Not run in CI (hosted runners kill long jobs) — run it locally or on your own hardware before releases. | Memory growth ≤ 5%, journal size bounded |
| `./gradlew :tiercache-tck:vtStressTest` | 100k virtual threads over the read path; fails on any `jdk.VirtualThreadPinned` event on library frames. Requires a JDK 21+ toolchain; skipped loudly otherwise. | Zero pinning events |
| `./gradlew :tiercache-tck:jmhBenchmark` | Throughput benchmark against a Redis container: `mixedWorkload` (95% hot L1 hits / 5% cold cascade reads, reference profile), `cascadeRead` (pure cascade, worst-case reference), `l1Hit` (attribution control). Results in `tiercache-tck/build/results/jmh-benchmark/results.txt`. | No absolute budget — trend/regression measurement (throughput is environment-dependent) |
| `./gradlew :tiercache-tck:propagationBenchmark` | Invalidation propagation latency harness (two Pub/Sub instances, 10k events by default; override with `-Dtiercache.propagation.events`). Results in `tiercache-tck/build/results/propagation/results.txt`. | p99 ≤ 5 ms publish-to-applied (single AZ) |

The propagation harness (`io.tiercache.tck.PropagationBenchmark`) is a plain
`main` class inside the `tests` jar, so consumers can also run it from the
artifact on a classpath assembled as shown above.

### Real-journal recovery and virtual threads

`RecoveryJfrTest` supplements the in-memory 100k-thread read gate with a real
Redis journal, a virtual-thread HTTP probe and the registered reconnect
callback. Deterministic gates verify callbacks return before replay; JFR
checks library-attributed monitor pinning on JDK 21. Run it with
`./gradlew :tiercache-tck:vtStressTest --tests '*RecoveryJfrTest'`; use
`-PtiercacheVtJdk=25` for newer-JDK functional coverage. Recordings remain in
`tiercache-tck/build/reports/recovery-jdk*.jfr`. See [recovery](recovery.md).

### Streams pending and corruption

`RedisStreamsRecoveryTest` and `ValkeyStreamsRecoveryTest` cover pending batch
remainder, missing payloads on Redis 6.2/newer reply behavior, safe baseline
and clear gates, ACK reply loss, same-group stable resume, other-group
isolation, closed/superseded callbacks and corrupt replay anchors. Shared
decoder tests verify sanitized failures and typed payload compatibility;
metric tests verify fixed labels under non-English JVM locales.
