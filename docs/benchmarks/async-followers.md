# Detached async singleflight followers

## Scope and setup

Baseline: commit `03c719c`. Candidate: the uncommitted `release-async-singleflight-waiters` implementation based on that commit. Both core jars run the same `AsyncFollowerBenchmark` class on the same Apple Silicon host, OpenJDK 21, four platform-thread API workers and a 512 MiB fixed heap. Each profile runs in three separate JVM forks per version, with two seconds of warm-up and three seconds of measurement. Version order alternates between forks. No builds or test suites ran alongside this final campaign. Jar hashes, commands and per-fork observations are in [the raw results](async-followers-results.json).

This isolates local executor scheduling with an in-memory L2. It is not a Redis latency measurement, six-instance capacity test or production RPS forecast. A separate full build ran the real Redis transport and TCK suites.

Except for the all-L1 control, each closed-loop batch submits 32 readers of one new slow key first, followed by 32 independent operations. The slow source sleeps 25 ms; the next batch starts after all 64 calls settle. Independent operations are hot L1 hits, distinct L2 hits, or distinct misses with a 2 ms source. The failure profile throws after the slow delay. Each round resets data; the all-L1 control instead uses 256 cached reads per batch without resetting. Throughput includes batch setup and completion, so a slow round can cap aggregate throughput even when independent reads finish much earlier.

## Throughput and independent-request latency

The table uses arithmetic means across forks. Latency columns are the mean of each fork’s p99, not a merged percentile.

| Profile | Baseline ops/s | Candidate ops/s | Change | Independent p99 before, ms | After, ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| l1 | 1,981,595 | 1,848,621 | -6.7% | 0.113 | 0.122 |
| hot | 2,172 | 2,213 | +1.9% | 34.511 | 2.753 |
| miss | 1,247 | 2,231 | +79.0% | 57.722 | 32.321 |
| l2 | 2,212 | 2,191 | -1.0% | 33.613 | 2.697 |
| failure | 285 | 2,191 | +668.6% | 249.368 | 2.829 |

Hot/L2 profiles primarily improve independent-request latency, rather than shortening the deliberately slow round. Misses can use the freed workers while the shared owner waits. With source failures, queued baseline readers can enter later failed load rounds; candidate followers are able to join the currently pending round. This is workload-dependent coalescing, not error caching, retry suppression, or a source circuit breaker.

## All-request percentiles

| Profile | Version | p50, ms | p95, ms | p99, ms |
| --- | --- | ---: | ---: | ---: |
| l1 | baseline | 0.020 | 0.068 | 0.113 |
| l1 | candidate | 0.019 | 0.076 | 0.122 |
| hot | baseline | 28.623 | 33.035 | 34.558 |
| hot | candidate | 25.175 | 32.030 | 33.946 |
| miss | baseline | 31.495 | 50.872 | 56.112 |
| miss | candidate | 25.144 | 30.391 | 31.655 |
| l2 | baseline | 28.217 | 31.913 | 33.623 |
| l2 | candidate | 25.027 | 31.748 | 34.100 |
| failure | baseline | 217.690 | 244.604 | 249.360 |
| failure | candidate | 24.835 | 31.803 | 33.998 |

These include the slow-key waiters, which still wait for their source. Their latency must not be confused with the latency of independent reads.

## Resource costs and bounds

CPU is whole-process CPU seconds during each roughly three-second measurement window. Allocation is summed over the live measured JVM threads, divided by completed requests. Both include the harness, latency-recording futures, boxed samples, sampler and per-round setup; these are not library-only allocation figures. Peak occupancy is sampled every millisecond and is a lower bound on instantaneous peaks.

| Profile | Version | CPU seconds | Allocated B/op | Queue peak | Retained operations peak | Claim wait nodes peak |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| l1 | baseline | 7.156 | 474.4 | 256 | 256 | 0 |
| l1 | candidate | 7.214 | 532.8 | 256 | 256 | 0 |
| hot | baseline | 0.361 | 629.2 | 64 | 64 | 3 |
| hot | candidate | 0.416 | 748.5 | 64 | 64 | 31 |
| miss | baseline | 0.548 | 1356.4 | 64 | 64 | 3 |
| miss | candidate | 0.647 | 1373.8 | 64 | 64 | 31 |
| l2 | baseline | 0.488 | 1586.3 | 64 | 64 | 3 |
| l2 | candidate | 0.473 | 1675.9 | 64 | 64 | 31 |
| failure | baseline | 0.190 | 1682.1 | 64 | 64 | 3 |
| failure | candidate | 0.412 | 1613.2 | 64 | 64 | 31 |

Baseline retained-operation occupancy is estimated as active workers plus queued tasks; those two reads are not atomic. Candidate occupancy reads the actual shared credit counter. Claim wait nodes use `CompletableFuture.getNumberOfDependents()`: baseline counts include blocking join signallers, whereas candidate nodes include detached terminal callbacks. None of these sampled counters replaces the exact gated resource tests.

All benchmark runs stayed below admission capacity and reported zero rejections. The separate overload test fills a two-worker factory to 10,002 retained async operations across two views, including 10,001 cancelled attachments and one running async owner; a synchronous owner of the second claim is outside the async budget. New work in either view is rejected without a loader call, cancelling the outer futures does not restore capacity, and releasing the source returns occupancy to zero. Queue disposal, close, completed-stage preservation and notification/cancellation races are tested separately.

The all-L1 control has an explicit cost: operation ownership and atomic admission add bookkeeping and allocation to async dispatch. This does not alter the synchronous L1 allocation contract. The final three-fork mean remains within the 10% control-regression gate; per-fork throughput changes were -4.84%, -10.27% and -4.82%. One short fork crossed 10%, but neither the final mean (-6.7%) nor the exploratory three-fork mean (-6.4%) reproduced a greater-than-10% regression. The added operation record, reference-count/disposal state and shared admission operations explain the measured cost; raw forks are retained. Owners still occupy API workers, including while joining an asynchronous loader stage. If every worker owns a different blocked load, independent work still queues. Arbitrary blocking user continuations can delay terminal fan-out; they retain their admission credit until they return.

## Correctness evidence

- The gated A/B regression fails on the baseline with a timeout and passes after the change: a hot B read finishes with no loader while A and its follower remain pending, on the same two-worker pool.
- 449 core tests pass on each of Java 17, 21 and 25; branch coverage is 797/878 = 90.77% (gate: 90%).
- Full `./gradlew build --offline` passes, including 56 TCK tests, 172 Redis transport tests and 61 invalidation tests. Spring (55), Reactor (13), Kotlin (25) and Micronaut (45) adapter tests pass.
- Shared synchronous/async fixtures cover value, cached null, uncached absence, original exception causes, cancelled loaders, opt-out, skipped-refresh promotion, concurrent replacement and lease/version outcomes. The stronger exception assertions were rerun on Java 17 after the matrix.
- Foreground and refresh results/errors retire map ownership before blocked user callbacks; eviction and expiry permit a fresh round, and delayed old cleanup cannot remove its replacement.
- Source cancellation isolation, factory-close/discard-before-drain, already-complete subscription, capacity overflow arithmetic and 100 cancellation/completion races are covered.
- Dependency audit, branch-coverage gate and strict OpenSpec validation pass. No new runtime dependency or configuration key is introduced.

## Reproduction

Build the baseline core jar from `03c719c` in a separate checkout. In the candidate checkout run `./gradlew :tiercache-core:shadowJar :tiercache-core:testClasses`, then use the same SLF4J API jar for both variants:

```sh
python3 scripts/benchmark-async-followers.py \
  --baseline /path/to/baseline-core.jar \
  --candidate tiercache-core/build/libs/tiercache-core-2.0.0.jar \
  --slf4j /path/to/slf4j-api-2.0.16.jar \
  --java /path/to/jdk-21/bin/java \
  --output /path/to/results
```

Keep other tests, builds and traffic generators stopped during measurement. The runner creates a new JVM for every version/profile/fork and retains logs and JSON.
