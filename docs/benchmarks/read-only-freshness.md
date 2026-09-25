# Read-only L1 freshness: measured change

Date: 2026-09-25. Baseline: d918cb70694b605236686195ec6faf7de4c6631e. Candidate: the uncommitted `avoid-read-only-l1-freshness-monitor` change, identified by source and artifact hashes in [the machine-readable results](read-only-freshness-results.json).

The candidate preserves the second, current-holder L1 observation and avoids the engine stripe only when degradation retention is enabled and access expiry is absent. Writes, invalidation, access refresh and the default-off path retain their semantics.

## Method

Apple M1 Pro; JMH 1.36; platform threads; 512 MiB G1 heap. Every ordinary comparison uses three fresh JVM forks, five one-second warmups and five one-second measurements per fork. The frozen baseline and candidate use identical benchmark code. Paired runs were shuffled with seed 925; cross-JVM comparisons use each JVM's own baseline. GC and JFR runs are separate from unprofiled throughput. No other test/build ran alongside the main comparison.

This is a low-occupancy CPU diagnostic: 64 entries in a Caffeine L1 of maximum size 10,000; in-memory L2; no HTTP, Redis or source I/O. See [the runnable benchmark guide](../l1-contention-benchmark.md) for modes and exact TTLs. Measurements do not establish production service capacity or a universal speedup.

## Shared hot-key reads, Java 21

Millions of cache operations per second; `stale` means retention enabled while the values themselves remain fresh.

| Callers | Baseline | Candidate | Change |
|---:|---:|---:|---:|
| 1 | 7.923 | 8.653 | +9.2% |
| 2 | 4.452 | 8.435 | +89.5% |
| 4 | 4.989 | 6.920 | +38.7% |
| 8 | 5.037 | 6.133 | +21.8% |
| 16 | 4.988 | 5.958 | +19.4% |
| 32 | 4.906 | 5.867 | +19.6% |

All six affected hot-key comparisons improved. At eight callers, the main result is 5.037 to 6.133 million ops/s (+21.8%); JMH's reported errors are 0.085 and 0.060 million ops/s. The gain is not inferred from monitor-wait totals.

## Controls and supported runtimes

| Case | Baseline, million ops/s | Candidate | Change |
|---|---:|---:|---:|
| plain-hot-8 | 18.510 | 18.361 | -0.8% |
| plainAccess-hot-8 | 13.018 | 13.344 | +2.5% |
| staleAccess-hot-8 | 2.468 | 2.416 | -2.1% |
| private-stale-8 | 8.136 | 8.088 | -0.6% |
| stale-distributed-8 | 5.752 | 6.168 | +7.2% |
| stale-skewed-8 | 4.906 | 6.100 | +24.3% |
| stale-stripe-8 | 4.587 | 6.114 | +33.3% |
| stale-marker-8 | 5.051 | 6.201 | +22.8% |
| jdk17-plain | 17.930 | 17.991 | +0.3% |
| jdk17-stale | 3.725 | 6.194 | +66.3% |
| jdk25-plain | 17.718 | 17.700 | -0.1% |
| jdk25-stale | 5.194 | 6.100 | +17.4% |

No measured unaffected control regressed by more than 10%; the largest point-estimate loss among plain/access controls was about 2.2%. The private-cache control is approximately unchanged, and is not substituted for shared-cache evidence. The marker case uses `getOrCompute`; pure-hit iteration guards passed with no L2 or loader activity throughout.

Runtimes were Amazon Corretto 17.0.5, OpenJDK 21+35-2513, and Oracle GraalVM 25+37.1. JVM absolute rates are not compared as if they were the same environment.

## Latency and mixed controls

Separate JMH sample-mode profiles, eight callers. Values below are microseconds per cache operation, not network/request latency. Mixed controls intentionally permit L2 calls; recovery here is a local generation reset, not Redis journal replay.

| Case | Baseline mean | Candidate mean | Baseline p99 | Candidate p99 |
|---|---:|---:|---:|---:|
| latency | 4.061 | 2.119 | 115.200 | 17.664 |
| mixed-write | 2.683 | 2.223 | 30.496 | 18.816 |
| mixed-clear | 2.709 | 2.670 | 32.608 | 22.368 |
| mixed-recovery | 2.805 | 2.662 | 38.336 | 23.680 |

## Monitor and allocation attribution

Three separately preserved JFR recordings per revision used a 100-microsecond JavaMonitorEnter threshold. Baseline recorded 380,272 `freshnessOf` waits totaling 63.433 thread-seconds; candidate recorded zero at that engine monitor. This is summed waiting across workers, not elapsed request time; shorter waits and spinning are not represented. JFR throughput is not used for the speed comparison.

The short GC profiles must be reported honestly: default reads were 0.00668 versus 0.00673 B/op; retained reads were 0.00850 versus 0.01899 B/op. No collections occurred in those profiles. The latter normalized number increased, so it was investigated rather than silently treated as equal.

A separate allocation-attribution JFR run, with deliberately small TLABs, observed FreshnessSnapshot allocations only in an initial burst (baseline 21–59 ms, candidate 6–59 ms relative to the earliest recorded allocation event, out of about 5.1 seconds of recorded events). That intrusive run locates allocations; it is not a normal-throughput comparison and does not establish the exact cause of every GC-profiler byte.

A further ThreadMXBean control kept the same eight workers alive: ten seconds warmup, a five-second calibration window, then two ten-second windows. Candidate allocated zero bytes in those read workers across 61,216,768 and 61,220,864 operations. Baseline recorded 736 bytes across 49,900,544 operations in the first window and zero across 49,824,768 in the second. Thus the continuous warmed read path showed no sustained per-read allocation increase; the short integrated JMH metric and startup materializations remain visible in the evidence. This is not a claim of zero allocation during startup, arbitrary call contexts, custom providers, or enabled application observers.

## Correctness and reproduction

- 354 core tests passed, with zero failures, errors or skips, on each of Java 17/21/25.
- Branch coverage: 713 covered / 787 total = 90.6%, satisfying the existing 90% gate; dependency audit passed.
- Twelve new gated races cover completed put, null-marker replacement, eviction and clear between the initial L1 read and freshness observation across get/lookup/getOrCompute. Existing consistency assertions were preserved.
- Provider, missing-metadata, fence expiry/capacity, stale/null lifetime and access-refresh regressions passed in the full suite.
- The attribution negative control deliberately caused an L2 call and failed the benchmark trial as expected; it is not included in successful performance numbers.
- A separate detached checkout at the baseline commit plus the exact change files built and ran the documented Gradle task offline. Focused tests passed; its DefaultTierCache class bytes matched the measured candidate. Reproduced retained hot-key throughput was approximately 6.159 million ops/s.

Raw JMH JSON/logs, per-fork JFR files, probe sources, runtime XML/coverage reports, commands and artifact hashes are retained in the task workspace under `outputs/read-only-freshness`. The adjacent JSON preserves throughput fork data, score uncertainties and sampled-latency percentiles; full sample histograms remain in the raw files. Implementation acceptance is based on correctness, measured throughput, controls and continuous steady-state allocation evidence together, not disappearance of a monitor alone.

The remaining read-only cost includes two L1 observations and deadline classification. Access-expiry mutation remains separately synchronized and is reserved for the next change.
