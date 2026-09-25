# Atomic access refresh: implementation and measured trade-off

Date: 2026-09-25. Baseline is c0ebf3ab58b502211862a8883fd041d9644ecb27 (read-only freshness optimization already applied). Candidate is the uncommitted `optimize-atomic-l1-access-refresh` implementation. [Raw fork values, uncertainty, hashes and validation summary](atomic-access-refresh-results.json) accompany this report.

## Behavior and compatibility

The optional LocalCache capability combines observation and access refresh in one provider-owned operation. Built-in Caffeine uses a non-inserting computeIfPresent transaction. An immutable private holder also carries its observation result, and the configured duration pair reuses a stateless remapper. A separate result box is unnecessary. Stale/unknown observations preserve the exact opaque StoredEntry and all deadlines; only the private observation carrier may change at the first state transition.

Retained holders carry absolute physical deadlines. Expiry callbacks return their remaining lifetime, so unchanged stale/unknown entries cannot gain another TTL. The operation samples the monotonic clock inside the transaction and rechecks physical expiry after any earlier Caffeine clock sample. It preserves null-marker/value identity semantics, version, write timestamp, barrier and store floor. A short access TTL may shorten logical freshness. Stale reads never slide and the engine still decides whether breaker rejection authorizes serving them.

Existing identity-replacement providers keep the stripe-protected fallback. A provider dynamically compiled against the frozen previous LocalCache source was loaded with the new interface and exercised successfully. Unsupported old combinations still fail validation. No new runtime dependency, Redis frame/key migration or application setting is introduced.

## Method

Apple M1 Pro, OpenJDK 21+35-2513 for performance, JMH 1.36, platform threads, 512 MiB G1 heap. Each throughput/GC/sample comparison uses three fresh JVM forks, five one-second warmups and five one-second measurements. Core correctness/coverage separately ran on Java 17/21/25. JFR was recorded separately with a 100-microsecond monitor threshold; allocation-stack sampling used the default TLAB configuration and is qualitative attribution, not exact object counts per request.

The [shared fixture](../l1-contention-benchmark.md) prepopulates 64 entries in a maximum-size-10,000 cache, with one-hour write TTL, thirty-minute access TTL and a five-minute retention window. L2 is an instrumented in-memory map. Pure-hit iterations reject any L2/loader activity. The collision profile uses distinct underlying map buckets sharing an engine stripe. Measurements are local JVM operations, not HTTP/Redis capacity. Baseline and candidate artifacts are frozen; the broader campaign alternated comparison order. The final compact-carrier variant reuses the same captured baseline, rather than claiming a new randomized experiment.

## Throughput

Millions of operations per second. Access expiry plus retention is enabled except in the two explicit controls.

| Profile | Baseline | Candidate | Change |
|---|---:|---:|---:|
| hot, 1 caller | 3.913 | 3.744 | -4.3% |
| hot, 8 callers | 2.437 | 2.574 | +5.6% |
| hot, 32 callers | 2.325 | 2.515 | +8.2% |
| distributed, 8 callers | 3.206 | 3.280 | +2.3% |
| stripe collision, 8 callers | 2.277 | 3.238 | +42.2% |
| plain, 8 callers | 18.325 | 17.800 | -2.9% |
| retention without access expiry | 6.104 | 6.102 | -0.0% |

The clear benefit is reduced cross-key stripe contention; the hot-key multithreaded gain is smaller. The single-thread affected profile is about 4.3% slower. Small distributed-key differences should not be overstated beyond their uncertainty. No measured unaffected control regressed by more than 10%. On the candidate's same Caffeine backend, forced previous-SPI fallback measured approximately 2.420 million hot-key ops/s at eight callers, compared with 2.574 million for the native capability.

## Allocation cost and adoption decision

This optimization is not memory-neutral. Access-refresh allocation rose from approximately **296 to 400 bytes per operation (+35.1%)**. The previous-SPI fallback on the candidate measured about 304 B/op. An initial per-read mutable operation/result prototype allocated 440–448 B/op and was replaced by the final immutable carrier/stateless-remapper design.

JFR locates the extra work in Caffeine's general `remap` path, including object/int arrays and larger remapping lambdas, versus the previous specialized `replace` path. Sampled remapping lambdas were 72 bytes versus 56 bytes; the new private observation carrier is 32 bytes. The immutable StoredEntry and LocalFreshness allocation sites remain present in both implementations. Sampling does not assign an exact fraction of every byte to each site, but the additional remapping bookkeeping and carrier are identified; this is allocation pressure, not evidence of a leak or retained-heap growth.

The GC profile reported 35 collections/35 ms total collection time for the baseline and 49 collections/29 ms for the candidate across its measurement iterations. These short-run timings are not a claim that more allocation improves GC. Default/plain and retained read-only controls stayed at profiler-scale fractions of a byte per operation (about 0.006 and 0.016 B/op), without a new access-refresh allocation on those paths.

The plan requires repeatable benefit in an affected profile, no unexplained allocation increase and no >10% regression in unaffected controls; it does not require every affected profile to be faster or allocation to be identical. Acceptance is based on the large collision-profile benefit, preserved behavior, unaffected controls and attributed allocation cost. The single-thread slowdown and increased garbage are explicit costs of adopting the provider transaction. Allocation-sensitive workloads should evaluate this table rather than treating the change as a universal acceleration.

## Sampled latency and synchronization

Separate sample-mode measurements at eight callers, microseconds per operation:

| Workload | Baseline mean | Candidate mean | Baseline p99 | Candidate p99 |
|---|---:|---:|---:|---:|
| latency | 7.770 | 7.616 | 197.632 | 177.152 |
| mixed-write | 3.895 | 3.686 | 37.952 | 34.176 |
| mixed-clear | 3.915 | 3.837 | 43.200 | 39.552 |
| mixed-recovery | 3.932 | 3.800 | 42.496 | 41.024 |

Mixed controls exercise local writes/clear/recovery-generation resets, not network journal recovery. Functional tests separately verify coherence. In the separate JFR traces, the baseline recorded 142,556 engine-freshness monitor events (25.512 summed thread-seconds); the candidate recorded 110,638 Caffeine remap monitor events (22.681 summed thread-seconds). Serialization moved into Caffeine's per-key operation; it did not disappear. These waits are not request latency, and profiled throughput is not used as an unprofiled comparison.

## Validation and reproduction

- 406 core tests passed without failures/errors/skips on each of Java 17, 21 and 25. Branch coverage was 761/834 = 91.25%; dependency audit passed.
- Adapter tests passed: Spring 55, Reactor 13, Kotlin 25 and Micronaut 45.
- Two real-Redis lifetime/outage tests passed, including the new access-expiry case with a paused Redis container, exact local clock, no stale sliding, logical cutoff before the physical floor and no metadata in Redis frames.
- Deterministic races cover all four read paths (get, lookup, getOrCompute and readThrough) with native and legacy providers, completed replacements/null markers/evictions/clear, delayed readers, concurrent provider mutations, physical expiry while awaiting a transaction, and wrapping clocks.
- The HALF_OPEN exact-clock test proves an admitted failed probe cannot be replaced with a stale response; subsequent rejected calls retain their existing eligibility.
- Sixteen concurrent readers of each stable value/marker completed 240,000 API calls per case with no L2/loader calls and no false outcome.

Run `./gradlew :tiercache-core:sharedL1Benchmark -PsharedL1.java=21 -PsharedL1.features=staleAccess -PsharedL1.keys=hot,distributed,stripe -PsharedL1.threads=8`. Add `-PsharedL1.provider=fallback` to hide the optional capability over the same provider. Use separate invocations with `-PsharedL1.profiler=gc` or `-PsharedL1.mode=sample`; preserve each output file before the next invocation. Commands are described in the benchmark guide.

The documented Gradle fallback command was executed successfully on the final sources (approximately 2.421 million ops/s). The final compiled runtime class bytes match the measured candidate3 artifact, and the frozen compatibility interface matches the baseline Git source exactly.

Full raw results, command logs, JFR files and runtime XML/coverage evidence are retained under `outputs/atomic-access-refresh` in the task workspace. Superseded prototypes are labelled candidate/candidate2; the final implementation is candidate3. No universal capacity multiplier, lock-free guarantee or lower-allocation claim is made.
