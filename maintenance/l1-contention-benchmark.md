# Shared L1 contention benchmark

`SharedL1HitBenchmark` measures concurrent callers of one TierCache instance. It complements the existing thread-local `L1HitBenchmark`; the default nightly `jmh` task continues to run the original small baseline. The larger shared matrix is an explicit diagnostic task.

## Run

```bash
./gradlew :tiercache-core:sharedL1Benchmark \
  -PsharedL1.java=21 -PsharedL1.threads=8 \
  -PsharedL1.features=plain,stale -PsharedL1.keys=hot,distributed
```

The task compiles with the project's Java 17 baseline and selects the requested installed runtime. Each combination uses three JVM forks, five one-second warmup iterations and five one-second measurement iterations, with a 512 MiB G1 heap. Results, including individual measurements, are written to `tiercache-core/build/results/shared-l1/results.json`. Copy that file to a uniquely named location after each invocation; the next invocation replaces it.

| Property | Values / default |
|---|---|
| `sharedL1.java` | Installed JDK version; default `17` |
| `sharedL1.threads` | Caller count; default `8`; use `1, 2, 4, 8, 16, 32` in separate runs |
| `sharedL1.features` | Comma-separated `plain`, `plainAccess`, `stale`, `staleAccess`; default `plain,stale` |
| `sharedL1.keys` | Comma-separated `hot`, `distributed`, `skewed`, `stripe`; default `hot,distributed` |
| `sharedL1.provider` | `builtin` (default), or `fallback` to wrap Caffeine using only the previous replacement SPI |
| `sharedL1.method` | `shared` (default), or `threadLocal` for independent-cache control |
| `sharedL1.api` | `get` (default), `lookup`, `compute` |
| `sharedL1.valueKind` | `value` (default), `null` for cached-null markers |
| `sharedL1.workload` | `read` (default), `write`, `clear`, `recovery` |
| `sharedL1.mode` | `thrpt` (default), or `sample` for a separate sampled-latency run |
| `sharedL1.profiler` | Omitted by default; `gc` or `jfr` for separate attribution runs |

Example allocation profile:

```bash
./gradlew :tiercache-core:sharedL1Benchmark \
  -PsharedL1.features=plain,stale -PsharedL1.keys=hot \
  -PsharedL1.threads=8 -PsharedL1.profiler=gc
```

Use `-PsharedL1.mode=sample` in a separate invocation for latency. The output unit is seconds per operation in sample mode; convert units when reporting nanoseconds or microseconds. JFR monitor-wait percentiles are not request latency. With JMH's JFR profiler, identical fork recordings may use the same output filename: preserve each fork separately when multiple raw recordings are required.

## Workload and attribution

The fixture uses the built-in Caffeine L1 and a thread-safe in-memory L2 only to populate data and attribute unexpected cascade work. It does not benchmark Redis, HTTP or a data source. All modes prepopulate 64 distinct entries in a cache of maximum size 10,000. TTLs are long enough for individual trials: one-hour write TTL, optional thirty-minute access TTL, optional five-minute degradation window, thirty-minute null-marker TTL. Jitter is disabled for this experiment. All read-only measurements concern logically fresh entries, including when retention is enabled.

| Feature | Degradation retention | Access expiry |
|---|---|---|
| `plain` | Disabled | Disabled |
| `plainAccess` | Disabled | 30 minutes |
| `stale` | 5 minutes | Disabled |
| `staleAccess` | 5 minutes | 30 minutes |

Here `stale` names the enabled retention policy, not the age of the measured values: pure-hit entries remain fresh.

`hot` always reads key zero; `distributed` samples all 64 keys; `skewed` sends approximately 90% of reads to key zero. `stripe` uses integer hashes `(i << 16) + (i << 6)`: their low six bits coincide in TierCache's stripe selection, while their ConcurrentHashMap spread hashes differ. It does not conflate stripe contention with the earlier multiples-of-64 hash-bucket collision diagnostic.

In `read`, every warmup/measurement iteration fails if any L2 or loader operation occurred after setup. Counters execute only on those operations, not on successful L1 hits. `FreshHitAttributionTest` validates the guard. A deliberately invalid benchmark can also be run through JMH with fork JVM argument `-Dtiercache.benchmark.injectL2Call=true`; its expected outcome is a failed trial containing `Invalid fresh-hit benchmark`, never a usable performance result.

Mixed controls deliberately permit cascade work. `write` replaces a value or marker every 128 operations per caller. `clear` clears local L1 every 8192 operations per caller; `recovery` requests a local recovery-generation reset at that cadence. The latter exercises the local reset/read interaction, not Redis journal replay or Sentinel recovery. Functional race tests separately verify correctness; mixed throughput or latency alone is not a coherence proof.

## Compare changes

Keep the fixture, JVM, heap, key distribution, cache occupancy and caller counts identical. Freeze baseline and candidate artifacts; alternate or randomize their run order. Retain raw per-fork results and environment/artifact hashes. Run throughput without profilers, allocation separately, and JFR separately. Longer trials and further forks are necessary when uncertainty prevents a decision.

When changing the read-only freshness path, require repeatable improvement in the affected retained hot-key profile, no extra cascade work, no increased retained-read allocation, and no repeatable regression greater than 10% in unaffected profiles. Tiny nonzero GC-normalized byte counts can reflect profiler/background overhead; inspect raw values and GC counts before claiming per-operation allocations. Keep default reads within the existing zero-allocation expectation. Results on one machine do not establish a universal operations-per-second or HTTP-capacity guarantee.
