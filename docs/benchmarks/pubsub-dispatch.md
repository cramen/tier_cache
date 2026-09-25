# Bounded Pub/Sub dispatch and registration readiness

## What changed

The receiver uses one ordered lane per cache over two fixed workers by default. A transport retains at most 1024 messages and 16 MiB of encoded frames, including executing messages. Overflow pauses that lane, retires queued data and requests checked journal catch-up or the existing conservative reset. Another loss before completion requires another repair. A separate control budget includes retired registrations with unfinished repair/fence callbacks.

Recovery clears, including EVICT_ALL rows applied during replay, wait for old admitted delivery to finish. Registration has an explicit completion stage; the factory publishes a new cache only after successful registration, outside its lifecycle/cache-map monitors. Synchronous registration on delivery/recovery workers fails with an actionable async alternative. Streams ACK/coverage proofs remain separate from local catch-up proofs.

## Measurement method

Baseline: `a2b61ab`. Candidate: this change. Runs used an Apple M1 Pro (10 cores, 32 GiB RAM), Docker with five CPUs and about 7.67 GiB RAM, real Redis 6.2 Alpine, and OpenJDK 21 with a fixed 512 MiB heap. Each version/profile ran in three separate JVM forks; order alternated between forks. Other stand nodes and build/test jobs were stopped during these comparisons. [Raw runs, commands, test counts and hashes](pubsub-dispatch-results.json) are retained.

The same standalone benchmark class and legacy transport constructor were used for both versions. Healthy runs warmed up with 500 messages, then attempted 5000 messages at a target rate of 1000/s. The serial append-plus-publish driver achieved less than that target: about 650/s. Its Redis round trips constrain these throughput numbers, so this is not a maximum-capacity result or proof against regressions at higher offered rates. The predeclared gate was no greater than 10% regression in mean healthy completed throughput, with mandatory resource bounds, coherence and independent-cache progress.

The burst profile paused the first handler for cache A, submitted 2001 messages for A and one independent message for B, then released A and awaited complete delivery/repair. The pause was controlled by latches, not inferred from sleeps. Publish-to-applied latency starts just before publication, after journal append. Every recorded publication must produce an observation; duplicate replay observations are counted once.

## Results

Table values are arithmetic means across forks; percentile columns are means of per-fork percentiles, not merged distributions.

| Profile | Version | Completed operations/s | p50, ms | p95, ms | p99, ms | Peak retained messages |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| healthy | baseline | 671.4 | 0.636 | 1.583 | 2.493 | 2 |
| healthy | candidate | 694.3 | 0.645 | 1.428 | 2.341 | 1 |
| burst | baseline | 663.6 | 1486.950 | 2845.244 | 2969.199 | 2001 |
| burst | candidate | 683.0 | 1391.628 | 2741.024 | 2860.709 | 1024 |

Healthy throughput changed by **+3.4%**, within the declared gate. The p99 difference is small compared with fork/host variability; no general latency improvement is claimed.

| Paused-A result | Baseline | Candidate |
| --- | ---: | ---: |
| Independent B latency, ms | 2997.306 | 1.249 |
| B completed before A was released | No, all three forks | Yes, all three forks |
| Queue drain / checked repair after release, ms | 14.705 | 45.418 |

The old queue retained all 2001 A messages and blocked B behind A. The candidate capped retained messages at 1024 and used checked replay after overflow. Its extra repair work is a real trade-off; the baseline drain is not a journal repair. Latency for the deliberately paused A requests remains dominated by that pause. The improvement is independent-cache progress and bounded retention, not faster execution of the blocked handler.

## CPU, heap and accounting

| Profile | Version | Process CPU seconds | Peak heap MiB (mean) | Peak encoded bytes | Peak control groups |
| --- | --- | ---: | ---: | ---: | ---: |
| healthy | baseline | 3.469 | 225.4 | not instrumented | not applicable |
| healthy | candidate | 3.676 | 143.2 | 106 | 0 |
| burst | baseline | 1.539 | 82.9 | not instrumented | not applicable |
| burst | candidate | 1.670 | 87.9 | 107437 | 1 |

CPU and heap include the publisher, receiver, benchmark observer, latency samples and journal client. Heap peaks depend on GC timing and are not a retained-message or leak measurement. Baseline message occupancy is its executor queue plus active worker; candidate occupancy reads the actual reservation counters. Sampling can miss instantaneous peaks; deterministic tests cover exact count/byte limits, oversized frames and executing-message ownership. Network/client buffers and arbitrary deserialization expansion are outside the byte budget.

## Six-instance Docker acceptance

The stand contains six separate Java 17 processes, Redis, nginx and an HTTP origin with a 100 ms response delay. Each node uses two dispatch workers, a deliberately small 16-message / 65536-byte budget, a 10000-row journal and UPDATE mode on both the engine and L2 journal writer. The final run is in [the stand trace](pubsub-dispatch-stand.json).

| Scenario | Result | Duration, s |
| --- | --- | ---: |
| warmup | Passed | 0.302 |
| paused-receiver-burst | Passed | 2.289 |
| intact-history-repair | Passed | 0.154 |
| update-evict-all-repair | Passed | 0.621 |
| redis-restart-and-new-mutation | Passed | 8.767 |

The independent cache updated in 6.9 ms while node 6's A handler remained paused. Across 155 sampled snapshots, observed retained peaks were 9 messages and 558 encoded bytes per node, below both configured limits. After the final Redis restart and a subsequent mutation, all six L1s contained the new value; queues were empty and neither cache remained pending.

The EVICT_ALL scenario allows a safe L1 miss before re-warming from current L2: replaying a writer’s own clear may evict a value subsequently written locally. The test rejects an old value, verifies the next read obtains the current value and then checks convergence. The first exploratory stand run used INVALIDATE journal rows while expecting UPDATE replay; its L1 misses were correct. The final recipe explicitly aligns both UPDATE settings. Writes performed only during a total Redis outage are not claimed to be reconstructed by replay.

## Correctness and compatibility

- Full offline build passes. Core branch coverage is 807/896 = 90.07% (gate: 90%).
- Java 17: 452 core, 69 invalidation, 195 transport, 26 Micrometer, 57 Spring, 47 Micronaut, 25 Kotlin, 13 Reactor and 56 TCK tests pass. The full transport suite includes existing Redis/Valkey Streams regressions.
- Java 21 and 25: 452 core, 69 invalidation and 23 focused dispatcher/real-PubSub tests pass on each runtime. The final observer assertions were also rerun on Java 17.
- Deterministic tests cover FIFO/quantum fairness, byte/count caps, throwing decoders/handlers/observers, malformed channel attribution, repeated loss, no-journal repair, unavailable recovery, close/replacement, weak retention of old handlers and hung custom stages.
- Registration tests cover readiness, failed baseline/subscribe, reentrancy, replacement/close and a 5000-row intact catch-up without a forced clear. A replayed EVICT_ALL is gated behind a paused old deserializer.
- A negative-control build disables capacity admission: the bounded-queue regression fails. The same probe passes with the actual dispatcher. The real baseline also fails the independent-cache-progress condition in every paused-handler fork.
- Numeric overflow cannot silently select a Micronaut default; both framework bindings reject invalid values with the full property name. Existing constructors and SPI implementations retain compatibility defaults.
- The shutdown regression now re-samples live worker threads instead of repeatedly checking a captured set; a stopped worker can no longer cause a false timeout.
- Strict OpenSpec validation and dependency audit pass. No runtime dependency or Redis wire-format change is introduced.

## Reproduction

Build a separate baseline checkout at `a2b61ab`. Export the runtime classpath in each checkout with the candidate’s helper script (use its absolute path when invoking it from the baseline):

```sh
./gradlew -I scripts/pubsub-runtime.gradle \
  -PpubsubClasspathFile=/absolute/path/candidate-classpath.txt \
  :tiercache-transport-redis:exportPubSubRuntime --offline
```

Prepare and run the Docker stand from the candidate checkout. The default local ports are 18090–18096 and 16389:

```sh
python3 scripts/prepare-pubsub-stand.py \
  --classpath-file /absolute/path/candidate-classpath.txt
docker compose -f build/pubsub-stand/compose.yaml up -d --build
python3 build/pubsub-stand/run.py
```

For an uncontended comparison, stop app1–app6, balancer and origin in that same Compose project, leaving Redis up. Then run:

```sh
python3 scripts/benchmark-pubsub-dispatch.py \
  --baseline-classpath /absolute/path/baseline-classpath.txt \
  --candidate-classpath /absolute/path/candidate-classpath.txt \
  --java /path/to/jdk-21/bin/java \
  --output build/pubsub-comparison
docker compose -f build/pubsub-stand/compose.yaml down
```

Keep other builds and traffic generators stopped while measuring. Stand timing is a functional acceptance observation, not a production latency SLA. Blocking all dispatch workers still stalls delivery, sustained overload may stay pending, and a hung custom recovery stage can exhaust the bounded control budget without authorizing an unsafe resume.
