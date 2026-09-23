# Troubleshooting

Start with one cache and one affected instance: record time, observed value/source
version, profile, breaker state, pending recovery and Redis connectivity. Successful
requests are an availability signal, not proof of freshness. Do not log serialized
keys, values, credentials or entire Redis URIs. Avoid FLUSHDB or broad SCAN deletion
on shared Redis; use the [namespace migration procedure](redis-keyspace-v2.md).

## Degraded or pending recovery

**Signals:** `tiercache.degraded=1`, `tiercache.breaker.state=2`, or
`tiercache.invalidation.recovery.pending{cache}=1`. HALF_OPEN is state 1 while the
degraded gauge is already zero; additional L2 calls can still be rejected during
recovery. Check Lettuce connection messages and `io.tiercache.TierCacheFactory`
breaker logs together with `io.tiercache.invalidation.InvalidationService` logs.

**Check:** availability, credentials, TLS/DNS, command latency, Sentinel's agreed
primary and reachable advertised addresses; then journal readability and retention.
Successful election is not proof that all application connections recovered.

**Act:** restore connectivity and let bounded probes/replay proceed. Repeated
triggers do not bypass retry backoff. A failed baseline retains the confirmed
cursor and pending state even after a conservative local clear. Investigate
persistent failures before changing timeouts or capacity. See [recovery](recovery.md).

## Dropped invalidations and fallback clears

**Signal:** increasing `tiercache.invalidation{direction="dropped"}`. This counts
journal-backed fallback-clear events, not individually lost messages. Inspect the
reason in invalidation logs: trimmed cursor/history, unreadable journal, malformed
rows or unconfirmable integrity. Even an apparently short outage can require a clear.

**Check:** event rate multiplied by outage/recovery lag versus journal capacity.
The protocol minimum 65 is not a production sizing recommendation. For Streams,
inspect your own consumer group's pending rows and [settlement diagnostics](streams-recovery.md#diagnostics-and-operator-checks).
Do not ACK/delete pending data manually just to silence a counter.

**Act:** repair the reader/connectivity or size retention for observed lag. A
verified baseline precedes a fallback clear; failed baseline acquisition keeps
recovery pending. A no-journal recovery handler logs its fallback but has no
journal-overflow counter, so zero `dropped` does not prove no cache was cleared.

## Publication failures

**Signals:** `tiercache.invalidation.publish{outcome="failed"}` and the bounded
publication diagnostics from `io.tiercache.invalidation.PublicationObserver`.
`sent` counts attempts. Redis acknowledgement can occur with zero subscribers;
neither counter proves that receivers applied the event.

**Check:** serialization failures, connection/command failures and receiver
reconnect/pending state. A timeout can be ambiguous: Redis may have executed the
command. `unconfirmed` indicates a legacy outcome API; `not_required` is normal
for Streams and is not a publication error.

**Act:** fix connectivity or serializer compatibility, then inspect recovery and
observed value versions. Do not blindly retry ambiguous application writes. A
publication failure cannot roll back a committed data write, and recovery cannot
reconstruct an invalidation that never entered retained history.

## Loader and refresh failures

**Signals:** application errors/latency and source load, plus
`tiercache.l2.revalidation.failures` for background refresh. That counter does not
cover every foreground loader exception. A completion counter is not proof of a
new store; a coordinated refresh can skip. Successful and failed refreshes may
coexist under the alert expression.

**Check:** source errors and timeout budgets, async executor saturation, null
policy, and whether the caller uses `getOrCompute` or Spring `@Cacheable(sync=true)`.
Ordinary `sync=false` annotations invoke the method outside the coordinated loader
path. Under `null-policy=deny`, repeated missing keys can repeatedly hit the source.

**Act:** fix source failures and apply application-side concurrency/rate limits.
Enable null/stale caching only when its semantics fit the data. Stale windows end;
they do not guarantee indefinite availability or suppress all source traffic.

## Increased source load

Compare request outcomes, traffic, L1 capacity/expiry, fallback-clear events and
L2 expiry patterns. `miss` alone does not count every loader invocation; inspect
`load`, application metrics and coalesced outcomes. L2 TTLs are not jittered, so
bulk writes can create an expiry wave. XFetch is optional and needs workload sizing.

Fleet-wide fallback clears, cold starts and unrecoverable history can all produce
bursts. There is no universal two-times source-load ceiling. Protect the source
with application capacity controls and staggered warm-up where appropriate. See
[TTL sizing](sizing-and-ttl.md) and [stale settings](configuration.md#stale-window-semantics).

## Lock cleanup delays and missing metrics

Rebuild-lock release failures are logged at DEBUG by
`io.tiercache.internal.DefaultTierCache`, without a dedicated counter or a breaker
failure. Enable that logger temporarily, correlate timings and lease expiry, and
check transport connectivity. Cleanup preserves the loaded value/original loader
failure; it does not turn a failed release into proof the lock was removed.

If a cache is absent from JMX or per-cache gauges, check whether its name is listed
in starter configuration. Activity counters and recovery registrations can exist
for dynamically created caches that are absent from the configured-cache list.
JMX's fixed name belongs to the first successful registration and does not
aggregate factories. See [observability scope](observability.md#coverage-and-diagnosis-boundaries).

## Verification and release decisions

Use the [platform matrix](compatibility.md) for tested combinations and the
[release evidence procedure](release-evidence.md) for dependency acceptance.
A functional consumer PASS is not a clean application CVE scan; an enforced
application BOM can override the library's patched defaults. A signed trial bundle
is not a final release or proof that separately published Central bytes match.
