# Sizing and TTL guide

Practical guidance for the four knobs that decide how a cache behaves in
production: `l1-max-size`, `l1-expire-after-write`, `l2-ttl`, and
`jitter-amplitude`. Every property name and default below is defined in
[configuration](configuration.md); every metric name in
[observability](observability.md).

## Sizing L1

L1 is a bounded Caffeine cache capped by **entry count only**
(`l1-max-size`, default `10000`). There is no weight-based bound, so heap
budgeting is your job: budget entries × average entry footprint.

```
heap per cache ≈ l1-max-size × (key footprint + value footprint + small holder overhead)
```

Assume the worst case: all configured caches full at the same time. Keep
the sum of all L1 budgets a small fraction of the heap — if L1 needs more
than ~10–20% of `-Xmx` to hold its working set, the working set belongs in
L2, not in-process.

Estimating entry footprint:

- **Lower bound:** the serialized size of key + value. Serialize a
  representative sample with the same serializer the cache uses (for
  example `JdkCacheSerializer`) and average the byte lengths. Heap
  footprint is typically 1.5–3× the serialized size for JDK-serializable
  object graphs.
- **Ground truth:** warm the cache in a staging run, take a heap dump, and
  measure the retained size of the value classes. Adjust `l1-max-size`
  from the measured number, not from a guess.

Signs `l1-max-size` is too small: the `l1_hit` share of
`tiercache.requests` stays low while `l2_hit` is high — entries are evicted
from L1 before they stop being hot and every read pays an L2 round trip
(watch `tiercache.latency{level="l2"}`). Raising the cap helps only while
the working set actually fits; beyond that, evictions are healthy and the
fix is a shorter key space or accepting the L2 traffic.

## L1 TTL vs L2 TTL

**Rule: `l1-expire-after-write <= l2-ttl`, always.** This is not a
recommendation — it is enforced. Startup validation
(`CacheConfigValidator`) rejects any cache where an L1 TTL
(expire-after-write, expire-after-access, or the null-marker TTL) exceeds
`l2-ttl`, throwing `CacheConfigurationException` that names the cache, both
values, and the fix. Startup aborts; no traffic is served with the invalid
configuration.

This rule compares configured durations, not the remaining lifetime of a
specific Redis entry. L2 expiry is silent, and warming L1 from L2 starts a
new local TTL. The rule therefore does not guarantee that a local copy
expires at the same instant as its Redis counterpart.

For write-based expiry, with no access sliding, SWR or degradation stale
window, and no later stale writes, budget a stale Redis copy's remaining
TTL plus one final L1 warm. This matters for out-of-band source changes and
evictions that never reached Redis during an outage. Expire-after-access
can slide local deadlines; opt-in SWR and degradation windows permit
additional stale serving. Assess those policies separately rather than
applying the simple write-expiry bound.

Choosing the ratio within that constraint:

- **`l2-ttl` controls a Redis entry's logical freshness lifetime.** Size it together
  with the final L1 warm and any access/stale policies against your
  application's freshness budget. Timely invalidation usually shortens
  staleness, but propagation is asynchronous.
- **`l1-expire-after-write` trades L2 traffic against local retention.**
  It bounds one L1 copy's lifetime without access sliding or stale serving;
  it does not bound repeated warming from a still-stale L2 entry. The shipped default ratio is 1:12 (`5m` vs `1h`); ratios
  between 1:4 and 1:20 are the sensible band. Below 1:4 L1 stops earning
  its keep; above 1:20 the staleness window on a lost event usually stops
  being acceptable.
- Remember jitter: the effective L1 TTL of each write lies in
  `[l1-expire-after-write × (1 - jitter-amplitude), l1-expire-after-write]`.
  Budget staleness against the configured value (the worst case), budget
  reload traffic against the shortened average.

## Jitter amplitude

`jitter-amplitude` (default `0.10`, range `[0, 1)`) draws each L1 entry TTL
uniformly from `[base × (1 - amplitude), base]`. It only ever shortens, so
it can never break the TTL ordering invariant — that is why startup
validation can compare the configured values directly.

Purpose: avalanche protection. Entries written together — deploy-time
warm-up, a mass invalidation, journal replay after a reconnect — would
otherwise expire together and hit the loader in one synchronized wave.
Jitter spreads the expirations over a window of `amplitude × TTL`, turning
the cliff into a ramp.

Guidance:

- Leave `0.10` unless you have a reason. It costs at most 10% of L1
  lifetime and flattens the common synchronized-write cases.
- Raise it (up to roughly `0.2`–`0.3`) for caches that are bulk-written in
  one shot — nightly reference-data reloads, warm-up scripts — where a
  synchronized expiry would be a real loader storm.
- Do not raise it on short-TTL caches to fix a reload wave that comes from
  L2: jitter covers L1 and null-markers only. L2 entries are shared by all
  instances and are stored with the unjittered `l2-ttl` by design, so a
  fleet-wide L2 expiry cliff is mitigated by XFetch (`xfetch-enabled`),
  not by jitter. See [TTL jitter and XFetch](configuration.md#ttl-jitter).

## Journal capacity

The invalidation journal (`tiercache.invalidation.journal-capacity`,
default 10000 entries per cache stream) bounds how long an instance can be
disconnected and still heal by replay instead of a full L1 flush. Size it
as `expected disconnect window x invalidation rate`: how many invalidation
events one cache can see during the longest outage you want to survive
without a flush. A Pub/Sub blip of a few seconds against hundreds of writes
per second fits the default easily; a multi-minute network partition at
high write churn may not.

When the window is exceeded (or no journal is wired), the reconnecting
instance flushes its whole L1 for the affected caches — a cold start that
hammers the loader path exactly when the partition just healed. The flush
is signalled via log, the `tiercache.invalidation{direction="dropped"}`
metric, and the `TiercacheDroppedInvalidations` alert rule in
[docs/grafana/](grafana/). If you see drops, raise the capacity before
reaching for longer L1 TTLs.

The enforced protocol floor is **65 entries**: the fixed live cursor cadence is
64 delivered events, and the inclusive integrity check also needs the previously
confirmed cursor row. Capacity 64 loses that row by the next full tick under exact
retention, so values <=64 are rejected rather than silently increased. Direct
Redis journal construction and both starters share the same validator. The default
remains 10000; a disabled journal does not validate its unused capacity setting.

Accepting 65 is not a no-flush guarantee or an outage budget. Bursts, time between
scheduling and executing recovery, failed reads and disconnected receivers need
headroom beyond the floor. Size for the invalidation rate times maximum expected
recovery/read lag, with operational margin; a few hundred or more can be needed
even for short delays. Redis MAXLEN trimming is approximate: the stream can retain
more rows than the configured target, but that temporary over-retention is not a
correctness mechanism. This property is neither a byte cap nor an exact row maximum.
Conservative reset still applies whenever history cannot be verified.

## Per-cache overrides vs global defaults

Keep `tiercache.defaults.*` aimed at your most common cache shape and use
`tiercache.caches.<name>.*` only where a cache genuinely differs in
volatility, value size, loader cost, or miss behavior. A name absent from
`tiercache.caches` is created on demand with the defaults — fine for
unremarkable caches, wrong for the ones that matter.

Override when:

- the freshness bound differs (a cache fed by an out-of-band nightly job
  needs a different `l2-ttl` than one written only through the app);
- values are much larger or smaller than the norm (adjust `l1-max-size`
  per the heap budget above);
- misses are expensive and hostile (`null-policy: allow` belongs on the
  caches exposed to arbitrary external keys, not globally);
- the cache is hot enough to justify stale serving or XFetch, which are
  off by default.

Three recipes:

**Reference data / dictionary** — changes rarely, small values, read
constantly, brief staleness acceptable. Long TTLs, generous L1, UPDATE
mode so peers warm directly, null markers to absorb lookups of unknown
codes:

```yaml
tiercache:
  caches:
    countries:
      l1-max-size: 50000
      l1-expire-after-write: 30m
      l2-ttl: 6h
      invalidation-mode: update
      null-policy: allow
      null-marker-ttl: 5m
```

**Session-like data** — user-scoped, moderate churn, staleness is a
correctness problem. Short L1 TTL so a missed invalidation heals in
seconds, `l2-ttl` matched to the session lifetime, no null caching (a
missing session is a real answer, not an attack):

```yaml
tiercache:
  caches:
    sessions:
      l1-max-size: 20000
      l1-expire-after-write: 30s
      l2-ttl: 30m
      jitter-amplitude: 0.05
```

**Hot catalog** — huge read volume, expensive loader, staleness tolerant.
Stale-while-revalidate keeps serving during refresh, XFetch refreshes hot
entries before the L2 expiry cliff, higher jitter spreads the warm-up
writes:

```yaml
tiercache:
  caches:
    catalog:
      l1-max-size: 10000
      l1-expire-after-write: 5m
      l2-ttl: 30m
      jitter-amplitude: 0.20
      stale-ttl: 2m
      xfetch-enabled: true
      xfetch-beta: 500ms
```

Every recipe still satisfies the TTL ordering invariant; if you break it,
startup tells you.

## Metrics to watch while iterating

Change one knob at a time and read the effect from metrics — the full
catalog with tag names is in [observability](observability.md).

- `tiercache.requests{result=...}` — the primary feedback. `l1_hit` share
  answers "is L1 big/long-lived enough"; `l2_hit` answers "is L2 doing its
  job"; `miss` + `load` is traffic reaching the backing system;
  `coalesced` shows singleflight absorbing stampedes.
- `tiercache.latency{level="l2"}` — p99 of L2-touching operations. If you
  shortened L1 TTLs, this shows the added L2 load; if it climbs, you
  traded too much.
- `tiercache.null.entries` — under `null-policy: allow`, confirms markers
  are absorbing penetration instead of the loader.
- `tiercache.invalidation{direction="dropped"}` — must stay zero. Any drop
  is a staleness incident; if you raised L1 TTLs "for safety" after seeing
  drops, fix the journal capacity instead
  (`tiercache.invalidation.journal-capacity`).
- `tiercache.l2.stale.hits` and `tiercache.l2.revalidation.{triggers,completions,failures}`
  — when `stale-ttl` / XFetch are on, completions should track triggers
  and failures should stay near zero; otherwise stale entries are serving
  but never refreshing.
- `tiercache.degraded` — `1` means the breaker is open and the cache runs
  L1-only; sizing conclusions drawn during degradation are invalid.
- JMX (`io.tiercache:type=Inspection`): `getL1HitRatio(cache)` /
  `getL2HitRatio(cache)` for a quick per-cache hit-ratio check without a
  metrics backend.

A typical iteration: raise `l1-max-size` or L1 TTL → confirm `l1_hit`
share climbs and `tiercache.latency{level="l2"}` rate drops → confirm
`miss`+`load` rate and heap stay flat → stop. The shipped Prometheus alert
`TiercacheMissGrowth` catches the inverse regression (miss rate doubling
on flat traffic) after a bad change.
