# Redis keyspace v2 and coordinated migration

## Compatibility decision

This is an operational and wire/keyspace **breaking change**. Although Java
method signatures and value frames stay unchanged, old and new instances
use different cache data, journals, channels and locks. They cannot safely
serve shared cached data during an ordinary rolling upgrade.

The published [compatibility policy](../README.md#compatibility-and-versioning)
reserves incompatible documented behavior for a **major release**. Therefore
this change must ship in a major release with these migration instructions;
it is not a compatible 1.x patch or minor update. The current development
snapshot coordinate does not override that requirement. Exact release
numbering and publication remain a release-owner decision; this change does
not assign or publish a release version.

## Exact addresses

`token(S)` is unpadded Base64URL of the strict UTF-8 bytes of the entire
string `S`. Unpaired UTF-16 surrogates are rejected before mutation. Case,
Unicode normalization and delimiters are preserved, not interpreted. The
empty string has an empty token where the API accepts an empty name.
`byteToken(K)` is unpadded Base64URL of the original bytes, not a hash.

`D` is the physical data namespace, including a framework prefix. `L` is
the logical cache name used by invalidation. `K` is the application's
serialized key, and `T` is a tag.

| Purpose | Address |
| --- | --- |
| Data | `tiercache:v2:data:<token(D)>:` followed by raw `K` bytes |
| Tag set | `tiercache:v2:tags:<token(D)>:<token(T)>` |
| Reverse index | `tiercache:v2:tagkeys:<token(D)>:<byteToken(K)>` |
| Journal stream | `tiercache:v2:journal:<token(L)>` |
| Trim counter | `tiercache:v2:journal-trims:<token(L)>` |
| Pub/Sub channel | `tiercache:v2:inv:<token(L)>` |
| Streams consumer group | `tiercache:v2:cg:<token(L)>:<instance UUID>` |
| Built-in rebuild lock | `tiercache:v2:rebuild:<token(opaque lock name)>` |

Tag sets contain complete v2 data keys. Reverse-index sets contain encoded
tag tokens. Data and its reverse index share one absolute server expiry instant, including
on Redis 6.2 where time can advance during a Lua script. Tag-set expiry remains
extend-only, and bounded member reclamation rules are unchanged.
Resulting addresses must fit Redis's 512 MiB key limit; invalid lengths fail
before the mutating command. Value frames, application serializers, message
payloads, stream row fields and version comparison rules are unchanged.

For a Spring cache named `users`, `D = spring:users` and `L = users`.
Micronaut uses `D = micronaut:users` with the same logical-name rule.
Producers, replay and consumers all use the journal token for `L`. Different
physical data namespaces do not by themselves establish safe coordination
between independently configured applications: all participants must agree
on cache identity and data contracts.

## What clear guarantees

Clear scans only the literal `tiercache:v2:data:<token(D)>:` prefix followed
by `*`, then unlinks those data keys in batches. Names such as `user`,
`user:roles`, `a?` and `tiercache` cannot expand that scan into another data
namespace or a control-key family. Arbitrary serialized application key
bytes do not change the namespace boundary.

Whole-cache clear remains **non-transactional**. Concurrent writes can
survive the scan. Versioned clear appends its journal row after the scan;
a failure between deletion and append can leave a partial outcome. Tag
indexes survive a clear and expire or are reclaimed under the existing
TTL/janitor rules. They do not make absent data live. No global snapshot,
source transaction or Redis Cluster support is introduced.

## Cold cutover

1. Inventory every cache participant, including application replicas,
   background workers, loaders and publishers. Establish source capacity
   for cold-cache misses and an optional bounded warm-up.
2. Pause source mutations or remove all traffic from the cached path.
   Drain old requests, loaders and publishers, then stop all old-format
   instances. An old publisher must not remain active during activation.
3. Start the v2 deployment with empty v2 namespaces. If an earlier v2
   attempt left state, clear the known v2 namespaces while all writers are
   stopped. Use isolated storage when ownership cannot be established.
4. Confirm all participants use v2 and their logical cache identities
   agree. Resume traffic/mutations; authoritative loading fills the cold
   cache. Apply any warm-up gradually within the source's capacity.

Binaries may be staged ahead of time, but activation requires this drained
cutover. Do not run old and v2 instances against active shared cached data
and call it a compatible rolling update. There are no dual reads/writes,
legacy event subscriptions, automatic copying or implicit migration flags.

## Legacy cleanup and rollback

V2 ignores and preserves legacy data/control keys, channels and groups.
Old data expires under its existing TTL. Journals or indexes that remain
need an operator-approved inventory of exact keys with proven ownership,
or an explicitly dedicated retired cache database. The library provides no
legacy-prefix SCAN/UNLINK, automatic cleanup or FLUSHDB migration step.

Rollback also requires pausing traffic/mutations, draining and stopping all
v2 participants, and preparing clean, explicitly isolated cache storage
before starting old binaries. Never reactivate unverified legacy cached
values merely because they still exist. Resuming either format against
stale leftover state is not a supported rollback.
