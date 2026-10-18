# proplyd

Central cache service for the [c12s](https://github.com/c12s) platform.

proplyd sits between c12s services and their backing store (etcd). It keeps
cached entries in **Redis**, loads and writes through the store, follows the
store's change stream, and streams invalidations to the **near-caches** that
every client keeps in process ([proplyd-go](https://github.com/c12s/proplyd-go)).
When the cache is slow or down, clients keep working against the store
directly, behind a circuit breaker.

```
 edge service                                         central
┌───────────────────────────────┐   gRPC   ┌────────────────┐  RESP  ┌───────┐
│ app ─▶ proplyd-go             │ ───────▶ │    proplyd     │ ─────▶ │ Redis │
│        near-cache ([]byte)    │ ◀─────── │   (stateless)  │        └───────┘
│        breaker, fallback      │ invalid- └───────┬────────┘
└──────────────┬────────────────┘ ations           │ load, write, watch
               │                                   ▼
               └──── fallback (cache down) ──▶   etcd
```

Everything that must survive a restart lives in Redis: namespaces, entries,
watch progress and the invalidation log. proplyd processes are disposable;
several can run against one Redis.

## Layout

| Path | What |
|---|---|
| `pkg/api/proto/proplyd.proto` | gRPC API; generated code in `pkg/api` |
| `pkg/protocol` | Namespace spec (validation, defaults, proto conversion) and error reasons, shared with clients |
| `pkg/backend` | `Backend`/`Watcher` interfaces; `etcd`, in-memory `memory`, fault-injecting `faulty`, and the `backendtest` conformance suite |
| `pkg/server` | gRPC server: error mapping, health, graceful/crash shutdown. Public so it can be embedded in tests |
| `internal/engine` | The cache engine: Lua scripts, strategies, watcher, invalidation streams |
| `cmd/proplyd` | Server binary |
| `proplydtest` | Test kit: real `redis-server` you can kill/restart, TCP fault proxy, restartable nodes; `etcdtest` for embedded etcd |

## Namespaces and strategies

A client registers a **namespace** with a spec; every client of the
namespace must use the same spec (a different one is rejected with
`NAMESPACE_CONFLICT`). Keys are relative to the namespace's store prefix.

**Read strategy**

| Strategy | On a miss |
|---|---|
| `ReadThrough` (default) | proplyd loads from the store. Concurrent misses share one load. |
| `CacheAside` | proplyd reports a miss; the client loads from the store and calls `Fill`. |
| `RefreshAhead` | Read-through, and entries older than `RefreshAfter` (default 75% of TTL) are reloaded in the background while the cached value is served. |

**Write strategy**

| Strategy | Cache reachable | Cache unreachable |
|---|---|---|
| `WriteThrough` (default) | proplyd writes the store, then caches the value | **Write rejected** (`ErrCacheUnavailable`) |
| `WriteAround` | proplyd writes the store and invalidates the entry | **Client writes the store directly**; the cache picks it up through its watch |

Write-behind is not implemented yet (the enum value is reserved).

**Sync mode:** `SyncWatch` (default) follows the store's change stream for
the namespace prefix, so writes that bypass the cache (client fallbacks,
other services) update or invalidate cached entries within milliseconds.
`SyncNone` relies on writes going through the cache; anything else stays
invisible until the entry expires.

**Eviction:** each client picks the near-cache policy for its device (SIEVE,
LRU, LFU, 2Q, FIFO, TinyLFU, W-TinyLFU) in proplyd-go. Centrally, entries
expire after the namespace TTL (with 10% jitter), and Redis's
`maxmemory-policy` evicts under memory pressure.

## Consistency model

Every cached entry carries the store revision it reflects. All state
changes (loads, fills, writes, watch events) go through one Lua script that
applies a change only if it is newer than what Redis holds:

- **Values and "not found" entries** are ordered by revision; an older
  load never overwrites a newer value.
- **Every change is announced.** Writes and watch events publish an
  invalidation. A load that already sees a write which bypassed the cache
  (before the watcher does) marks its entry "quiet"; the watcher's later
  event then still publishes, so near-caches never miss a change.
- **Fences.** A change to a key that is not cached leaves a short-lived
  fence (default 30s, longer than the load timeout). A load that read the
  store *before* the change is refused. This closes the classic cache-aside
  race where a slow reader installs a value read before a concurrent write.
- **Epochs.** Every entry carries the namespace epoch. Changing the epoch
  (an O(1) purge) kills every entry at once.
- **Floor.** When watch history is lost (etcd compacted it), the namespace
  is purged and loads that read before the current revision are refused.
- **Catch-up gate.** When proplyd (re)activates a namespace, it serves it
  only once the watch progress recorded in Redis reaches the store's current
  revision. So a restarted proplyd never serves entries that missed changes
  made while it was down, such as client fallback writes. Until then
  requests fail with `CACHE_UNAVAILABLE` and clients fall back.
- **Store wiped or restored.** Cached revisions only compare with the
  store's own timeline. proplyd records the store's identity (the etcd
  cluster ID) and its watch progress per namespace. If the identity changes
  or the revision goes backwards (etcd wiped, or restored from a backup), the
  namespace is rebased: every entry is dropped, progress moves to the
  store's current revision, and near-caches reset. Nothing is served from
  the namespace until that is done. Namespaces with `SyncNone` don't follow
  the store and are not checked.
- **Redis restarts.** proplyd notices a changed Redis `run_id` (restart or
  failover) and reloads every namespace. A Redis that lost its data loses
  the namespaces, and clients re-register transparently. A Redis that came
  back from an older snapshot gets its watch replayed from the snapshot's
  progress.

### Near-cache invalidation

Every change is appended to a per-namespace Redis Stream; each proplyd tails
it and fans it out over gRPC (`Subscribe`). Invalidations carry revisions,
so a near-cache keeps copies that already reflect a change. A client that
reconnects passes its epoch and position and resumes without losing events.
If that is impossible (log trimmed, epoch changed, Redis rolled back), the
first message is a reset. Idle streams carry heartbeats, so a silently
stalled link is detected.

### Several proplyd replicas

All state is in Redis, so replicas need no coordination except for the
watcher: a lease (`<prefix>{ns}:w`, default 5s) makes one replica follow a
namespace's change stream at a time. When it dies, another replica takes
over after the lease expires and resumes from the recorded progress. Run
replicas with stable `-instance-id`s (for example the pod name of a
StatefulSet): a restarted replica with the same ID takes its lease back at
once.

## Failure behaviour

| Situation | Reads | Write-through | Write-around |
|---|---|---|---|
| proplyd down, restarting or partitioned from the client | store directly | rejected | store directly; the cache catches up via watch |
| proplyd slower than the client timeout | store directly | rejected | store directly |
| Redis down | store directly (proplyd reports `NOT_SERVING`) | rejected | store directly |
| proplyd ↔ store broken, client ↔ store fine | cached data served; misses: store directly | `ErrBackendUnavailable` | store directly |
| store down | cached data served; misses fail | fail | fail |

Only failures of the cache itself (unreachable, timeouts, Redis down) count
against the client's circuit breaker. Store errors reported by the cache,
invalid requests and the caller's own cancellations do not.

## Running

```
proplyd -listen :7070 -redis-addrs redis:6379 -etcd-endpoints etcd-0:2379,etcd-1:2379 \
        -allowed-prefixes /magnetar/,/kuiper/ -instance-id $POD_NAME
```

| Flag | Env | Default |
|---|---|---|
| `-listen` | `PROPLYD_LISTEN` | `:7070` |
| `-redis-addrs` | `PROPLYD_REDIS_ADDRS` | `localhost:6379` |
| `-redis-master` (Sentinel) | `PROPLYD_REDIS_MASTER` | |
| `-redis-username`, `-redis-password` | `PROPLYD_REDIS_USERNAME`, `PROPLYD_REDIS_PASSWORD` | |
| `-redis-key-prefix` | `PROPLYD_REDIS_KEY_PREFIX` | `pd:` |
| `-etcd-endpoints` | `PROPLYD_ETCD_ENDPOINTS` | `localhost:2379` |
| `-allowed-prefixes` | `PROPLYD_ALLOWED_PREFIXES` | any |
| `-instance-id` | `PROPLYD_INSTANCE_ID` | hostname |
| `-tls-cert`, `-tls-key` | `PROPLYD_TLS_CERT`, `PROPLYD_TLS_KEY` | plaintext |
| `-log-level` | `PROPLYD_LOG_LEVEL` | `info` |
| `-max-value-size`, `-load-timeout`, `-fence-ttl`, `-lease-ttl`, `-invalidation-log-size`, `-shutdown-timeout` | | 1 MiB, 5s, 30s, 5s, 10000, 30s |

The server exposes `grpc.health.v1` and reports `NOT_SERVING` while Redis is
unreachable, so health-checking clients skip it. SIGTERM shuts down
gracefully: subscriptions are ended (clients resume elsewhere), in-flight
calls finish, and watcher leases are handed back.

**Redis requirements.** Redis ≥ 7.0 or Valkey; the scripts use the bundled
`struct` Lua library. Use `maxmemory-policy volatile-lru` (or `volatile-lfu`
or `volatile-ttl`). Entries always have a TTL and metadata never does, so
these policies only evict entries; proplyd warns at startup otherwise.
`-redis-addrs` with several addresses selects Redis Cluster, and adding
`-redis-master` selects Sentinel. Every key of a namespace shares the hash
tag `{namespace}`, so the multi-key scripts work on Cluster.

## Tests

```
make test        # everything, with -race (needs redis-server on PATH)
make test-short  # skips embedded etcd
```

- **Engine** (`internal/engine`, real Redis): every strategy, shared loads,
  stale fills refused (fences), watch updates in place and fences,
  catch-up after a restart, compaction → purge, Redis restart without
  persistence, Redis rolled back to an old snapshot, validation, store down,
  Redis down (fail fast, no store write), watcher lease handover,
  invalidation streams (resume, reset on purge/stale epoch/trimmed log,
  heartbeats, end when Redis dies).
- **Backends**: one conformance suite (`backendtest`) run against etcd
  (embedded), the in-memory store and the fault wrapper.
- **Server**: gRPC error mapping, health following Redis, end to end on
  etcd (a direct etcd write reaches subscribers), graceful shutdown with
  open streams.

The multi-client stress, bad-network and chaos tests live in proplyd-go
(they need the client).

## Not done yet

- **Write-behind.** A durable design would keep pending writes in Redis
  (never evicted) with a flush queue.
- **Per-client invalidation filtering.** Today every subscriber gets every
  invalidation of its namespace. On very thin links (tens of KB/s) under a
  heavy write load, that stream alone can saturate the link. Tracking which
  keys each client holds (like Redis client-side caching) would fix it.
- **Batched watch apply.** The watcher runs one script per event
  (pipelined). One script per batch would raise throughput if the store's
  write rate ever outruns it; when the watcher falls behind etcd's history,
  the namespace is purged, which is correct but costly.
- **Authentication.** Use TLS plus `-allowed-prefixes` for now; add mTLS or
  per-namespace auth before exposing proplyd beyond the cluster.
