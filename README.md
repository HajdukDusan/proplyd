# proplyd

Centralized cache service for the [c12s](https://github.com/c12s) platform.

Services register a **namespace** and pick how it behaves: read strategy, write
strategy, eviction policy, capacity and TTL. The cache sits in front of etcd,
talks to it directly, and follows etcd's change stream to stay consistent.
Clients talk to the cache over gRPC. When the cache is slow, unreachable or
dead, they keep working against etcd directly, behind a circuit breaker.

```
 service A ─┐                                   ┌──────────────┐
 service B ─┼── gRPC ──▶  proplyd (cache) ──────▶│              │
 service C ─┘   │          ▲  per-namespace      │     etcd     │
                │          └── watch (changes) ──│              │
                └──────── fallback (cache down) ─▶│              │
                                                 └──────────────┘
```

## Layout

| Path | What |
|---|---|
| `api/proplyd/v1/cache.proto` | gRPC API (generated code in `api/proplydpb`) |
| `cache/` | The cache engine: namespaces, strategies, consistency |
| `store/` | Storage abstraction (`store.Store`) + samber/hot implementation |
| `backend/` | Backing-store abstraction (`backend.Backend`) + `etcd` and in-memory `memory` implementations |
| `server/` | gRPC server over the engine (error mapping, health, graceful shutdown) |
| `client/` | Go client: circuit breaker, timeouts, fallbacks, re-registration |
| `protocol/` | Types shared by client and server (namespace config, error reasons) |
| `cmd/proplyd` | Server binary |
| `tests/` | Stress, chaos and end-to-end etcd tests |
| `internal/testutil` | Fault-injecting proxy, restartable cache node, embedded etcd |

The client imports only `protocol`, `backend` and the generated API, never
the engine, so it stays small on constrained devices.

## Strategies

Strategies are chosen per namespace at registration. Every client of a
namespace must use the same configuration; a conflicting registration is
rejected.

**Read**

| Strategy | Miss handling |
|---|---|
| `ReadThrough` (default) | The cache loads from etcd. Concurrent misses for a key share one load. |
| `CacheAside` | The cache reports a miss. The client loads from etcd and calls `Fill`. |
| `RefreshAhead` | Read-through. Entries older than `RefreshAfter` (default 75% of TTL) are reloaded in the background while the cached value is still served. |

**Write**

| Strategy | Normal operation | Cache unreachable | Guarantee |
|---|---|---|---|
| `WriteThrough` (default) | The cache writes etcd, then updates itself | **Write rejected** (`ErrCacheUnavailable`) | The cache never diverges from etcd, but writes can be refused |
| `WriteAround` | The cache writes etcd and invalidates its entry | **Client writes etcd directly** | Every write is recorded; the cache catches up via its etcd watch |
| `WriteBehind` | Acknowledged in memory, flushed in batched etcd transactions | Write rejected | Lowest latency. **Unflushed writes are lost if the cache process crashes** |

With write-behind, writes to the same key are coalesced before a flush. A
graceful shutdown (SIGTERM) flushes everything pending. `MaxPending` bounds
memory: when the queue is full, writes fail with `ErrBackpressure`.

**Eviction:** `sieve` (default), `lru`, `lfu`, `2q`, `fifo`, `tinylfu`, `wtinylfu`. See the known issues below.

**Sync mode:** `WATCH` makes the cache follow etcd's change stream for the
namespace prefix. `AUTO` (the default) turns it on for write-around only.
Turn it on explicitly if other writers modify the keys.

## Consistency model

Every cached entry carries the etcd revision it reflects. All state changes
for a key (loads, fills, writes, watch events, flushes) are serialized by a
lock stripe and applied only if they are newer than:

1. the cached entry, and
2. the latest change to that key the namespace knows of (a bounded *change
   log*).

This closes the classic cache-aside race, where a slow reader installs a
value read before a concurrent write. A watch replay or a late load can never
overwrite newer data either. If the watch cannot resume because etcd
compacted the history, the namespace is purged and resynchronized.

### Failure behaviour

| Situation | Reads | Write-through | Write-around | Write-behind |
|---|---|---|---|---|
| Cache crashed / restarting | etcd directly | rejected | etcd directly | rejected; unflushed data lost |
| Client ↔ cache partition | etcd directly | rejected | etcd directly; the cache sees it via watch | rejected |
| Cache slower than timeout | etcd directly | rejected | etcd directly | rejected |
| Cache ↔ etcd broken, client ↔ etcd fine | etcd directly | `ErrStoreUnavailable` | etcd directly | queued, retried with backoff |
| etcd down | `ErrStoreUnavailable` | `ErrStoreUnavailable` | `ErrStoreUnavailable` | queued, retried with backoff |

Only failures of the cache itself (unreachable, timeouts, internal errors)
count against the circuit breaker. etcd failures reported by the cache,
backpressure and caller cancellations do not. After a cache restart the
client registers its namespace again transparently.

## Client usage

```go
cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{"etcd:2379"}})

c, err := client.New(ctx, client.Config{
	Target:    "dns:///proplyd:7070",
	Namespace: "magnetar",
	Spec: protocol.Namespace{
		Read:      protocol.ReadThrough,
		Write:     protocol.WriteAround,
		Eviction:  protocol.EvictSIEVE,
		Capacity:  50_000,
		TTL:       10 * time.Minute,
		KeyPrefix: "/magnetar/",
	},
	// Direct etcd access for fallback (and cache-aside loads).
	Store: etcd.New(cli, "/magnetar/"),
})

err = c.Set(ctx, "nodes/n1", payload, client.WithTTL(time.Minute))
val, found, err := c.Get(ctx, "nodes/n1")
item, err := c.Lookup(ctx, "nodes/n1") // + Source (cache/store) and ModRevision
```

`client.Config` also controls the timeouts (`RequestTimeout` for reads,
default 500ms; `WriteTimeout`, default 2s) and the circuit breaker
(consecutive failures, failure ratio, open duration, half-open probes).
`c.State()` and `c.Metrics()` expose the breaker state and fallback counters.

## Running the server

```
proplyd -listen :7070 -etcd-endpoints etcd-0:2379,etcd-1:2379 \
        -allowed-prefixes /magnetar/,/kuiper/ -shutdown-timeout 30s
```

| Flag | Env | Default |
|---|---|---|
| `-listen` | `PROPLYD_LISTEN` | `:7070` |
| `-etcd-endpoints` | `PROPLYD_ETCD_ENDPOINTS` | `localhost:2379` |
| `-allowed-prefixes` | `PROPLYD_ALLOWED_PREFIXES` | any |
| `-tls-cert` / `-tls-key` | `PROPLYD_TLS_CERT` / `PROPLYD_TLS_KEY` | plaintext |
| `-log-level` | `PROPLYD_LOG_LEVEL` | `info` |
| `-load-timeout`, `-shutdown-timeout`, `-etcd-dial-timeout` | | 5s, 30s, 5s |

The server also exposes the standard `grpc.health.v1` service.

## Tests

```
make test         # everything, with -race (embedded etcd included)
make test-short   # fast subset
make stress       # 30s, 16 clients per namespace
make chaos        # 60s of continuous fault injection
```

- **Engine unit tests** (`cache/`): every strategy, stale-fill races,
  load deduplication, refresh-ahead, TTL, each eviction policy, watch
  updates, resume and compaction resync, write-behind coalescing, outage
  retry, eviction of pending entries, backpressure, flush on close, and loss
  on crash.
- **Client tests** (`client/`), over real gRPC and TCP:
  - every read × write combination;
  - the cache never started, crashed and restarted (breaker open → half-open → closed, transparent re-registration);
  - slow cache, client partition (write-around keeps the cache in sync, write-through rejects);
  - etcd unreachable from the cache only, and etcd down;
  - write-behind lost on crash vs flushed on graceful shutdown;
  - backpressure, conflicts, caller cancellation.
- **Stress** (`tests/TestStress`): about 48 clients over six namespaces covering
  all strategy families. It checks read-your-writes on every read of a
  client's own keys, the exact final state in etcd, and cache/etcd
  convergence for every key.
- **Chaos** (`tests/TestChaos`): the same workload while crashing the cache,
  partitioning and resetting connections, slowing or killing the store,
  breaking the cache→store link, and dropping or compacting watch streams.
  After the chaos it verifies that no unexpected errors surfaced, that
  acknowledged write-through and write-around writes were never lost, and
  that every key converges.
- **End-to-end with etcd** (`tests/TestEndToEndWithEtcd`, `backend/etcd`),
  using embedded etcd.

## Extending

- **Distributed cache:** implement `store.Store` (for example on
  Olric/Redis) and pass it as `cache.Options.Stores`. Strategy and
  consistency logic stay in the engine. Note that write-behind state and the
  change log are per process today, so a multi-node cache also needs to
  distribute or partition those, for example by consistent-hashing
  namespaces to nodes.
- **Other databases:** implement `backend.Backend` (and `backend.Watcher`
  for change streams) and pass it as `cache.Options.Backends`.

## Known issues / notes

- **samber/hot v0.13.1:**
  - `arc` does not enforce its capacity (it grew to 26k entries at capacity
    1k in our benchmark), so it is rejected at registration.
  - `tinylfu`/`wtinylfu` filter admissions aggressively and hold far fewer
    entries than their capacity.
  - Its built-in sharding creates unlocked shards, so `store.Hot` does its
    own sharding.
- Watch-applied updates use the namespace TTL; per-key TTL overrides apply
  to writes and fills only.
- No authentication yet: use TLS plus `-allowed-prefixes`, and add mTLS or
  per-namespace auth before exposing the cache beyond the cluster.
