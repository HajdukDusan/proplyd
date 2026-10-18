# CLAUDE.md — proplyd

Central cache service for c12s (https://github.com/c12s), a k8s-like
platform on unikernels whose clients may be small edge devices. Go only; all
service communication is gRPC. The client library is the sibling repo
`../proplyd-go` (near-cache, breaker, fallback). README.md is the user doc;
this file holds decisions and invariants the code does not explain.

## Decisions the user made (don't change without asking)

- Architecture: **central cache = Redis behind proplyd**, plus a
  **near-cache in every client** (proplyd-go, samber/hot caching `[]byte`).
  Single Redis for now; it will become replicated, so proplyd stays
  stateless and every Redis key is hash-tagged `{namespace}`.
- **proplyd talks to the store (etcd) itself**: no client callbacks. Clients
  have direct store access only for fallback and cache-aside loads.
- **Write-behind is left out for now** (proto enum value 3 is reserved).
- Cache unreachable: **write-through returns an error; write-around writes
  the store directly**. The user asked how the cache then gets refreshed;
  the answer is the etcd watch (SyncWatch, the default for every namespace).
- Eviction: near-cache policies per client, plus per-namespace TTL in
  Redis; Redis `maxmemory-policy` is server-wide (recommend volatile-*).
- The user wants options with trade-offs and a recommendation when unsure,
  "professional" quality, and heavy testing (stress, bad network, chaos).

## Environment gotchas

- Local Go is 1.24.0: always `GOTOOLCHAIN=local`. Pins: grpc v1.73.0,
  protobuf v1.36.6, go-redis v9.22.0 (newest with `go 1.24`), etcd v3.6.4,
  x/sync v0.16.0.
- Codegen: `PATH=$HOME/go/bin:$PATH buf generate` (protoc-gen-go v1.36.6,
  protoc-gen-go-grpc v1.5.1; no protoc needed). buf lint STANDARD minus
  PACKAGE_DIRECTORY_MATCH (c12s keeps protos in `pkg/api/proto`).
- Tests need `redis-server` (Homebrew Redis 8.4 is installed;
  `PROPLYD_REDIS_SERVER` overrides). Docker is not running locally.
- macOS prints `ld: warning: ... LC_DYSYMTAB`; harmless, filter it out.
- Keep `gofmt -l .`, `go vet` and `~/go/bin/staticcheck` clean. gofmt
  rewrites `''` in doc comments into a typographic quote; avoid it.
- `proplydtest` imports `pkg/server` → `internal/engine`, so engine tests
  are an external package (`engine_test`) to avoid an import cycle.
  Embedded etcd lives in `proplydtest/etcdtest` so that proplyd-go's tests
  don't compile the etcd server. `proplydtest` takes its own small `TB`
  interface (not `testing.TB`), so that `../proplyd-playground` can use it
  outside tests.

## Invariants (tests depend on these)

- **Entry layout** (`internal/engine/entry.go`, read by Lua via GETRANGE +
  `struct.unpack`, and by proplyd-go's `verifyRedis`): kind(1) | epoch(8) |
  rev(8, BE) | storedAt ms(8) | value. Kinds: 1 value (rev = mod revision),
  2 negative (rev = revision at which absence was read), 3 fence (rev = the
  change's revision). Bit 0x80 of the kind byte is the **quiet flag**.
- **Every state change goes through `installScript`** with a mode:
  - `fill`: needs read_rev ≥ floor and ≥ fence. Equal rev refreshes the TTL.
  - `write`: write-through.
  - `fence`: write-around and deletes without negative caching.
  - `watch`: in place if cached, else a fence.
  - `drop`: explicit invalidate; publishes r=0.

  Everything except `fill` publishes to the stream. Comparisons: fences
  vs read_rev; values/negatives vs rev.
- **Quiet flag.** A fill into an empty slot, or over an older fence, sets
  it: the change the load saw may have bypassed the cache and not been
  published yet. Any later write/fence/watch at or below the entry's rev
  publishes and clears the flag (SETRANGE byte 0). A fill that replaces an
  older live state publishes at once. Without this, a load that beats the
  watcher swallows the invalidation and near-caches stay stale; the chaos
  test caught it, and `TestBypassingWriteSeenByLoadFirstIsStillPublished`
  guards it.
- **FenceTTL must exceed LoadTimeout** (checked in Options).
- **Epoch change = purge** (O(1)); `purgeScript` also XADDs a reset marker
  `x=<epoch bytes>`. Compaction resync sets floor = wrev = current store
  revision.
- **Readiness gate**: a namespace is served by an engine only after
  `meta.wrev` ≥ store revision observed at activation (`awaitSync`).
- **Stream resume** checks that the entry *at* the client's position still
  exists (XRANGE pos pos), or for 0-0 that `entries-added == length`.
  `max-deleted-entry-id` is useless: Redis only moves it on XDEL, not on
  trimming.
- **Watch batches**: `WatchBatch.Revision` means every event at or below it
  was delivered. For etcd, only progress notifications carry the header
  revision; event batches use the last event's revision.
- **Graceful shutdown** drains subscriptions first (`DrainSubscriptions`),
  otherwise GracefulStop waits on never-ending streams.
- After a backend write, the cache update runs with
  `context.WithoutCancel` (UpdateTimeout), so a caller giving up cannot
  leave the old value cached.
- **Store reset detection**: meta field `bid` holds the store identity
  (`backend.Identifier`; etcd cluster ID). At every watch session start,
  `storeReset` compares identity and `rev < wrev`; on a mismatch `rebase`
  runs `purgeScript` with rebase=1 (floor/wrev may move back, new epoch,
  reset marker). `awaitSync` won't mark ready while a reset is pending, and
  `applyBatch` returns `errStoreRegressed` if a watch reports a revision
  below wrev. Tests: `TestStoreReset*` (swappable backend).
- Redis restart detection: `run_id` change → `reloadAll` (stop and forget
  every namespace; the next request re-activates and the watch replays).
