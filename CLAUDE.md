# CLAUDE.md — proplyd

Centralized cache service for c12s (https://github.com/c12s), a k8s-like
platform built on unikernels, so clients may run on small or constrained
devices. Go only; all communication is gRPC. README.md is the user-facing
doc. This file holds the context and decisions that the code alone doesn't
explain.

## Decisions the user made (do not change without asking)

- **The cache talks to the DB (etcd) directly.** It does not use client
  callbacks. The client also has direct etcd access (`client.Config.Store`),
  used only for fallback and cache-aside loads.
- **Fallback when the cache is unreachable (crash, partition, timeout):**
  - reads go to etcd directly;
  - **write-around** writes go to etcd directly ("every write recorded, cache may briefly differ");
  - **write-through** returns `ErrCacheUnavailable` and writes nothing ("cache always equals DB, some writes rejected");
  - **write-behind** also rejects. On a crash, unflushed data is lost; this is **documented only**. The user explicitly declined a client-side journal or durability option.
- **The "check DB vs cache" requirement** is implemented as an etcd watch
  (`SyncMode`). AUTO means WATCH for write-around and NONE otherwise.
  Write-through doesn't need it unless something writes etcd behind the
  cache's back.
- **Strategies are scoped per namespace.** Re-registering a namespace with a
  different config is an error (`NAMESPACE_CONFLICT`).
- **The store is samber/hot, caching `[]byte`** to limit GC pressure.
  `store.Store` is the seam for a future distributed cache.
- **User preferences:**
  - ask when unsure about design choices, offering options with trade-offs;
  - wants "professional" quality: circuit breakers, thorough tests, and a
    stress test with many clients plus edge-case and chaos tests.

## Environment gotchas

- The local Go is **1.24.0**. Always run Go with `GOTOOLCHAIN=local`,
  otherwise newer dependencies silently switch the toolchain. Dependencies
  are pinned to versions compatible with Go 1.24:
  - grpc v1.73.0, protobuf v1.36.6;
  - etcd v3.6.4, gobreaker/v2 v2.4.0, x/sync v0.16.0, samber/hot v0.13.1.
- **Codegen:** run `buf generate` (buf.yaml / buf.gen.yaml) with
  `protoc-gen-go@v1.36.6` and `protoc-gen-go-grpc@v1.5.1` from `~/go/bin`.
  Use `PATH=$HOME/go/bin:$PATH`. There is no protoc; buf doesn't need it.
  - `buf lint` requires enum zero values to end in `_UNSPECIFIED`.
  - Commit the generated code in `api/proplydpb`.
- The macOS linker prints `ld: warning: ... malformed LC_DYSYMTAB` during
  tests. It is harmless; filter it with `grep -v "ld: warning"`.
- staticcheck is at `~/go/bin/staticcheck`. Keep `gofmt -l .`, `go vet` and
  staticcheck clean.
- Embedded etcd for tests: `internal/testutil.StartEtcd`, with log level set
  to `panic` to silence shutdown noise.

## Architecture (where things live)

- `protocol/`: namespace config (Go type ↔ proto conversion, defaults,
  validation) and ErrorInfo reasons. It is shared by client and server and
  **must not import `cache`**, to keep client binaries small.
- `cache/`: the engine.
  - `engine.go`: namespace registry.
  - `namespace.go`: reads, writes, `installLocked`.
  - `writebehind.go`: dirty map and flusher.
  - `watch.go`: etcd watch, resync.
  - `changelog.go`: stale-fill protection.
  - `entry.go`: byte encoding.
- `store/`: `Store` interface plus `Hot` (samber/hot with its **own**
  sharding: 16 shards at capacity ≥ 4096).
- `backend/`: `Backend`/`Watcher` interfaces, `etcd` impl, and a `memory`
  impl. The memory impl is etcd-like (revisions, history, compaction) and
  supports fault injection:
  - `SetDown`, `SetLatency`;
  - `Compact`, `BreakWatches`, `PauseWatches`.
- `server/`: gRPC service with error→status mapping via `errdetails.ErrorInfo`
  reasons, plus the health service. `runner.go` has `Start`/`Shutdown`
  (flushes write-behind)/`Kill` (crash).
- `client/`: breaker (gobreaker v2), timeouts, fallbacks, transparent
  re-registration after a cache restart (on `NAMESPACE_NOT_FOUND`).
- `internal/testutil/`:
  - `Proxy` (Forward/Refuse/Blackhole, switchable upstream);
  - `CacheNode` (Kill/Start/Restart/Shutdown behind a stable proxy address);
  - `Faults` (breaks only the cache→store link);
  - `NewClient`, `Eventually`, `StartEtcd`.

## Core invariants (the tests depend on these)

- **Entry format:** a single `[]byte` = flags | version (int64) |
  storedAt (int64) | value.
  - version is the etcd ModRevision for present values, or the revision at
    which absence was observed for negative entries.
  - pending (unflushed write-behind) entries have version 0.
- **Every per-key mutation happens under that key's stripe lock** (256
  stripes). This covers install, write, watch event, flush commit and
  invalidate. `purge` and `resync` take all stripe locks.
- **`installLocked` admits data only if all of these hold:**
  - no pending write-behind for the key;
  - `changeLog.admits(key, readRev)`, i.e. no known change newer than the
    read and readRev ≥ floor;
  - the incoming data is not older than the cached entry.
- **Change log:** a bounded ring. When it forgets a change, the floor rises
  to that change's revision.
  - `block()` rejects all installs until the watch knows its start revision.
  - `reset(rev)` is called on resync.
- **Own writes** use `applyOwnWrite`: if the install is rejected, an older
  cached value is deleted, never left behind.
- **Write-behind:** the dirty map is the source of truth, and Get checks it
  first. Store copies of pending entries may be evicted.
  - A flush retires a dirty entry only if its `seq` is unchanged.
  - Watch events for dirty keys are ignored (last writer wins).
- **Watch:**
  - it resumes from `next` after transient errors;
  - on `ErrCompacted` it runs `block` → purge → `reset(current)`;
  - it updates cached keys in place and never inserts uncached keys.
- **Startup order:** `newNamespace` must initialize all state **before**
  starting goroutines. A real race in exactly this spot was found and fixed.
- **Load deduplication:** read-through loads use singleflight `DoChan` with
  a context detached from the caller (`LoadTimeout`).
- **Client breaker:** only cache failures count. These are **neutral** and
  don't count:
  - `BACKEND_UNAVAILABLE` (the fallback still runs);
  - backpressure;
  - invalid argument or conflict;
  - the caller's own context cancel.

  Neutral errors are wrapped in `*neutral` and excluded via
  `Settings.IsExcluded`.
- **Client timeouts:** `RequestTimeout` (reads, default 500ms) and
  `WriteTimeout` (writes, default 2s). The test helper uses 250ms for both.

## samber/hot v0.13.1 bugs (verified with a benchmark)

- ARC ignores its capacity: 26,869 entries at capacity 1,000. It is
  **rejected at registration** in `protocol.Validate`.
- TinyLFU and W-TinyLFU underfill (13 and 157 entries of 1,000). They are
  allowed but documented. **SIEVE is the default** (72.5% hit rate, the best
  of the bounded policies).
- hot's `WithSharding` builds unlocked shards, so it is never used.
- hot loaders are not used either: they bypass the revision checks.
- Re-check all of this if hot is upgraded.

## Testing

- Commands:
  - `make test` runs everything with `-race`, in about 1 minute;
  - `make test-short` skips etcd and shortens stress/chaos;
  - `make stress` and `make chaos` run long versions.
  - Environment knobs: `PROPLYD_STRESS_DURATION`, `PROPLYD_CHAOS_DURATION`,
    `PROPLYD_STRESS_CLIENTS`, `PROPLYD_STRESS_KEYS`.
  - `PROPLYD_TEST_LOGS=1` enables debug logs.
- `tests/harness_test.go` uses single-writer key ownership so the checks
  can be exact:
  - read-your-writes (stress test only);
  - the final etcd value is the last acknowledged write or a later write
    with an unknown outcome;
  - under chaos, write-behind is allowed any written value;
  - the cache converges to etcd for every key.
  - Cache-aside without `CacheMisses` never caches absent keys, so a store
    read of an absent key counts as converged.
- The chaos injector cycles through shuffled rounds so every fault type
  fires. `settle()` must reconnect clients (which re-registers namespaces
  after a crash) **before** calling `Engine.Flush`.
- Before declaring work done, run the full suite several times with `-race`.
  Chaos flakiness has so far always been a harness bug; investigate the
  cause, don't retry until it passes.

## Known gaps / possible next steps

- No authentication. TLS flags and `-allowed-prefixes` exist; mTLS or
  per-namespace auth is not done.
- Namespaces are never deleted; there is no `DeleteNamespace` RPC.
- No batch RPCs (MGet/MSet) and no Prometheus endpoint (there is a `Stats`
  RPC).
- Watch-applied updates use the namespace TTL; per-key TTL overrides are
  lost on those updates.
- A distributed `store.Store` would also need to distribute or partition the
  write-behind dirty state and the change log, which are per process today.
- The repo is a git repo with one initial commit; the work is uncommitted.
  Commit only when the user asks.
