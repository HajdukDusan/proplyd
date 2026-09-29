package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c12s/proplyd/backend"
	"github.com/c12s/proplyd/backend/memory"
	"github.com/c12s/proplyd/protocol"
)

const prefix = "/test/"

func testLogger() *slog.Logger {
	if os.Getenv("PROPLYD_TEST_LOGS") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newEngine(t *testing.T, db *memory.Store) *Engine {
	t.Helper()
	e, err := New(Options{
		Backends:     db.Factory(),
		Logger:       testLogger(),
		LoadTimeout:  time.Second,
		FlushTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Close(ctx)
	})
	return e
}

func register(t *testing.T, e *Engine, ns string, cfg protocol.Namespace) {
	t.Helper()
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = prefix
	}
	if _, _, err := e.Register(ns, cfg); err != nil {
		t.Fatalf("register: %v", err)
	}
}

func mustGet(t *testing.T, e *Engine, ns, key string) Result {
	t.Helper()
	r, err := e.Get(context.Background(), ns, key)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	return r
}

func put(t *testing.T, db *memory.Store, key, value string) int64 {
	t.Helper()
	rev, err := db.Backend(prefix).Put(context.Background(), key, []byte(value))
	if err != nil {
		t.Fatal(err)
	}
	return rev
}

func eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out: " + msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitWatch waits until a watching namespace has started its watch.
func waitWatch(t *testing.T, e *Engine, ns string) {
	t.Helper()
	eventually(t, func() bool { s, _ := e.Stats(ns); return s.WatchResyncs > 0 }, "watch did not start")
}

// ---------------------------------------------------------------- registration

func TestRegister(t *testing.T) {
	e := newEngine(t, memory.NewStore())

	cfg, created, err := e.Register("a", protocol.Namespace{KeyPrefix: prefix})
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if cfg.Capacity != protocol.DefaultCapacity || cfg.Eviction != protocol.EvictSIEVE || cfg.Sync != protocol.SyncNone {
		t.Fatalf("defaults not applied: %+v", cfg)
	}

	// Same config again: idempotent.
	if _, created, err := e.Register("a", protocol.Namespace{KeyPrefix: prefix}); err != nil || created {
		t.Fatalf("re-register: created=%v err=%v", created, err)
	}
	// Different config: conflict.
	if _, _, err := e.Register("a", protocol.Namespace{KeyPrefix: prefix, Write: protocol.WriteBehind}); !errors.Is(err, ErrNamespaceConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	// Invalid config.
	for name, cfg := range map[string]protocol.Namespace{
		"no prefix":        {},
		"bad eviction":     {KeyPrefix: prefix, Eviction: "random"},
		"arc (unbounded)":  {KeyPrefix: prefix, Eviction: protocol.EvictARC},
		"refresh w/o ttl":  {KeyPrefix: prefix, Read: protocol.RefreshAhead},
		"refresh > ttl":    {KeyPrefix: prefix, Read: protocol.RefreshAhead, TTL: time.Second, RefreshAfter: 2 * time.Second},
		"negative ttl":     {KeyPrefix: prefix, TTL: -time.Second},
		"unknown strategy": {KeyPrefix: prefix, Write: 42},
	} {
		if _, _, err := e.Register("bad-"+name, cfg); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: want invalid argument, got %v", name, err)
		}
	}
	if _, err := e.Get(context.Background(), "missing", "k"); !errors.Is(err, ErrNamespaceNotFound) {
		t.Fatalf("want namespace not found, got %v", err)
	}
}

func TestPrefixPolicy(t *testing.T) {
	db := memory.NewStore()
	e, _ := New(Options{Backends: db.Factory(), PrefixPolicy: func(ns, p string) error {
		if p != "/"+ns+"/" {
			return errors.New("namespace must use its own prefix")
		}
		return nil
	}})
	defer e.Close(context.Background())
	if _, _, err := e.Register("svc", protocol.Namespace{KeyPrefix: "/other/"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("want rejection, got %v", err)
	}
	if _, _, err := e.Register("svc", protocol.Namespace{KeyPrefix: "/svc/"}); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------- reads

func TestReadThrough(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Read: protocol.ReadThrough})
	rev := put(t, db, "k", "v1")

	r := mustGet(t, e, "ns", "k")
	if r.Outcome != OutcomeLoaded || string(r.Value) != "v1" || !r.Found || r.ModRevision != rev {
		t.Fatalf("first get: %+v", r)
	}
	r = mustGet(t, e, "ns", "k")
	if r.Outcome != OutcomeHit || string(r.Value) != "v1" {
		t.Fatalf("second get: %+v", r)
	}
	// Missing key without negative caching: loaded every time.
	for range 2 {
		if r := mustGet(t, e, "ns", "absent"); r.Found || r.Outcome != OutcomeLoaded {
			t.Fatalf("absent: %+v", r)
		}
	}
}

func TestReadThroughDeduplicatesConcurrentLoads(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{})
	put(t, db, "k", "v")
	db.SetLatency(50 * time.Millisecond)
	readsBefore, _ := db.Counts()

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r, err := e.Get(context.Background(), "ns", "k"); err != nil || string(r.Value) != "v" {
				t.Errorf("get: %+v %v", r, err)
			}
		}()
	}
	wg.Wait()
	if reads, _ := db.Counts(); reads-readsBefore != 1 {
		t.Fatalf("want 1 store read for 100 concurrent misses, got %d", reads-readsBefore)
	}
}

func TestLoadIsDetachedFromCallerContext(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{})
	put(t, db, "k", "v")
	db.SetLatency(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := e.Get(ctx, "ns", "k"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
	// A patient caller joining the same in-flight load still gets the value.
	if r := mustGet(t, e, "ns", "k"); string(r.Value) != "v" {
		t.Fatalf("patient caller: %+v", r)
	}
}

func TestNegativeCaching(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{CacheMisses: true})

	mustGet(t, e, "ns", "absent")
	readsBefore, _ := db.Counts()
	r := mustGet(t, e, "ns", "absent")
	if r.Found || r.Outcome != OutcomeHit {
		t.Fatalf("want cached miss, got %+v", r)
	}
	if reads, _ := db.Counts(); reads != readsBefore {
		t.Fatal("negative entry should not hit the store")
	}
	// Writing the key through the cache replaces the negative entry.
	if _, err := e.Set(context.Background(), "ns", "absent", []byte("now"), 0); err != nil {
		t.Fatal(err)
	}
	if r := mustGet(t, e, "ns", "absent"); !r.Found || string(r.Value) != "now" {
		t.Fatalf("after set: %+v", r)
	}
}

func TestBackendErrorOnLoad(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{})
	db.SetDown(true)
	_, err := e.Get(context.Background(), "ns", "k")
	var be *BackendError
	if !errors.As(err, &be) {
		t.Fatalf("want BackendError, got %v", err)
	}
	db.SetDown(false)
	mustGet(t, e, "ns", "k")
}

func TestCacheAside(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Read: protocol.CacheAside})
	put(t, db, "k", "v1")

	if r := mustGet(t, e, "ns", "k"); r.Outcome != OutcomeMiss {
		t.Fatalf("want miss, got %+v", r)
	}
	rec, _ := db.Backend(prefix).Get(context.Background(), "k")
	if ok, err := e.Fill("ns", "k", rec, 0); !ok || err != nil {
		t.Fatalf("fill: %v %v", ok, err)
	}
	if r := mustGet(t, e, "ns", "k"); r.Outcome != OutcomeHit || string(r.Value) != "v1" {
		t.Fatalf("after fill: %+v", r)
	}
}

// The classic cache-aside race: a reader loads an old value, a writer updates
// the key through the cache, then the reader's fill arrives. The fill must
// be rejected.
func TestStaleFillRejectedAfterWrite(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Read: protocol.CacheAside})
	put(t, db, "k", "old")

	stale, _ := db.Backend(prefix).Get(context.Background(), "k") // reader
	if _, err := e.Set(context.Background(), "ns", "k", []byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	e.Invalidate("ns", "k") // make sure it is not cached, so only the change log protects us
	if ok, _ := e.Fill("ns", "k", stale, 0); ok {
		t.Fatal("stale fill was accepted")
	}
	if r := mustGet(t, e, "ns", "k"); r.Outcome != OutcomeMiss {
		t.Fatalf("stale value got cached: %+v", r)
	}
	s, _ := e.Stats("ns")
	if s.FillsRejected != 1 {
		t.Fatalf("want 1 rejected fill, got %d", s.FillsRejected)
	}
}

// Same race, but the writer bypasses the cache; the store watch must catch it.
func TestStaleFillRejectedAfterExternalWrite(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Read: protocol.CacheAside, Sync: protocol.SyncWatch})
	waitWatch(t, e, "ns")
	put(t, db, "k", "old")

	stale, _ := db.Backend(prefix).Get(context.Background(), "k")
	put(t, db, "k", "new")
	eventually(t, func() bool { s, _ := e.Stats("ns"); return s.WatchEvents >= 2 }, "watch events")
	if ok, _ := e.Fill("ns", "k", stale, 0); ok {
		t.Fatal("stale fill accepted")
	}
}

func TestFillOlderThanCachedIsIgnored(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Read: protocol.CacheAside})
	put(t, db, "k", "v1")
	old, _ := db.Backend(prefix).Get(context.Background(), "k")
	put(t, db, "k", "v2")
	cur, _ := db.Backend(prefix).Get(context.Background(), "k")

	e.Fill("ns", "k", cur, 0)
	if ok, _ := e.Fill("ns", "k", old, 0); ok {
		t.Fatal("older fill accepted")
	}
	if r := mustGet(t, e, "ns", "k"); string(r.Value) != "v2" {
		t.Fatalf("got %q", r.Value)
	}
}

func TestFillValidation(t *testing.T) {
	e := newEngine(t, memory.NewStore())
	register(t, e, "ns", protocol.Namespace{Read: protocol.CacheAside, MaxValueBytes: 4})
	if _, err := e.Fill("ns", "k", backend.Record{Found: true, Value: []byte("v"), ModRevision: 5, Revision: 3}, 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("want invalid argument, got %v", err)
	}
	if _, err := e.Fill("ns", "k", backend.Record{Found: true, Value: []byte("too big"), ModRevision: 1, Revision: 1}, 0); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("want too large, got %v", err)
	}
}

func TestRefreshAhead(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Read: protocol.RefreshAhead, TTL: time.Minute, RefreshAfter: 50 * time.Millisecond})
	put(t, db, "k", "v1")
	mustGet(t, e, "ns", "k")
	put(t, db, "k", "v2") // external change, no watch

	time.Sleep(80 * time.Millisecond)
	// The old value is served immediately while a refresh runs in background.
	if r := mustGet(t, e, "ns", "k"); r.Outcome != OutcomeHit || string(r.Value) != "v1" {
		t.Fatalf("want stale hit, got %+v", r)
	}
	eventually(t, func() bool { return string(mustGet(t, e, "ns", "k").Value) == "v2" }, "refresh")
	if s, _ := e.Stats("ns"); s.Refreshes == 0 {
		t.Fatal("no refresh recorded")
	}
}

func TestTTLExpiry(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{TTL: 50 * time.Millisecond})
	put(t, db, "k", "v")
	mustGet(t, e, "ns", "k")
	if r := mustGet(t, e, "ns", "k"); r.Outcome != OutcomeHit {
		t.Fatal("expected hit")
	}
	time.Sleep(80 * time.Millisecond)
	if r := mustGet(t, e, "ns", "k"); r.Outcome != OutcomeLoaded {
		t.Fatalf("expected reload after ttl, got %+v", r)
	}
	// Per-key TTL override.
	e.Set(context.Background(), "ns", "long", []byte("v"), time.Hour)
	time.Sleep(80 * time.Millisecond)
	if r := mustGet(t, e, "ns", "long"); r.Outcome != OutcomeHit {
		t.Fatalf("per-key ttl ignored: %+v", r)
	}
}

func TestEvictionPolicies(t *testing.T) {
	for _, policy := range []string{
		protocol.EvictLRU, protocol.EvictLFU, protocol.EvictTinyLFU, protocol.EvictWTinyLFU,
		protocol.EvictTwoQueue, protocol.EvictFIFO, protocol.EvictSIEVE,
	} {
		t.Run(policy, func(t *testing.T) {
			db := memory.NewStore()
			e := newEngine(t, db)
			register(t, e, "ns", protocol.Namespace{Eviction: policy, Capacity: 100})
			for i := range 1000 {
				if _, err := e.Set(context.Background(), "ns", fmt.Sprint(i), []byte("v"), 0); err != nil {
					t.Fatal(err)
				}
			}
			s, _ := e.Stats("ns")
			if s.Entries == 0 || s.Entries > 100 {
				t.Fatalf("entries=%d, want 1..100", s.Entries)
			}
			// Evicted keys are still readable through the store.
			for i := range 1000 {
				if r := mustGet(t, e, "ns", fmt.Sprint(i)); !r.Found {
					t.Fatalf("key %d lost", i)
				}
			}
		})
	}
}

// ---------------------------------------------------------------- writes

func TestWriteThrough(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Write: protocol.WriteThrough})
	ctx := context.Background()

	rev, err := e.Set(ctx, "ns", "k", []byte("v1"), 0)
	if err != nil || rev == 0 {
		t.Fatalf("set: %d %v", rev, err)
	}
	if v, _ := db.Peek(prefix + "k"); string(v) != "v1" {
		t.Fatalf("store has %q", v)
	}
	if r := mustGet(t, e, "ns", "k"); r.Outcome != OutcomeHit || string(r.Value) != "v1" || r.ModRevision != rev {
		t.Fatalf("get: %+v", r)
	}
	if _, err := e.Delete(ctx, "ns", "k"); err != nil {
		t.Fatal(err)
	}
	if _, ok := db.Peek(prefix + "k"); ok {
		t.Fatal("store still has key")
	}
	if r := mustGet(t, e, "ns", "k"); r.Found {
		t.Fatalf("deleted key found: %+v", r)
	}

	// Store down: the write fails and the cache is not modified.
	e.Set(ctx, "ns", "k", []byte("v2"), 0)
	db.SetDown(true)
	_, err = e.Set(ctx, "ns", "k", []byte("v3"), 0)
	var be *BackendError
	if !errors.As(err, &be) {
		t.Fatalf("want BackendError, got %v", err)
	}
	if r := mustGet(t, e, "ns", "k"); string(r.Value) != "v2" {
		t.Fatalf("cache diverged: %q", r.Value)
	}
	db.SetDown(false)
}

func TestValueTooLarge(t *testing.T) {
	e := newEngine(t, memory.NewStore())
	register(t, e, "ns", protocol.Namespace{MaxValueBytes: 8})
	if _, err := e.Set(context.Background(), "ns", "k", make([]byte, 9), 0); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("want ErrValueTooLarge, got %v", err)
	}
	if _, err := e.Set(context.Background(), "ns", "", []byte("v"), 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty key: %v", err)
	}
}

func TestWriteAroundInvalidates(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Write: protocol.WriteAround})
	waitWatch(t, e, "ns")
	ctx := context.Background()
	put(t, db, "k", "v1")
	mustGet(t, e, "ns", "k")

	if _, err := e.Set(ctx, "ns", "k", []byte("v2"), 0); err != nil {
		t.Fatal(err)
	}
	r := mustGet(t, e, "ns", "k")
	if r.Outcome != OutcomeLoaded || string(r.Value) != "v2" {
		t.Fatalf("want reload of v2, got %+v", r)
	}
}

// Write-around namespaces watch the store: writes made while clients could not
// reach the cache (or by anyone else) update cached entries.
func TestWatchAppliesExternalChanges(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Write: protocol.WriteAround})
	waitWatch(t, e, "ns")
	put(t, db, "k", "v1")
	put(t, db, "gone", "x")
	mustGet(t, e, "ns", "k")
	mustGet(t, e, "ns", "gone")

	put(t, db, "k", "v2")
	db.Backend(prefix).Delete(context.Background(), "gone")
	put(t, db, "uncached", "u")

	eventually(t, func() bool {
		r := mustGet(t, e, "ns", "k")
		return r.Outcome == OutcomeHit && string(r.Value) == "v2"
	}, "cached key updated in place")
	if r := mustGet(t, e, "ns", "gone"); r.Found {
		t.Fatalf("deleted key still cached: %+v", r)
	}
	// Uncached keys are not pulled into the cache by the watch.
	if r := mustGet(t, e, "ns", "uncached"); r.Outcome != OutcomeLoaded {
		t.Fatalf("uncached: %+v", r)
	}
}

func TestWatchResumesAfterInterruption(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Write: protocol.WriteAround})
	waitWatch(t, e, "ns")
	put(t, db, "k", "v1")
	mustGet(t, e, "ns", "k")

	db.BreakWatches(errors.New("connection lost"))
	put(t, db, "k", "v2") // happens while the watch is down
	eventually(t, func() bool { return string(mustGet(t, e, "ns", "k").Value) == "v2" }, "missed event replayed")
	if s, _ := e.Stats("ns"); s.WatchResyncs != 1 {
		t.Fatalf("resume must not resync, resyncs=%d", s.WatchResyncs)
	}
}

func TestWatchResyncsAfterCompaction(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Write: protocol.WriteAround})
	waitWatch(t, e, "ns")
	put(t, db, "k", "v1")
	mustGet(t, e, "ns", "k")

	db.PauseWatches(true) // the cache loses its watch, writers carry on
	put(t, db, "k", "v2")
	db.Compact() // the missed event is gone from history
	db.PauseWatches(false)

	eventually(t, func() bool { s, _ := e.Stats("ns"); return s.WatchResyncs >= 2 }, "resync")
	if r := mustGet(t, e, "ns", "k"); string(r.Value) != "v2" {
		t.Fatalf("stale value after resync: %q", r.Value)
	}
}

func TestWriteBehind(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Write: protocol.WriteBehind, FlushInterval: 20 * time.Millisecond})
	ctx := context.Background()

	if _, err := e.Set(ctx, "ns", "k", []byte("v1"), 0); err != nil {
		t.Fatal(err)
	}
	// Read-your-writes before the flush.
	if r := mustGet(t, e, "ns", "k"); string(r.Value) != "v1" || r.ModRevision != 0 {
		t.Fatalf("pending read: %+v", r)
	}
	eventually(t, func() bool { v, _ := db.Peek(prefix + "k"); return string(v) == "v1" }, "flush")
	eventually(t, func() bool { return mustGet(t, e, "ns", "k").ModRevision > 0 }, "entry committed")

	e.Delete(ctx, "ns", "k")
	if r := mustGet(t, e, "ns", "k"); r.Found {
		t.Fatal("pending delete not visible")
	}
	eventually(t, func() bool { _, ok := db.Peek(prefix + "k"); return !ok }, "delete flushed")
}

func TestWriteBehindCoalesces(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Write: protocol.WriteBehind, FlushInterval: time.Hour})
	ctx := context.Background()
	_, writesBefore := db.Counts()
	for i := range 1000 {
		e.Set(ctx, "ns", fmt.Sprintf("k%d", i%10), []byte(fmt.Sprint(i)), 0)
	}
	if err := e.Flush(ctx, "ns"); err != nil {
		t.Fatal(err)
	}
	_, writes := db.Counts()
	if writes-writesBefore != 1 {
		t.Fatalf("1000 writes to 10 keys should be one transaction, got %d", writes-writesBefore)
	}
	for i := range 10 {
		v, _ := db.Peek(fmt.Sprintf("%sk%d", prefix, i))
		if want := fmt.Sprint(990 + i); string(v) != want {
			t.Fatalf("k%d: got %s want %s", i, v, want)
		}
	}
}

func TestWriteBehindSurvivesStoreOutage(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Write: protocol.WriteBehind, FlushInterval: 10 * time.Millisecond})
	db.SetDown(true)
	e.Set(context.Background(), "ns", "k", []byte("v"), 0)
	time.Sleep(100 * time.Millisecond)
	s, _ := e.Stats("ns")
	if s.PendingWrites != 1 || s.FlushErrors == 0 {
		t.Fatalf("stats during outage: %+v", s)
	}
	if r := mustGet(t, e, "ns", "k"); string(r.Value) != "v" {
		t.Fatal("pending value not readable during outage")
	}
	db.SetDown(false)
	eventually(t, func() bool { v, _ := db.Peek(prefix + "k"); return string(v) == "v" }, "flush after recovery")
}

func TestWriteBehindPendingSurvivesEviction(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Write: protocol.WriteBehind, FlushInterval: time.Hour, Capacity: 10, Eviction: protocol.EvictFIFO})
	for i := range 100 {
		e.Set(context.Background(), "ns", fmt.Sprint(i), []byte(fmt.Sprint(i)), 0)
	}
	for i := range 100 {
		if r := mustGet(t, e, "ns", fmt.Sprint(i)); string(r.Value) != fmt.Sprint(i) {
			t.Fatalf("pending key %d lost after eviction: %+v", i, r)
		}
	}
	e.Flush(context.Background(), "ns")
	for i := range 100 {
		if v, _ := db.Peek(prefix + fmt.Sprint(i)); string(v) != fmt.Sprint(i) {
			t.Fatalf("key %d not flushed", i)
		}
	}
}

func TestWriteBehindBackpressure(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Write: protocol.WriteBehind, FlushInterval: time.Hour, MaxPending: 5})
	db.SetDown(true)
	ctx := context.Background()
	for i := range 5 {
		if _, err := e.Set(ctx, "ns", fmt.Sprint(i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.Set(ctx, "ns", "overflow", []byte("v"), 0); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("want backpressure, got %v", err)
	}
	// Overwriting an already pending key is still allowed.
	if _, err := e.Set(ctx, "ns", "0", []byte("v2"), 0); err != nil {
		t.Fatal(err)
	}
}

func TestCloseFlushesWriteBehind(t *testing.T) {
	db := memory.NewStore()
	e, _ := New(Options{Backends: db.Factory(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	register(t, e, "ns", protocol.Namespace{Write: protocol.WriteBehind, FlushInterval: time.Hour})
	for i := range 500 {
		e.Set(context.Background(), "ns", fmt.Sprint(i), []byte("v"), 0)
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := range 500 {
		if _, ok := db.Peek(prefix + fmt.Sprint(i)); !ok {
			t.Fatalf("key %d not flushed on close", i)
		}
	}
	if _, err := e.Get(context.Background(), "ns", "0"); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

func TestCloseWithExpiredContextLosesPendingWrites(t *testing.T) {
	db := memory.NewStore()
	e, _ := New(Options{Backends: db.Factory(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	register(t, e, "ns", protocol.Namespace{Write: protocol.WriteBehind, FlushInterval: time.Hour})
	e.Set(context.Background(), "ns", "k", []byte("v"), 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.Close(ctx); err == nil {
		t.Fatal("expected an error reporting lost writes")
	}
	if _, ok := db.Peek(prefix + "k"); ok {
		t.Fatal("unexpected flush")
	}
}

func TestPurgeKeepsPendingWrites(t *testing.T) {
	db := memory.NewStore()
	e := newEngine(t, db)
	register(t, e, "ns", protocol.Namespace{Write: protocol.WriteBehind, FlushInterval: time.Hour})
	e.Set(context.Background(), "ns", "k", []byte("v"), 0)
	e.Purge("ns")
	if r := mustGet(t, e, "ns", "k"); string(r.Value) != "v" {
		t.Fatal("pending write lost by purge")
	}
}

// ---------------------------------------------------------------- concurrency

// Concurrent writers, readers and external changes on a small key space must
// always converge to the store state.
func TestConcurrentConvergence(t *testing.T) {
	for _, w := range []protocol.WriteStrategy{protocol.WriteThrough, protocol.WriteAround, protocol.WriteBehind} {
		for _, r := range []protocol.ReadStrategy{protocol.ReadThrough, protocol.CacheAside} {
			t.Run(fmt.Sprintf("w%d-r%d", w, r), func(t *testing.T) {
				db := memory.NewStore()
				e := newEngine(t, db)
				register(t, e, "ns", protocol.Namespace{Read: r, Write: w, Sync: protocol.SyncWatch, Capacity: 50, FlushInterval: 5 * time.Millisecond})
				waitWatch(t, e, "ns")
				ctx := context.Background()
				be := db.Backend(prefix)
				var stop atomic.Bool
				var wg sync.WaitGroup
				for g := range 8 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := 0; !stop.Load(); i++ {
							key := fmt.Sprintf("k%d", (g*7+i)%20)
							switch i % 5 {
							case 0:
								e.Set(ctx, "ns", key, []byte(fmt.Sprintf("%d-%d", g, i)), 0)
							case 1:
								if w != protocol.WriteBehind { // external writers race with pending flushes by design
									be.Put(ctx, key, []byte(fmt.Sprintf("ext-%d-%d", g, i)))
								}
							case 2:
								e.Delete(ctx, "ns", key)
							default:
								res, _ := e.Get(ctx, "ns", key)
								if res.Outcome == OutcomeMiss {
									rec, err := be.Get(ctx, key)
									if err == nil {
										e.Fill("ns", key, rec, 0)
									}
								}
							}
						}
					}()
				}
				time.Sleep(300 * time.Millisecond)
				stop.Store(true)
				wg.Wait()
				e.Flush(ctx, "ns")

				for i := range 20 {
					key := fmt.Sprintf("k%d", i)
					want, wantOK := db.Peek(prefix + key)
					eventually(t, func() bool {
						res := mustGet(t, e, "ns", key)
						if res.Outcome == OutcomeMiss {
							return true // nothing cached: trivially consistent
						}
						return res.Found == wantOK && string(res.Value) == string(want)
					}, fmt.Sprintf("key %s: cache diverged from store (%q)", key, want))
				}
			})
		}
	}
}
