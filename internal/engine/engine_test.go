package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c12s/proplyd/internal/engine"
	"github.com/c12s/proplyd/pkg/api"
	"github.com/c12s/proplyd/pkg/backend"
	"github.com/c12s/proplyd/pkg/backend/faulty"
	"github.com/c12s/proplyd/pkg/backend/memory"
	"github.com/c12s/proplyd/pkg/protocol"
	"github.com/c12s/proplyd/proplydtest"
)

const ns = "test"

type env struct {
	t     *testing.T
	redis *proplydtest.Redis
	store *memory.Store
	be    *faulty.Backend
	opts  engine.Options
}

func newEnv(t *testing.T, p proplydtest.Persistence) *env {
	t.Helper()
	r := proplydtest.StartRedis(t, p)
	store := memory.New(memory.Options{ProgressInterval: 50 * time.Millisecond})
	return &env{t: t, redis: r, store: store, be: faulty.New(store), opts: engine.Options{
		InstanceID:         "engine-1",
		LoadTimeout:        time.Second,
		FenceTTL:           5 * time.Second,
		ReadyTimeout:       2 * time.Second,
		LeaseTTL:           time.Second,
		HeartbeatInterval:  100 * time.Millisecond,
		RedisCheckInterval: 50 * time.Millisecond,
	}}
}

// engine starts an engine; override tweaks its options.
func (v *env) engine(override ...func(*engine.Options)) *engine.Engine {
	v.t.Helper()
	o := v.opts
	o.Redis = proplydtest.NewRedisClient(v.redis.Addr())
	o.Backend = v.be
	for _, f := range override {
		f(&o)
	}
	e, err := engine.New(o)
	if err != nil {
		v.t.Fatal(err)
	}
	v.t.Cleanup(func() {
		e.Abort()
		o.Redis.Close()
	})
	return e
}

func (v *env) register(e *engine.Engine, spec protocol.Spec) protocol.Spec {
	v.t.Helper()
	if spec.KeyPrefix == "" {
		spec.KeyPrefix = "/test/"
	}
	got, err := e.Register(context.Background(), ns, spec)
	if err != nil {
		v.t.Fatalf("register: %v", err)
	}
	return got
}

func (v *env) put(key, value string) int64 {
	v.t.Helper()
	rev, err := v.store.Put(context.Background(), "/test/"+key, []byte(value))
	if err != nil {
		v.t.Fatal(err)
	}
	return rev
}

func get(t *testing.T, e *engine.Engine, key string) engine.Result {
	t.Helper()
	r, err := e.Get(context.Background(), ns, key, 0)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	return r
}

func expectValue(t *testing.T, r engine.Result, want string) {
	t.Helper()
	if !r.Found || string(r.Value) != want {
		t.Fatalf("got found=%v value=%q, want %q", r.Found, r.Value, want)
	}
}

func eventually(t *testing.T, cond func() error) {
	t.Helper()
	proplydtest.Eventually(t, 5*time.Second, cond)
}

func TestReadThrough(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Read: protocol.ReadThrough})
	v.put("a", "1")

	r := get(t, e, "a")
	expectValue(t, r, "1")
	if !r.FromBackend {
		t.Fatal("first read should load from the backend")
	}
	r = get(t, e, "a")
	expectValue(t, r, "1")
	if r.FromBackend {
		t.Fatal("second read should hit the cache")
	}

	// Not found is cached too.
	if r := get(t, e, "missing"); r.Found || !r.FromBackend {
		t.Fatalf("missing: %+v", r)
	}
	if r := get(t, e, "missing"); r.Found || r.FromBackend {
		t.Fatalf("missing should be a cached negative: %+v", r)
	}
}

func TestNegativeCachingDisabled(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{NegativeTTL: -1})
	get(t, e, "missing")
	if r := get(t, e, "missing"); !r.FromBackend {
		t.Fatal("not-found must not be cached")
	}
}

// countingBackend counts Get calls.
type countingBackend struct {
	backend.Backend
	backend.Watcher
	gets atomic.Int64
}

func (c *countingBackend) Get(ctx context.Context, key string) (backend.Item, error) {
	c.gets.Add(1)
	return c.Backend.Get(ctx, key)
}

func TestConcurrentMissesShareOneLoad(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	v.be.SetLatency(100 * time.Millisecond)
	cb := &countingBackend{Backend: v.be, Watcher: v.be}
	e := v.engine(func(o *engine.Options) { o.Backend = cb })
	v.register(e, protocol.Spec{})
	v.put("hot", "x")

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			expectValue(t, get(t, e, "hot"), "x")
		}()
	}
	wg.Wait()
	if n := cb.gets.Load(); n != 1 {
		t.Fatalf("backend loads = %d, want 1", n)
	}
}

func TestCacheAside(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Read: protocol.CacheAside})
	v.put("a", "1")
	ctx := context.Background()

	if r := get(t, e, "a"); !r.Miss {
		t.Fatalf("cache-aside must report a miss: %+v", r)
	}
	item, _ := v.store.Get(ctx, "/test/a")
	if ok, err := e.Fill(ctx, ns, "a", item); err != nil || !ok {
		t.Fatalf("fill: %v %v", ok, err)
	}
	expectValue(t, get(t, e, "a"), "1")

	// Invalid fills are refused.
	if _, err := e.Fill(ctx, ns, "b", backend.Item{Found: true, Value: []byte("x"), ModRevision: 10, ReadRevision: 5}); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("fill with mod > read: %v", err)
	}
}

// A load that read before a concurrent write must not be installed after
// the write invalidated the entry: the classic cache-aside race.
func TestStaleFillRejected(t *testing.T) {
	for _, write := range []protocol.WriteStrategy{protocol.WriteThrough, protocol.WriteAround} {
		t.Run(write.String(), func(t *testing.T) {
			v := newEnv(t, proplydtest.NoPersistence)
			e := v.engine()
			v.register(e, protocol.Spec{Read: protocol.CacheAside, Write: write, Sync: protocol.SyncNone})
			ctx := context.Background()
			v.put("k", "old")
			stale, _ := v.store.Get(ctx, "/test/k") // a slow reader reads...

			if _, err := e.Set(ctx, ns, "k", []byte("new")); err != nil { // ...a writer writes...
				t.Fatal(err)
			}
			ok, err := e.Fill(ctx, ns, "k", stale) // ...the reader fills late.
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("stale fill was installed")
			}
			r := get(t, e, "k")
			if r.Miss {
				fresh, _ := v.store.Get(ctx, "/test/k")
				if _, err := e.Fill(ctx, ns, "k", fresh); err != nil {
					t.Fatal(err)
				}
				r = get(t, e, "k")
			}
			expectValue(t, r, "new")
		})
	}
}

func TestWriteThrough(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Write: protocol.WriteThrough})
	ctx := context.Background()

	rev, err := e.Set(ctx, ns, "a", []byte("1"))
	if err != nil {
		t.Fatal(err)
	}
	r := get(t, e, "a")
	expectValue(t, r, "1")
	if r.FromBackend || r.Revision != rev {
		t.Fatalf("write-through should cache the written value at rev %d: %+v", rev, r)
	}

	drev, existed, err := e.Delete(ctx, ns, "a")
	if err != nil || !existed {
		t.Fatalf("delete: %v %v", existed, err)
	}
	r = get(t, e, "a")
	if r.Found || r.FromBackend || r.Revision != drev {
		t.Fatalf("delete should cache a negative entry at %d: %+v", drev, r)
	}
}

func TestWriteAround(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Write: protocol.WriteAround})
	ctx := context.Background()
	v.put("a", "0")
	get(t, e, "a")

	if _, err := e.Set(ctx, ns, "a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	r := get(t, e, "a")
	expectValue(t, r, "1")
	if !r.FromBackend {
		t.Fatal("write-around should invalidate, not cache")
	}
	if got, _ := v.store.Get(ctx, "/test/a"); string(got.Value) != "1" {
		t.Fatalf("backend has %q", got.Value)
	}
}

func TestRefreshAhead(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Read: protocol.RefreshAhead, Sync: protocol.SyncNone, TTL: 2 * time.Second, RefreshAfter: 200 * time.Millisecond})
	v.put("a", "1")
	get(t, e, "a")
	v.put("a", "2") // behind the cache's back; SyncNone does not see it

	time.Sleep(250 * time.Millisecond)
	// This read is served from the cache and triggers a refresh.
	if r := get(t, e, "a"); r.FromBackend {
		t.Fatal("refresh-ahead must serve the cached value")
	}
	eventually(t, func() error {
		if r := get(t, e, "a"); string(r.Value) != "2" || r.FromBackend {
			return fmt.Errorf("got %+v", r)
		}
		return nil
	})
}

func TestMinRevision(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Sync: protocol.SyncNone})
	v.put("a", "1")
	get(t, e, "a")
	rev := v.put("a", "2") // e.g. a client's fallback write

	r, err := e.Get(context.Background(), ns, "a", rev)
	if err != nil {
		t.Fatal(err)
	}
	expectValue(t, r, "2")
}

func TestWatchAppliesWritesThatBypassTheCache(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Read: protocol.CacheAside, Write: protocol.WriteAround})
	ctx := context.Background()

	v.put("cached", "1")
	item, _ := v.store.Get(ctx, "/test/cached")
	if _, err := e.Fill(ctx, ns, "cached", item); err != nil {
		t.Fatal(err)
	}
	// Direct writes, like a client falling back while the cache is
	// unreachable.
	v.put("cached", "2")
	eventually(t, func() error {
		if r := get(t, e, "cached"); r.Miss || string(r.Value) != "2" {
			return fmt.Errorf("cached entry not updated in place: %+v", r)
		}
		return nil
	})

	// An uncached key gets a fence once the watcher sees the change, so a
	// load from before the write is refused. (A load installed before the
	// watcher gets there is overwritten by the newer change instead.)
	v.put("other", "1")
	stale, _ := v.store.Get(ctx, "/test/other")
	v.put("other", "2")
	rc := v.redis.Client()
	defer rc.Close()
	eventually(t, func() error {
		b, err := rc.Get(ctx, "pd:{test}:k:other").Bytes()
		if err != nil || len(b) == 0 || b[0] != 3 {
			return fmt.Errorf("no fence yet (%v)", err)
		}
		return nil
	})
	if ok, err := e.Fill(ctx, ns, "other", stale); err != nil || ok {
		t.Fatalf("stale fill over a watched change: stored=%v err=%v", ok, err)
	}

	// Deletes too.
	if _, _, err := v.store.Delete(ctx, "/test/cached"); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		if r := get(t, e, "cached"); r.Miss || r.Found {
			return fmt.Errorf("delete not applied: %+v", r)
		}
		return nil
	})
}

// After a restart, the engine must not serve entries that missed changes
// made while it was down; it waits until the watch has caught up.
func TestCatchUpAfterRestart(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e1 := v.engine()
	v.register(e1, protocol.Spec{Write: protocol.WriteAround})
	v.put("a", "1")
	expectValue(t, get(t, e1, "a"), "1")
	e1.Abort()

	v.put("a", "2") // nobody is watching

	e2 := v.engine()
	expectValue(t, get(t, e2, "a"), "2")
}

func TestCompactedHistoryPurgesNamespace(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e1 := v.engine()
	v.register(e1, protocol.Spec{})
	v.put("a", "1")
	expectValue(t, get(t, e1, "a"), "1")
	e1.Abort()

	v.put("a", "2")
	v.put("b", "x")
	v.store.Compact(v.put("c", "y")) // the changes to a are gone from history

	e2 := v.engine()
	expectValue(t, get(t, e2, "a"), "2")
	if n := e2.Stats().Purges.Load(); n != 1 {
		t.Fatalf("purges = %d, want 1", n)
	}
}

func TestRedisRestartWithoutPersistence(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	spec := protocol.Spec{}
	v.register(e, spec)
	v.put("a", "1")
	get(t, e, "a")

	v.redis.Restart()
	eventually(t, func() error {
		_, err := e.Get(context.Background(), ns, "a", 0)
		if !errors.Is(err, engine.ErrNamespaceNotFound) {
			return fmt.Errorf("want namespace not found, got %v", err)
		}
		return nil
	})
	v.register(e, spec)
	expectValue(t, get(t, e, "a"), "1")
}

// A Redis that comes back with an old snapshot holds stale entries and old
// watch progress. The engine notices the restart and replays the watch.
func TestRedisRollback(t *testing.T) {
	v := newEnv(t, proplydtest.RDB)
	e := v.engine()
	v.register(e, protocol.Spec{Write: protocol.WriteThrough})
	ctx := context.Background()
	if _, err := e.Set(ctx, ns, "a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	expectValue(t, get(t, e, "a"), "1")
	v.redis.Save()
	if _, err := e.Set(ctx, ns, "a", []byte("2")); err != nil {
		t.Fatal(err)
	}

	v.redis.Restart() // back to the snapshot, where a=1
	eventually(t, func() error {
		if e.Stats().RedisRestarts.Load() == 0 {
			return errors.New("restart not detected")
		}
		return nil
	})
	eventually(t, func() error {
		r, err := e.Get(ctx, ns, "a", 0)
		if err != nil {
			return err
		}
		if string(r.Value) != "2" {
			t.Fatalf("served rolled-back value %q", r.Value)
		}
		return nil
	})
}

func TestRegisterValidation(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine(func(o *engine.Options) { o.AllowedPrefixes = []string{"/test/"}; o.MaxValueSize = 16 })
	ctx := context.Background()
	v.register(e, protocol.Spec{TTL: time.Minute})

	if _, err := e.Register(ctx, ns, protocol.Spec{KeyPrefix: "/test/", TTL: time.Hour}); !errors.Is(err, engine.ErrNamespaceConflict) {
		t.Fatalf("conflicting spec: %v", err)
	}
	if _, err := e.Register(ctx, ns, protocol.Spec{KeyPrefix: "/test/", TTL: time.Minute}); err != nil {
		t.Fatalf("same spec must be accepted again: %v", err)
	}
	if _, err := e.Register(ctx, "other", protocol.Spec{KeyPrefix: "/secret/"}); !errors.Is(err, engine.ErrPrefixNotAllowed) {
		t.Fatalf("disallowed prefix: %v", err)
	}
	if _, err := e.Register(ctx, "Bad{Name}", protocol.Spec{KeyPrefix: "/test/"}); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("bad name: %v", err)
	}
	if _, err := e.Get(ctx, "unknown", "a", 0); !errors.Is(err, engine.ErrNamespaceNotFound) {
		t.Fatalf("unknown namespace: %v", err)
	}
	if _, err := e.Get(ctx, ns, "", 0); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("empty key: %v", err)
	}
	if _, err := e.Set(ctx, ns, "a", make([]byte, 17)); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("oversized value: %v", err)
	}
}

func TestBackendDown(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{})
	v.put("a", "1")
	get(t, e, "a")

	v.be.SetDown(true)
	// Cached data is still served; misses and writes report the backend.
	expectValue(t, get(t, e, "a"), "1")
	if _, err := e.Get(context.Background(), ns, "b", 0); !errors.Is(err, backend.ErrUnavailable) {
		t.Fatalf("miss with backend down: %v", err)
	}
	if _, err := e.Set(context.Background(), ns, "a", []byte("2")); !errors.Is(err, backend.ErrUnavailable) {
		t.Fatalf("write with backend down: %v", err)
	}
}

func TestRedisDown(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{})
	v.redis.Kill()
	eventually(t, func() error {
		start := time.Now()
		_, err := e.Get(context.Background(), ns, "a", 0)
		if !errors.Is(err, engine.ErrCacheUnavailable) {
			return fmt.Errorf("want cache unavailable, got %v", err)
		}
		if d := time.Since(start); d > 50*time.Millisecond {
			return fmt.Errorf("failing took %v; should fail fast", d)
		}
		return nil
	})
	// Writes are refused before touching the backend.
	if _, err := e.Set(context.Background(), ns, "a", []byte("1")); !errors.Is(err, engine.ErrCacheUnavailable) {
		t.Fatalf("set: %v", err)
	}
	if it, _ := v.store.Get(context.Background(), "/test/a"); it.Found {
		t.Fatal("write reached the backend while the cache was down")
	}
}

func TestLeaseHandover(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e1 := v.engine(func(o *engine.Options) { o.InstanceID = "a" })
	e2 := v.engine(func(o *engine.Options) { o.InstanceID = "b" })
	v.register(e1, protocol.Spec{})
	v.register(e2, protocol.Spec{})
	v.put("k", "1")
	expectValue(t, get(t, e2, "k"), "1")

	holder, err := v.redis.Client().Get(context.Background(), "pd:{test}:w").Result()
	if err != nil {
		t.Fatal(err)
	}
	if holder == "a" {
		e1.Close()
	} else {
		e2.Close()
		e2 = e1
	}
	v.put("k", "2")
	eventually(t, func() error {
		if r := get(t, e2, "k"); string(r.Value) != "2" {
			return fmt.Errorf("survivor did not take over the watch: %+v", r)
		}
		return nil
	})
}

func TestInvalidate(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Sync: protocol.SyncNone})
	v.put("a", "1")
	v.put("b", "1")
	get(t, e, "a")
	get(t, e, "b")
	v.put("a", "2")
	v.put("b", "2")

	if err := e.Invalidate(context.Background(), ns, []string{"a"}, false); err != nil {
		t.Fatal(err)
	}
	expectValue(t, get(t, e, "a"), "2")
	expectValue(t, get(t, e, "b"), "1")
	if err := e.Invalidate(context.Background(), ns, nil, true); err != nil {
		t.Fatal(err)
	}
	expectValue(t, get(t, e, "b"), "2")
}

// subscription collects what a subscriber receives.
type subscription struct {
	mu     sync.Mutex
	events []*api.SubscribeResponse
	done   chan error
	cancel context.CancelFunc
}

func subscribe(e *engine.Engine, epoch, pos string) *subscription {
	ctx, cancel := context.WithCancel(context.Background())
	s := &subscription{done: make(chan error, 1), cancel: cancel}
	go func() {
		s.done <- e.Subscribe(ctx, ns, epoch, pos, func(r *api.SubscribeResponse) error {
			s.mu.Lock()
			s.events = append(s.events, r)
			s.mu.Unlock()
			return nil
		})
	}()
	return s
}

func (s *subscription) snapshot() []*api.SubscribeResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*api.SubscribeResponse(nil), s.events...)
}

func (s *subscription) keys() []string {
	var out []string
	for _, ev := range s.snapshot() {
		for _, inv := range ev.GetInvalidations() {
			out = append(out, inv.GetKey())
		}
	}
	return out
}

func (s *subscription) last() *api.SubscribeResponse {
	ev := s.snapshot()
	return ev[len(ev)-1]
}

func TestSubscribe(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Write: protocol.WriteAround})
	ctx := context.Background()

	s := subscribe(e, "", "")
	eventually(t, func() error {
		if len(s.snapshot()) == 0 {
			return errors.New("no first event")
		}
		return nil
	})
	if first := s.snapshot()[0]; first.GetKind() != api.SubscribeResponse_KIND_RESET {
		t.Fatalf("fresh subscription must start with a reset, got %v", first.GetKind())
	}
	for _, k := range []string{"a", "b", "c"} {
		if _, err := e.Set(ctx, ns, k, []byte("1")); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, func() error {
		if got := fmt.Sprint(s.keys()); got != "[a b c]" {
			return fmt.Errorf("invalidations %s", got)
		}
		return nil
	})
	// Heartbeats keep coming while idle.
	n := len(s.snapshot())
	eventually(t, func() error {
		if s.last().GetKind() != api.SubscribeResponse_KIND_HEARTBEAT || len(s.snapshot()) == n {
			return errors.New("no heartbeat")
		}
		return nil
	})
	epoch, pos := s.last().GetEpoch(), s.last().GetPosition()
	s.cancel()

	// Changes while disconnected are replayed on resume.
	for _, k := range []string{"d", "e"} {
		if _, err := e.Set(ctx, ns, k, []byte("1")); err != nil {
			t.Fatal(err)
		}
	}
	s2 := subscribe(e, epoch, pos)
	eventually(t, func() error {
		if got := fmt.Sprint(s2.keys()); got != "[d e]" {
			return fmt.Errorf("resumed invalidations %s", got)
		}
		return nil
	})
	if s2.snapshot()[0].GetKind() == api.SubscribeResponse_KIND_RESET {
		t.Fatal("resume must not reset")
	}

	// A purge resets subscribers with the new epoch.
	if err := e.Invalidate(ctx, ns, nil, true); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		for _, ev := range s2.snapshot() {
			if ev.GetKind() == api.SubscribeResponse_KIND_RESET && ev.GetEpoch() != epoch {
				return nil
			}
		}
		return errors.New("no reset after purge")
	})

	// An unknown epoch cannot resume.
	s3 := subscribe(e, epoch, pos)
	eventually(t, func() error {
		if ev := s3.snapshot(); len(ev) == 0 || ev[0].GetKind() != api.SubscribeResponse_KIND_RESET {
			return errors.New("stale epoch must reset")
		}
		return nil
	})
}

func TestSubscribeTrimmedHistoryResets(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	v.opts.InvalidationLogSize = 10
	e := v.engine()
	v.register(e, protocol.Spec{Write: protocol.WriteAround})
	ctx := context.Background()
	if _, err := e.Set(ctx, ns, "first", []byte("1")); err != nil {
		t.Fatal(err)
	}
	s := subscribe(e, "", "")
	eventually(t, func() error {
		if len(s.snapshot()) == 0 {
			return errors.New("not subscribed")
		}
		return nil
	})
	epoch, pos := s.last().GetEpoch(), s.last().GetPosition()
	s.cancel()

	// Approximate trimming works in whole stream nodes; write enough.
	for i := range 500 {
		if _, err := e.Set(ctx, ns, fmt.Sprint("k", i), []byte("1")); err != nil {
			t.Fatal(err)
		}
	}
	s2 := subscribe(e, epoch, pos)
	eventually(t, func() error {
		if ev := s2.snapshot(); len(ev) == 0 || ev[0].GetKind() != api.SubscribeResponse_KIND_RESET {
			return errors.New("trimmed history must reset")
		}
		return nil
	})
}

func TestSubscribersEndWhenRedisDies(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{})
	s := subscribe(e, "", "")
	eventually(t, func() error {
		if len(s.snapshot()) == 0 {
			return errors.New("not subscribed")
		}
		return nil
	})
	v.redis.Kill()
	select {
	case err := <-s.done:
		if !errors.Is(err, engine.ErrCacheUnavailable) {
			t.Fatalf("subscription ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscription outlived Redis")
	}
}

// invalidated waits until s has received an invalidation of key at rev or
// later.
func (s *subscription) invalidated(t *testing.T, key string, rev int64) {
	t.Helper()
	eventually(t, func() error {
		for _, ev := range s.snapshot() {
			for _, inv := range ev.GetInvalidations() {
				if inv.GetKey() == key && inv.GetRevision() >= rev {
					return nil
				}
			}
		}
		return fmt.Errorf("no invalidation of %s at %d or later", key, rev)
	})
}

// A load can see a write that bypassed the cache before the watcher does.
// When the watcher's event then finds Redis already up to date, it must
// still publish the change, or near-caches holding the old value never
// hear about it.
func TestBypassingWriteSeenByLoadFirstIsStillPublished(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{})
	v.put("k", "1")
	expectValue(t, get(t, e, "k"), "1")
	s := subscribe(e, "", "")
	eventually(t, func() error {
		if len(s.snapshot()) == 0 {
			return errors.New("not subscribed")
		}
		return nil
	})

	v.be.PauseWatches(true)
	rev := v.put("k", "2") // e.g. a client's write-around fallback
	// The entry is evicted (memory pressure, TTL) and reloaded before the
	// watcher gets to the change.
	rc := v.redis.Client()
	defer rc.Close()
	if err := rc.Del(context.Background(), "pd:{test}:k:k").Err(); err != nil {
		t.Fatal(err)
	}
	expectValue(t, get(t, e, "k"), "2")
	v.be.PauseWatches(false)

	s.invalidated(t, "k", rev)
}

// Without a watcher, a load that replaces an older cached state publishes
// the change itself.
func TestLoadReplacingOlderStatePublishes(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Sync: protocol.SyncNone})
	v.put("k", "1")
	get(t, e, "k")
	s := subscribe(e, "", "")
	eventually(t, func() error {
		if len(s.snapshot()) == 0 {
			return errors.New("not subscribed")
		}
		return nil
	})
	rev := v.put("k", "2")
	r, err := e.Get(context.Background(), ns, "k", rev)
	if err != nil {
		t.Fatal(err)
	}
	expectValue(t, r, "2")
	s.invalidated(t, "k", rev)
}

// swappable is a backend whose store can be replaced at runtime, like an
// etcd cluster that is wiped or restored from a backup. Swapping breaks the
// current watches, as a real restore drops connections.
type swappable struct {
	mu       sync.Mutex
	cur      *memory.Store
	identity bool // expose the store identity
	broken   chan struct{}
}

func newSwappable(s *memory.Store, identity bool) *swappable {
	return &swappable{cur: s, identity: identity, broken: make(chan struct{})}
}

func (w *swappable) store() *memory.Store {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cur
}

func (w *swappable) swap(s *memory.Store) {
	w.mu.Lock()
	w.cur = s
	close(w.broken)
	w.broken = make(chan struct{})
	w.mu.Unlock()
}

func (w *swappable) Get(ctx context.Context, k string) (backend.Item, error) {
	return w.store().Get(ctx, k)
}
func (w *swappable) Put(ctx context.Context, k string, v []byte) (int64, error) {
	return w.store().Put(ctx, k, v)
}
func (w *swappable) Delete(ctx context.Context, k string) (int64, bool, error) {
	return w.store().Delete(ctx, k)
}
func (w *swappable) Revision(ctx context.Context) (int64, error) { return w.store().Revision(ctx) }

func (w *swappable) Watch(ctx context.Context, prefix string, from int64) <-chan backend.WatchBatch {
	w.mu.Lock()
	s, broken := w.cur, w.broken
	w.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	in := s.Watch(ctx, prefix, from)
	out := make(chan backend.WatchBatch)
	go func() {
		defer close(out)
		defer cancel()
		for {
			select {
			case b, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- b:
				case <-ctx.Done():
					return
				}
			case <-broken:
				select {
				case out <- backend.WatchBatch{Err: backend.Unavailable(errors.New("store replaced"))}:
				case <-ctx.Done():
				}
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// identified adds the store identity when enabled.
type identified struct{ *swappable }

func (w identified) Identity(ctx context.Context) (string, error) { return w.store().Identity(ctx) }

func storeWith(t *testing.T, writes int, final string) *memory.Store {
	t.Helper()
	s := memory.New(memory.Options{ProgressInterval: 50 * time.Millisecond})
	for i := range writes {
		if _, err := s.Put(context.Background(), "/test/k", []byte(fmt.Sprint("old-", i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Put(context.Background(), "/test/k", []byte(final)); err != nil {
		t.Fatal(err)
	}
	return s
}

// The store is replaced while proplyd is down: on startup it must not
// serve entries from the old store's timeline.
func TestStoreResetDetectedAtStartup(t *testing.T) {
	for _, withIdentity := range []bool{true, false} {
		t.Run(fmt.Sprint("identity=", withIdentity), func(t *testing.T) {
			v := newEnv(t, proplydtest.NoPersistence)
			sw := newSwappable(storeWith(t, 50, "before"), withIdentity)
			var be backend.Backend = sw
			if withIdentity {
				be = identified{sw}
			}
			e1 := v.engine(func(o *engine.Options) { o.Backend = be })
			v.register(e1, protocol.Spec{})
			expectValue(t, get(t, e1, "k"), "before")
			e1.Abort()

			// Restored from an old backup: far fewer revisions.
			sw.swap(storeWith(t, 0, "restored"))
			e2 := v.engine(func(o *engine.Options) { o.Backend = be })
			expectValue(t, get(t, e2, "k"), "restored")
			if n := e2.Stats().StoreResets.Load(); n != 1 {
				t.Fatalf("store resets = %d, want 1", n)
			}
			// New writes on the restored store are cached normally.
			if _, err := e2.Set(context.Background(), ns, "k", []byte("after")); err != nil {
				t.Fatal(err)
			}
			expectValue(t, get(t, e2, "k"), "after")
		})
	}
}

// The store is replaced under a running proplyd, and by the time anyone
// looks it has already passed the old revision: only the identity can
// tell.
func TestStoreResetDetectedAtRuntime(t *testing.T) {
	v := newEnv(t, proplydtest.NoPersistence)
	sw := newSwappable(storeWith(t, 5, "before"), true)
	e := v.engine(func(o *engine.Options) { o.Backend = identified{sw} })
	v.register(e, protocol.Spec{})
	expectValue(t, get(t, e, "k"), "before")
	s := subscribe(e, "", "")

	sw.swap(storeWith(t, 50, "replaced")) // higher revisions than before
	eventually(t, func() error {
		if r := get(t, e, "k"); string(r.Value) != "replaced" {
			return fmt.Errorf("still serving %q from the old store", r.Value)
		}
		return nil
	})
	eventually(t, func() error {
		for _, ev := range s.snapshot()[1:] {
			if ev.GetKind() == api.SubscribeResponse_KIND_RESET {
				return nil
			}
		}
		return errors.New("near-caches were not told to reset")
	})
}

func wbEnv(t *testing.T, p proplydtest.Persistence) *env {
	t.Helper()
	v := newEnv(t, p)
	v.opts.FlushInterval = 200 * time.Millisecond
	return v
}

func storeValue(t *testing.T, v *env, key string) (string, bool) {
	t.Helper()
	it, err := v.store.Get(context.Background(), "/test/"+key)
	if err != nil {
		t.Fatal(err)
	}
	return string(it.Value), it.Found
}

func storeHas(t *testing.T, v *env, key, want string) {
	t.Helper()
	eventually(t, func() error {
		got, found := storeValue(t, v, key)
		if (want == "") == found || got != want {
			return fmt.Errorf("store has %q (found=%v), want %q", got, found, want)
		}
		return nil
	})
}

func TestWriteBehindAcknowledgesBeforeTheStore(t *testing.T) {
	v := wbEnv(t, proplydtest.NoPersistence)
	v.opts.FlushInterval = time.Second
	e := v.engine()
	v.register(e, protocol.Spec{Write: protocol.WriteBehind})
	s := subscribe(e, "", "")
	eventually(t, func() error {
		if len(s.snapshot()) == 0 {
			return errors.New("not subscribed")
		}
		return nil
	})
	ctx := context.Background()

	rev, err := e.Set(ctx, ns, "k", []byte("v"))
	if err != nil || rev != 0 {
		t.Fatalf("set: rev=%d err=%v", rev, err)
	}
	if _, found := storeValue(t, v, "k"); found {
		t.Fatal("write-behind wrote the store synchronously")
	}
	r := get(t, e, "k")
	expectValue(t, r, "v")
	if !r.Pending {
		t.Fatal("unflushed write must read as pending")
	}
	s.invalidated(t, "k", 0) // near-caches drop their copy at once

	storeHas(t, v, "k", "v")
	eventually(t, func() error {
		if r := get(t, e, "k"); r.Pending {
			return errors.New("still pending after the flush")
		}
		return nil
	})
	it, _ := v.store.Get(ctx, "/test/k")
	s.invalidated(t, "k", it.ModRevision)
	if r := get(t, e, "k"); r.Revision != it.ModRevision {
		t.Fatalf("cached revision %d, store %d", r.Revision, it.ModRevision)
	}
}

func TestWriteBehindCoalesces(t *testing.T) {
	v := wbEnv(t, proplydtest.NoPersistence)
	v.opts.FlushInterval = time.Second
	e := v.engine()
	v.register(e, protocol.Spec{Write: protocol.WriteBehind})
	before, _ := v.store.Revision(context.Background())
	for i := range 20 {
		if _, err := e.Set(context.Background(), ns, "k", []byte(fmt.Sprint("v", i))); err != nil {
			t.Fatal(err)
		}
	}
	storeHas(t, v, "k", "v19")
	after, _ := v.store.Revision(context.Background())
	if writes := after - before; writes > 2 {
		t.Fatalf("20 writes to one key made %d store writes", writes)
	}
}

func TestWriteBehindAcceptsWritesWhileTheStoreIsDown(t *testing.T) {
	v := wbEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Write: protocol.WriteBehind})
	v.put("cached", "0")
	get(t, e, "cached")

	v.be.SetDown(true)
	for _, k := range []string{"cached", "uncached"} {
		if _, err := e.Set(context.Background(), ns, k, []byte("new")); err != nil {
			t.Fatalf("%s: %v", k, err)
		}
		expectValue(t, get(t, e, k), "new")
	}
	time.Sleep(500 * time.Millisecond) // flushes fail meanwhile
	v.be.SetDown(false)
	storeHas(t, v, "cached", "new")
	storeHas(t, v, "uncached", "new")
	if n := e.Stats().UnknownBases.Load(); n != 1 {
		t.Fatalf("unknown bases = %d, want 1 (the uncached key)", n)
	}
}

// Someone writes the store behind a pending write: the store wins.
func TestWriteBehindConflictStoreWins(t *testing.T) {
	v := wbEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Write: protocol.WriteBehind})
	v.put("k", "v1")
	get(t, e, "k")

	v.be.SetDown(true) // hold the flush
	if _, err := e.Set(context.Background(), ns, "k", []byte("mine")); err != nil {
		t.Fatal(err)
	}
	v.put("k", "theirs") // another writer, straight to the store
	v.be.SetDown(false)

	eventually(t, func() error {
		if e.Stats().FlushConflicts.Load() != 1 {
			return errors.New("no conflict yet")
		}
		return nil
	})
	if got, _ := storeValue(t, v, "k"); got != "theirs" {
		t.Fatalf("store has %q", got)
	}
	eventually(t, func() error {
		if r := get(t, e, "k"); string(r.Value) != "theirs" || r.Pending {
			return fmt.Errorf("cache serves %+v", r)
		}
		return nil
	})
}

func TestWriteBehindSurvivesProplydCrash(t *testing.T) {
	v := wbEnv(t, proplydtest.NoPersistence)
	e1 := v.engine()
	v.register(e1, protocol.Spec{Write: protocol.WriteBehind})
	get(t, e1, "warm") // in sync with the store before it goes down
	v.be.SetDown(true)
	if _, err := e1.Set(context.Background(), ns, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	e1.Abort()
	v.be.SetDown(false)

	// No request touches the namespace: the restarted engine finds the
	// pending write by itself.
	v.engine()
	storeHas(t, v, "k", "v")
}

// Redis comes back from a snapshot that still holds an old pending write.
// Its base no longer matches the store, so it must not be replayed.
func TestWriteBehindRolledBackPendingIsNotReplayed(t *testing.T) {
	v := wbEnv(t, proplydtest.RDB)
	e := v.engine()
	v.register(e, protocol.Spec{Write: protocol.WriteBehind})
	v.put("k", "v0")
	get(t, e, "k")
	ctx := context.Background()

	v.be.SetDown(true)
	if _, err := e.Set(ctx, ns, "k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	v.redis.Save() // the snapshot holds v1 as pending
	v.be.SetDown(false)
	storeHas(t, v, "k", "v1")
	if _, err := e.Set(ctx, ns, "k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	storeHas(t, v, "k", "v2")

	v.redis.Restart() // back to the snapshot: v1 pending again
	eventually(t, func() error {
		if e.Stats().FlushConflicts.Load() == 0 {
			return errors.New("stale pending write not processed yet")
		}
		return nil
	})
	if got, _ := storeValue(t, v, "k"); got != "v2" {
		t.Fatalf("rolled-back pending write was replayed: store has %q", got)
	}
}

// The store write of a flush succeeds but its commit to Redis is lost.
// The next attempt must recognise its own write, not call it a conflict
// and drop the newer write that coalesced meanwhile.
func TestWriteBehindLostCommitIsNotAConflict(t *testing.T) {
	v := wbEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Write: protocol.WriteBehind})
	v.put("k", "v0")
	get(t, e, "k")
	ctx := context.Background()
	rc := v.redis.Client()
	defer rc.Close()

	v.be.SetDown(true)
	if _, err := e.Set(ctx, ns, "k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	// Simulate a flush of v1 that reached the store but whose commit was
	// lost: the store has v1 and the intent is still recorded.
	rec, err := rc.HGet(ctx, "pd:{test}:p", "k").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.HSet(ctx, "pd:{test}:n", "k", rec).Err(); err != nil {
		t.Fatal(err)
	}
	v.put("k", "v1")
	if _, err := e.Set(ctx, ns, "k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	v.be.SetDown(false)

	storeHas(t, v, "k", "v2")
	if n := e.Stats().FlushConflicts.Load(); n != 0 {
		t.Fatalf("own lost commit counted as %d conflicts", n)
	}
}

func TestWriteBehindBackpressure(t *testing.T) {
	v := wbEnv(t, proplydtest.NoPersistence)
	e := v.engine(func(o *engine.Options) { o.MaxPending = 3 })
	v.register(e, protocol.Spec{Write: protocol.WriteBehind})
	get(t, e, "warm") // a namespace serves only once it is in sync with the store
	v.be.SetDown(true)
	ctx := context.Background()
	for i := range 3 {
		if _, err := e.Set(ctx, ns, fmt.Sprint("k", i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.Set(ctx, ns, "k3", []byte("v")); !errors.Is(err, engine.ErrBackpressure) {
		t.Fatalf("4th pending key: %v", err)
	}
	if _, err := e.Set(ctx, ns, "k0", []byte("again")); err != nil {
		t.Fatalf("rewriting a pending key must not count: %v", err)
	}
	v.be.SetDown(false)
	storeHas(t, v, "k0", "again")
}

func TestWriteBehindDelete(t *testing.T) {
	v := wbEnv(t, proplydtest.NoPersistence)
	e := v.engine()
	v.register(e, protocol.Spec{Write: protocol.WriteBehind})
	v.put("k", "v")
	get(t, e, "k")
	if _, _, err := e.Delete(context.Background(), ns, "k"); err != nil {
		t.Fatal(err)
	}
	if r := get(t, e, "k"); r.Found || !r.Pending {
		t.Fatalf("pending delete: %+v", r)
	}
	storeHas(t, v, "k", "")
}
