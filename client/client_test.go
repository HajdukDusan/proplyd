package client_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c12s/proplyd/backend/memory"
	"github.com/c12s/proplyd/client"
	"github.com/c12s/proplyd/internal/testutil"
	"github.com/c12s/proplyd/protocol"
	"google.golang.org/grpc"
)

const prefix = "/svc/"

type env struct {
	db     *memory.Store
	faults *testutil.Faults
	node   *testutil.CacheNode
}

func newEnv(t *testing.T, opts ...testutil.NodeOption) *env {
	t.Helper()
	db := memory.NewStore()
	faults := &testutil.Faults{}
	node := testutil.StartCacheNode(t, faults.Wrap(db.Factory()), opts...)
	return &env{db: db, faults: faults, node: node}
}

func (e *env) client(t *testing.T, spec protocol.Namespace, mutate ...func(*client.Config)) *client.Client {
	t.Helper()
	spec.KeyPrefix = prefix
	return testutil.NewClient(t, e.node.Addr(), "svc", spec, e.db.Backend(prefix), mutate...)
}

// clientVia connects through an extra proxy so a single client can be
// partitioned from the cache while others are not.
func (e *env) clientVia(t *testing.T, spec protocol.Namespace) (*client.Client, *testutil.Proxy) {
	t.Helper()
	p, err := testutil.NewProxy(e.node.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	spec.KeyPrefix = prefix
	return testutil.NewClient(t, p.Addr(), "svc", spec, e.db.Backend(prefix)), p
}

func (e *env) stored(key string) (string, bool) {
	v, ok := e.db.Peek(prefix + key)
	return string(v), ok
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

func lookup(t *testing.T, c *client.Client, key string) client.Item {
	t.Helper()
	it, err := c.Lookup(ctx(t), key)
	if err != nil {
		t.Fatalf("lookup %q: %v", key, err)
	}
	return it
}

// ---------------------------------------------------------------- happy paths

func TestStrategies(t *testing.T) {
	for _, w := range []protocol.WriteStrategy{protocol.WriteThrough, protocol.WriteAround, protocol.WriteBehind} {
		for _, r := range []protocol.ReadStrategy{protocol.ReadThrough, protocol.CacheAside, protocol.RefreshAhead} {
			t.Run(fmt.Sprintf("write=%d/read=%d", w, r), func(t *testing.T) {
				e := newEnv(t)
				spec := protocol.Namespace{Read: r, Write: w, TTL: time.Minute, FlushInterval: 10 * time.Millisecond}
				c := e.client(t, spec)

				if err := c.Set(ctx(t), "k", []byte("v1")); err != nil {
					t.Fatal(err)
				}
				testutil.Eventually(t, 2*time.Second, func() bool { v, _ := e.stored("k"); return v == "v1" }, "persisted")

				v, found, err := c.Get(ctx(t), "k")
				if err != nil || !found || string(v) != "v1" {
					t.Fatalf("get: %q %v %v", v, found, err)
				}
				// Second read is always answered by the cache.
				if it := lookup(t, c, "k"); it.Source != client.SourceCache || string(it.Value) != "v1" {
					t.Fatalf("second read: %+v", it)
				}

				if err := c.Delete(ctx(t), "k"); err != nil {
					t.Fatal(err)
				}
				testutil.Eventually(t, 2*time.Second, func() bool { _, ok := e.stored("k"); return !ok }, "deleted")
				if _, found, _ := c.Get(ctx(t), "k"); found {
					t.Fatal("deleted key found")
				}
				if c.State() != client.StateClosed {
					t.Fatalf("breaker %v", c.State())
				}
			})
		}
	}
}

func TestCacheAsideLoadsThenServesFromCache(t *testing.T) {
	e := newEnv(t)
	c := e.client(t, protocol.Namespace{Read: protocol.CacheAside})
	e.db.Backend(prefix).Put(ctx(t), "k", []byte("v"))

	if it := lookup(t, c, "k"); it.Source != client.SourceStore || string(it.Value) != "v" {
		t.Fatalf("first: %+v", it)
	}
	if it := lookup(t, c, "k"); it.Source != client.SourceCache || string(it.Value) != "v" {
		t.Fatalf("second: %+v", it)
	}
	if m := c.Metrics(); m.StoreReads != 1 || m.CacheReads != 1 {
		t.Fatalf("metrics: %+v", m)
	}
}

func TestSharedNamespaceAcrossClients(t *testing.T) {
	e := newEnv(t)
	a := e.client(t, protocol.Namespace{})
	b := e.client(t, protocol.Namespace{})
	a.Set(ctx(t), "k", []byte("from-a"))
	if it := lookup(t, b, "k"); it.Source != client.SourceCache || string(it.Value) != "from-a" {
		t.Fatalf("b: %+v", it)
	}
}

func TestNamespaceConflictIsReportedByNew(t *testing.T) {
	e := newEnv(t)
	e.client(t, protocol.Namespace{Write: protocol.WriteThrough})
	_, err := client.New(ctx(t), client.Config{
		Target:    e.node.Addr(),
		Namespace: "svc",
		Spec:      protocol.Namespace{Write: protocol.WriteBehind, KeyPrefix: prefix},
		Store:     e.db.Backend(prefix),
		Logger:    testutil.QuietLogger(),
	})
	if err == nil {
		t.Fatal("expected conflict error")
	}
}

func TestInvalidConfig(t *testing.T) {
	_, err := client.New(ctx(t), client.Config{Target: "localhost:1", Namespace: "x", Spec: protocol.Namespace{}})
	if err == nil {
		t.Fatal("expected validation error (no prefix, no store)")
	}
}

func TestInvalidateAndPurge(t *testing.T) {
	e := newEnv(t)
	c := e.client(t, protocol.Namespace{})
	c.Set(ctx(t), "a", []byte("1"))
	c.Set(ctx(t), "b", []byte("2"))
	// Change the store behind the cache's back (no watch in this namespace).
	e.db.Backend(prefix).Put(ctx(t), "a", []byte("1*"))
	e.db.Backend(prefix).Put(ctx(t), "b", []byte("2*"))

	if err := c.Invalidate(ctx(t), "a"); err != nil {
		t.Fatal(err)
	}
	if v, _, _ := c.Get(ctx(t), "a"); string(v) != "1*" {
		t.Fatalf("a=%q", v)
	}
	if v, _, _ := c.Get(ctx(t), "b"); string(v) != "2" {
		t.Fatalf("b should still be cached, got %q", v)
	}
	if err := c.Purge(ctx(t)); err != nil {
		t.Fatal(err)
	}
	if v, _, _ := c.Get(ctx(t), "b"); string(v) != "2*" {
		t.Fatalf("b=%q after purge", v)
	}
	st, err := c.Stats(ctx(t))
	if err != nil || st.Hits == 0 {
		t.Fatalf("stats: %v %v", st, err)
	}
}

// ---------------------------------------------------------------- cache down

func TestCacheNeverStarted(t *testing.T) {
	e := newEnv(t)
	e.node.Kill()
	e.db.Backend(prefix).Put(ctx(t), "k", []byte("v"))

	for _, w := range []protocol.WriteStrategy{protocol.WriteThrough, protocol.WriteAround, protocol.WriteBehind} {
		t.Run(fmt.Sprint(w), func(t *testing.T) {
			c := e.client(t, protocol.Namespace{Write: w})
			if it := lookup(t, c, "k"); it.Source != client.SourceStore || string(it.Value) != "v" {
				t.Fatalf("read: %+v", it)
			}
			err := c.Set(ctx(t), "k2", []byte(fmt.Sprint(w)))
			v, stored := e.stored("k2")
			switch w {
			case protocol.WriteAround:
				if err != nil || v != fmt.Sprint(w) {
					t.Fatalf("write-around must go to the store: err=%v stored=%q", err, v)
				}
				e.db.Backend(prefix).Delete(ctx(t), "k2")
			default:
				if !errors.Is(err, client.ErrCacheUnavailable) {
					t.Fatalf("want ErrCacheUnavailable, got %v", err)
				}
				if stored {
					t.Fatal("rejected write reached the store")
				}
			}
		})
	}
}

func TestBreakerOpensAndRecoversAfterCacheRestart(t *testing.T) {
	e := newEnv(t)
	var transitions atomic.Int32
	c := e.client(t, protocol.Namespace{}, func(cfg *client.Config) {
		cfg.OnStateChange = func(_, _ client.State) { transitions.Add(1) }
	})
	c.Set(ctx(t), "k", []byte("v"))
	epoch := c.Epoch()

	e.node.Kill()
	for range 5 {
		if it := lookup(t, c, "k"); it.Source != client.SourceStore || string(it.Value) != "v" {
			t.Fatalf("fallback read: %+v", it)
		}
	}
	if c.State() != client.StateOpen {
		t.Fatalf("breaker should be open, is %v", c.State())
	}
	// While open, reads skip the cache entirely and are fast.
	start := time.Now()
	for range 100 {
		lookup(t, c, "k")
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("100 reads with open breaker took %v", d)
	}

	e.node.Start() // fresh process: empty memory, namespace unknown
	testutil.Eventually(t, 5*time.Second, func() bool {
		return lookup(t, c, "k").Source == client.SourceCache
	}, "client back on the cache")
	if c.State() != client.StateClosed {
		t.Fatalf("breaker %v", c.State())
	}
	if c.Epoch() == epoch {
		t.Fatal("epoch should change after restart")
	}
	if n := transitions.Load(); n < 3 { // closed→open→half-open→closed
		t.Fatalf("transitions %d", n)
	}
}

func TestTransparentReregistrationAfterRestart(t *testing.T) {
	e := newEnv(t)
	c := e.client(t, protocol.Namespace{})
	c.Set(ctx(t), "k", []byte("v"))
	e.node.Restart()
	// The first call after the restart hits NAMESPACE_NOT_FOUND internally;
	// the client re-registers and retries, the caller sees nothing.
	testutil.Eventually(t, 3*time.Second, func() bool {
		err := c.Set(ctx(t), "k", []byte("v2"))
		return err == nil
	}, "write after restart")
	if m := c.Metrics(); m.Reregistered == 0 {
		t.Fatalf("expected a re-registration, metrics %+v", m)
	}
}

func TestSlowCacheFallsBackToStore(t *testing.T) {
	var delay atomic.Int64
	e := newEnv(t, testutil.WithServerOptions(grpc.UnaryInterceptor(
		func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			time.Sleep(time.Duration(delay.Load()))
			return h(ctx, req)
		})))
	c := e.client(t, protocol.Namespace{})
	c.Set(ctx(t), "k", []byte("v"))
	delay.Store(int64(time.Second))

	start := time.Now()
	it := lookup(t, c, "k")
	if it.Source != client.SourceStore || time.Since(start) > 600*time.Millisecond {
		t.Fatalf("slow cache: %+v after %v", it, time.Since(start))
	}
}

// ---------------------------------------------------------------- partitions

// A client that cannot reach the cache keeps writing (write-around) directly
// to the store. The cache follows the store's change stream, so clients that
// can still reach it see the new value — the cache and store stay in sync.
func TestPartitionWriteAroundKeepsCacheInSync(t *testing.T) {
	e := newEnv(t)
	spec := protocol.Namespace{Write: protocol.WriteAround}
	a, link := e.clientVia(t, spec)
	b := e.client(t, spec)

	a.Set(ctx(t), "k", []byte("v1"))
	if it := lookup(t, b, "k"); string(it.Value) != "v1" {
		t.Fatalf("b: %+v", it)
	}
	lookup(t, b, "k") // cached

	link.SetMode(testutil.Blackhole)
	if err := a.Set(ctx(t), "k", []byte("v2")); err != nil {
		t.Fatalf("write-around during partition must succeed: %v", err)
	}
	if v, _ := e.stored("k"); v != "v2" {
		t.Fatalf("store has %q", v)
	}
	if a.Metrics().FallbackWrites != 1 {
		t.Fatalf("metrics %+v", a.Metrics())
	}
	testutil.Eventually(t, 2*time.Second, func() bool {
		it := lookup(t, b, "k")
		return it.Source == client.SourceCache && string(it.Value) == "v2"
	}, "cache picked up the direct write")

	link.SetMode(testutil.Forward)
	testutil.Eventually(t, 5*time.Second, func() bool {
		it := lookup(t, a, "k")
		return it.Source == client.SourceCache && string(it.Value) == "v2"
	}, "partitioned client back on the cache")
}

// Write-through refuses writes it cannot put through the cache, so the cache
// never diverges from the store.
func TestPartitionWriteThroughRejects(t *testing.T) {
	e := newEnv(t)
	spec := protocol.Namespace{Write: protocol.WriteThrough}
	a, link := e.clientVia(t, spec)
	b := e.client(t, spec)
	a.Set(ctx(t), "k", []byte("v1"))

	link.SetMode(testutil.Blackhole)
	if err := a.Set(ctx(t), "k", []byte("v2")); !errors.Is(err, client.ErrCacheUnavailable) {
		t.Fatalf("want ErrCacheUnavailable, got %v", err)
	}
	if v, _ := e.stored("k"); v != "v1" {
		t.Fatalf("store changed to %q", v)
	}
	if it := lookup(t, b, "k"); string(it.Value) != "v1" {
		t.Fatalf("b: %+v", it)
	}
	// Reads still work for the partitioned client.
	if it := lookup(t, a, "k"); it.Source != client.SourceStore || string(it.Value) != "v1" {
		t.Fatalf("a: %+v", it)
	}
}

// ---------------------------------------------------------------- store failures

func TestStoreUnreachableFromCacheOnly(t *testing.T) {
	e := newEnv(t)
	wa := e.client(t, protocol.Namespace{Write: protocol.WriteAround})
	e.db.Backend(prefix).Put(ctx(t), "k", []byte("v"))

	e.faults.SetFailing(true)
	for range 10 {
		if it := lookup(t, wa, "k"); it.Source != client.SourceStore || string(it.Value) != "v" {
			t.Fatalf("read: %+v", it)
		}
	}
	if wa.State() != client.StateClosed {
		t.Fatal("store failures behind the cache must not trip the breaker")
	}
	if err := wa.Set(ctx(t), "k", []byte("v2")); err != nil {
		t.Fatalf("write-around falls back to the store: %v", err)
	}
	if v, _ := e.stored("k"); v != "v2" {
		t.Fatalf("store %q", v)
	}

	e.node.Engine().Register("wt", protocol.Namespace{Write: protocol.WriteThrough, KeyPrefix: prefix})
	wt := testutil.NewClient(t, e.node.Addr(), "wt", protocol.Namespace{Write: protocol.WriteThrough, KeyPrefix: prefix}, e.db.Backend(prefix))
	if err := wt.Set(ctx(t), "k", []byte("v3")); !errors.Is(err, client.ErrStoreUnavailable) {
		t.Fatalf("want ErrStoreUnavailable, got %v", err)
	}
	e.faults.SetFailing(false)
}

func TestStoreDownEverywhere(t *testing.T) {
	e := newEnv(t)
	c := e.client(t, protocol.Namespace{})
	e.db.SetDown(true)
	defer e.db.SetDown(false)
	if _, err := c.Lookup(ctx(t), "k"); !errors.Is(err, client.ErrStoreUnavailable) {
		t.Fatalf("want ErrStoreUnavailable, got %v", err)
	}
}

// ---------------------------------------------------------------- write-behind

func TestWriteBehindFlushedOnGracefulShutdown(t *testing.T) {
	e := newEnv(t)
	c := e.client(t, protocol.Namespace{Write: protocol.WriteBehind, FlushInterval: time.Hour})
	for i := range 50 {
		if err := c.Set(ctx(t), fmt.Sprint(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := e.stored("0"); ok {
		t.Fatal("flushed too early")
	}
	if err := e.node.Shutdown(ctx(t)); err != nil {
		t.Fatal(err)
	}
	for i := range 50 {
		if _, ok := e.stored(fmt.Sprint(i)); !ok {
			t.Fatalf("key %d lost on graceful shutdown", i)
		}
	}
}

// Documented trade-off: write-behind acknowledges before persisting, so a
// crash loses writes that were not flushed yet.
func TestWriteBehindLosesUnflushedWritesOnCrash(t *testing.T) {
	e := newEnv(t)
	c := e.client(t, protocol.Namespace{Write: protocol.WriteBehind, FlushInterval: time.Hour})
	if err := c.Set(ctx(t), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	e.node.Kill()
	if _, ok := e.stored("k"); ok {
		t.Fatal("unexpected persistence")
	}
	e.node.Start()
	testutil.Eventually(t, 5*time.Second, func() bool {
		it, err := c.Lookup(ctx(t), "k")
		return err == nil && it.Source == client.SourceCache && !it.Found
	}, "write lost after crash")
}

func TestWriteBehindBackpressure(t *testing.T) {
	e := newEnv(t)
	c := e.client(t, protocol.Namespace{Write: protocol.WriteBehind, FlushInterval: time.Hour, MaxPending: 3})
	e.db.SetDown(true)
	defer e.db.SetDown(false)
	for i := range 3 {
		if err := c.Set(ctx(t), fmt.Sprint(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Set(ctx(t), "x", []byte("v")); !errors.Is(err, client.ErrBackpressure) {
		t.Fatalf("want ErrBackpressure, got %v", err)
	}
	if c.State() != client.StateClosed {
		t.Fatal("backpressure must not trip the breaker")
	}
}

// ---------------------------------------------------------------- misc

func TestCallerCancellationDoesNotTripBreaker(t *testing.T) {
	e := newEnv(t)
	c := e.client(t, protocol.Namespace{})
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 20 {
		if _, err := c.Lookup(cctx, "k"); !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	}
	if c.State() != client.StateClosed {
		t.Fatalf("breaker %v", c.State())
	}
}

func TestClosedClient(t *testing.T) {
	e := newEnv(t)
	c := e.client(t, protocol.Namespace{})
	c.Close()
	if _, err := c.Lookup(ctx(t), "k"); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}
