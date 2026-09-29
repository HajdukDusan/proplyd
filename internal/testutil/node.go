package testutil

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/c12s/proplyd/backend"
	"github.com/c12s/proplyd/cache"
	"github.com/c12s/proplyd/client"
	"github.com/c12s/proplyd/protocol"
	"github.com/c12s/proplyd/server"
	"google.golang.org/grpc"
)

// CacheNode is a cache server that can be crashed, restarted and partitioned.
// Clients reach it through a Proxy whose address never changes.
type CacheNode struct {
	t        testing.TB
	backends backend.Factory
	opts     []grpc.ServerOption
	engOpts  cache.Options

	Proxy *Proxy

	mu     sync.Mutex
	srv    *server.Server
	engine *cache.Engine
}

// NodeOption customises a CacheNode.
type NodeOption func(*CacheNode)

// WithServerOptions sets gRPC server options (e.g. interceptors).
func WithServerOptions(opts ...grpc.ServerOption) NodeOption {
	return func(n *CacheNode) { n.opts = append(server.DefaultServerOptions(), opts...) }
}

// WithEngineOptions sets engine options (Backends is always overridden).
func WithEngineOptions(o cache.Options) NodeOption {
	return func(n *CacheNode) { n.engOpts = o }
}

// StartCacheNode starts a cache backed by backends.
func StartCacheNode(t testing.TB, backends backend.Factory, opts ...NodeOption) *CacheNode {
	t.Helper()
	n := &CacheNode{t: t, backends: backends}
	for _, o := range opts {
		o(n)
	}
	p, err := NewProxy("")
	if err != nil {
		t.Fatal(err)
	}
	n.Proxy = p
	n.Start()
	t.Cleanup(func() {
		n.Kill()
		_ = p.Close()
	})
	return n
}

// Addr is the stable address clients should use.
func (n *CacheNode) Addr() string { return n.Proxy.Addr() }

// Engine returns the current engine (nil while stopped).
func (n *CacheNode) Engine() *cache.Engine {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.engine
}

// Start starts a fresh cache process (empty memory, new epoch).
func (n *CacheNode) Start() {
	n.t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.srv != nil {
		return
	}
	eo := n.engOpts
	eo.Backends = n.backends
	if eo.Logger == nil {
		eo.Logger = QuietLogger()
	}
	if eo.LoadTimeout == 0 {
		eo.LoadTimeout = 2 * time.Second
	}
	if eo.FlushTimeout == 0 {
		eo.FlushTimeout = 2 * time.Second
	}
	eng, err := cache.New(eo)
	if err != nil {
		n.t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		n.t.Fatal(err)
	}
	n.engine = eng
	n.srv = server.Start(lis, eng, eo.Logger, n.opts...)
	n.Proxy.SetUpstream(lis.Addr().String())
	n.Proxy.SetMode(Forward)
}

// Kill crashes the cache: connections drop, memory and unflushed
// write-behind data are lost.
func (n *CacheNode) Kill() {
	n.mu.Lock()
	srv := n.srv
	n.srv, n.engine = nil, nil
	n.mu.Unlock()
	n.Proxy.SetMode(Refuse)
	if srv != nil {
		srv.Kill()
	}
}

// Shutdown stops the cache gracefully (write-behind data is flushed).
func (n *CacheNode) Shutdown(ctx context.Context) error {
	n.mu.Lock()
	srv := n.srv
	n.srv, n.engine = nil, nil
	n.mu.Unlock()
	if srv == nil {
		return nil
	}
	err := srv.Shutdown(ctx)
	n.Proxy.SetMode(Refuse)
	return err
}

// Restart crashes and starts the cache.
func (n *CacheNode) Restart() {
	n.Kill()
	n.Start()
}

// NewClient creates a client for namespace ns with fast failure detection
// suitable for tests.
func NewClient(t testing.TB, addr, ns string, spec protocol.Namespace, store backend.Backend, mutate ...func(*client.Config)) *client.Client {
	t.Helper()
	cfg := client.Config{
		Target:         addr,
		Namespace:      ns,
		Spec:           spec,
		Store:          store,
		RequestTimeout: 250 * time.Millisecond,
		WriteTimeout:   250 * time.Millisecond,
		Logger:         QuietLogger(),
		Breaker: client.BreakerConfig{
			ConsecutiveFailures: 3,
			OpenTimeout:         200 * time.Millisecond,
			HalfOpenRequests:    1,
		},
	}
	for _, m := range mutate {
		m(&cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.New(ctx, cfg)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// QuietLogger discards logs unless PROPLYD_TEST_LOGS is set.
func QuietLogger() *slog.Logger {
	if os.Getenv("PROPLYD_TEST_LOGS") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Eventually polls cond until it returns true or timeout expires.
func Eventually(t testing.TB, timeout time.Duration, cond func() bool, msg string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v: "+msg, append([]any{timeout}, args...)...)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
