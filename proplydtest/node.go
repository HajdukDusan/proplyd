package proplydtest

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c12s/proplyd/pkg/backend"
	"github.com/c12s/proplyd/pkg/server"
	"github.com/redis/go-redis/v9"
)

// NodeConfig configures a Node.
type NodeConfig struct {
	// RedisAddr is where the node's Redis client connects (put a Proxy in
	// between to break the node-Redis link).
	RedisAddr string
	Backend   backend.Backend
	// Options are passed to server.New; Redis, Backend and Logger are set
	// by the node. Test-friendly intervals are used where zero.
	Options server.Options
}

var nodeSeq atomic.Int64

// Node is a proplyd server that can be crashed and restarted. Clients
// connect through a Proxy, so the address stays the same across restarts
// and network faults can be injected.
type Node struct {
	t     TB
	cfg   NodeConfig
	proxy *Proxy

	mu   sync.Mutex
	srv  *server.Server
	rdb  *redis.Client
	done chan struct{}
}

// StartNode starts a node.
func StartNode(t TB, cfg NodeConfig) *Node {
	t.Helper()
	o := &cfg.Options
	if o.InstanceID == "" {
		// Unique per node, stable across its restarts.
		o.InstanceID = fmt.Sprintf("node-%d", nodeSeq.Add(1))
	}
	if o.HeartbeatInterval == 0 {
		o.HeartbeatInterval = 200 * time.Millisecond
	}
	if o.RedisCheckInterval == 0 {
		o.RedisCheckInterval = 100 * time.Millisecond
	}
	if o.LeaseTTL == 0 {
		o.LeaseTTL = 2 * time.Second
	}
	if o.LoadTimeout == 0 {
		o.LoadTimeout = 2 * time.Second
	}
	if o.FenceTTL == 0 {
		o.FenceTTL = 10 * time.Second
	}
	if o.ReadyTimeout == 0 {
		o.ReadyTimeout = time.Second
	}
	n := &Node{t: t, cfg: cfg, proxy: NewProxy(t, "127.0.0.1:1")}
	n.Start()
	t.Cleanup(n.Kill)
	return n
}

// Addr is the stable client-facing address.
func (n *Node) Addr() string { return n.proxy.Addr() }

// Proxy is the proxy in front of the node.
func (n *Node) Proxy() *Proxy { return n.proxy }

// Server returns the running server, or nil.
func (n *Node) Server() *server.Server {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.srv
}

// Running reports whether the node is up.
func (n *Node) Running() bool { return n.Server() != nil }

// Start starts the server if it is not running.
func (n *Node) Start() {
	n.t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.srv != nil {
		return
	}
	opts := n.cfg.Options
	n.rdb = NewRedisClient(n.cfg.RedisAddr)
	opts.Redis = n.rdb
	opts.Backend = n.cfg.Backend
	if opts.Logger == nil {
		opts.Logger = testLogger()
	}
	srv, err := server.New(opts)
	if err != nil {
		n.t.Fatalf("server.New: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		n.t.Fatal(err)
	}
	n.srv = srv
	n.done = make(chan struct{})
	go func(done chan struct{}) {
		defer close(done)
		_ = srv.Serve(lis)
	}(n.done)
	n.proxy.SetUpstream(lis.Addr().String())
}

// Kill crashes the server: connections drop, nothing is cleaned up.
func (n *Node) Kill() {
	n.stop(func(s *server.Server) { s.Stop() })
}

// Shutdown stops the server gracefully.
func (n *Node) Shutdown() {
	n.stop(func(s *server.Server) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.Shutdown(ctx)
	})
}

// Restart crashes and starts the server.
func (n *Node) Restart() {
	n.Kill()
	n.Start()
}

func (n *Node) stop(how func(*server.Server)) {
	n.mu.Lock()
	srv, rdb, done := n.srv, n.rdb, n.done
	n.srv, n.rdb = nil, nil
	n.mu.Unlock()
	if srv == nil {
		return
	}
	n.proxy.SetUpstream("127.0.0.1:1") // refuse new connections
	how(srv)
	<-done
	n.proxy.ResetAll()
	_ = rdb.Close()
}

// testLogger logs warnings and errors to stderr when PROPLYD_TEST_LOG is
// set (to "debug" for everything), and discards them otherwise.
func testLogger() *slog.Logger {
	lvl, ok := os.LookupEnv("PROPLYD_TEST_LOG")
	if !ok {
		return slog.New(slog.DiscardHandler)
	}
	level := slog.LevelWarn
	if lvl == "debug" {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}
