// Package cache is the core of proplyd: a multi-tenant cache engine in which
// every namespace has its own read strategy (cache-aside, read-through,
// refresh-ahead), write strategy (write-through, write-around, write-behind)
// and eviction policy, in front of a revisioned backing store (etcd).
//
// # Consistency model
//
// Every cached entry carries the store revision it reflects. All state
// changes of a key are serialised by a lock stripe and are only applied if
// they are newer than both the cached entry and the latest change the
// namespace knows of (see changeLog). As a result a slow load, a racing
// cache-aside fill or a replayed watch event can never replace newer data.
//
// The engine is transport-agnostic; package server exposes it over gRPC.
package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/c12s/proplyd/backend"
	"github.com/c12s/proplyd/protocol"
	"github.com/c12s/proplyd/store"
)

// Options configures an Engine.
type Options struct {
	// Backends opens the backing store for a namespace key prefix. Required.
	Backends backend.Factory
	// Stores builds the in-memory store of a namespace. Default: store.HotFactory.
	Stores store.Factory
	Logger *slog.Logger
	// LoadTimeout bounds a single read-through load. Default 5s.
	LoadTimeout time.Duration
	// FlushTimeout bounds a single write-behind transaction. Default 10s.
	FlushTimeout time.Duration
	// ChangeLogSize is the number of recent key changes remembered per
	// namespace to reject stale fills. Default 16384.
	ChangeLogSize int
	// MaxConcurrentRefreshes bounds background refresh-ahead loads per
	// namespace. Default 64.
	MaxConcurrentRefreshes int
	// PrefixPolicy, if set, authorises the key prefix a namespace asks for.
	PrefixPolicy func(namespace, prefix string) error
}

func (o Options) withDefaults() Options {
	if o.Stores == nil {
		o.Stores = store.HotFactory
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.LoadTimeout <= 0 {
		o.LoadTimeout = 5 * time.Second
	}
	if o.FlushTimeout <= 0 {
		o.FlushTimeout = 10 * time.Second
	}
	if o.ChangeLogSize <= 0 {
		o.ChangeLogSize = 16384
	}
	if o.MaxConcurrentRefreshes <= 0 {
		o.MaxConcurrentRefreshes = 64
	}
	return o
}

// Engine hosts namespaces.
type Engine struct {
	opts  Options
	epoch string

	mu         sync.RWMutex
	namespaces map[string]*namespace
	closed     bool
}

// New creates an engine.
func New(opts Options) (*Engine, error) {
	if opts.Backends == nil {
		return nil, errors.New("cache: Options.Backends is required")
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	return &Engine{
		opts:       opts.withDefaults(),
		epoch:      hex.EncodeToString(b[:]),
		namespaces: map[string]*namespace{},
	}, nil
}

// Epoch identifies this engine instance; it changes on every restart.
func (e *Engine) Epoch() string { return e.epoch }

// Register creates a namespace or attaches to an existing one with the same
// configuration. It returns the effective configuration and whether the
// namespace was created.
func (e *Engine) Register(name string, cfg protocol.Namespace) (protocol.Namespace, bool, error) {
	if name == "" || len(name) > 256 {
		return protocol.Namespace{}, false, invalid("namespace name must be 1-256 bytes")
	}
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return protocol.Namespace{}, false, invalid("%v", err)
	}
	if e.opts.PrefixPolicy != nil {
		if err := e.opts.PrefixPolicy(name, cfg.KeyPrefix); err != nil {
			return protocol.Namespace{}, false, invalid("key prefix rejected: %v", err)
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return protocol.Namespace{}, false, ErrClosed
	}
	if ns, ok := e.namespaces[name]; ok {
		if ns.cfg != cfg {
			return ns.cfg, false, ErrNamespaceConflict
		}
		return ns.cfg, false, nil
	}

	st, err := e.opts.Stores(name, store.Config{
		Policy:          store.Policy(cfg.Eviction),
		Capacity:        cfg.Capacity,
		JanitorInterval: cfg.TTL,
	})
	if err != nil {
		return protocol.Namespace{}, false, invalid("%v", err)
	}
	be, err := e.opts.Backends(cfg.KeyPrefix)
	if err != nil {
		_ = st.Close()
		return protocol.Namespace{}, false, &BackendError{Err: err}
	}
	ns, err := newNamespace(name, cfg, st, be, &e.opts)
	if err != nil {
		_ = st.Close()
		_ = be.Close()
		return protocol.Namespace{}, false, err
	}
	e.namespaces[name] = ns
	e.opts.Logger.Info("namespace registered", "namespace", name, "read", cfg.Read, "write", cfg.Write,
		"eviction", cfg.Eviction, "capacity", cfg.Capacity, "ttl", cfg.TTL, "sync", cfg.Sync)
	return cfg, true, nil
}

func (e *Engine) ns(name string) (*namespace, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, ErrClosed
	}
	ns, ok := e.namespaces[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNamespaceNotFound, name)
	}
	return ns, nil
}

func validKey(key string) error {
	if key == "" {
		return invalid("key must not be empty")
	}
	return nil
}

// Get reads a key.
func (e *Engine) Get(ctx context.Context, namespace, key string) (Result, error) {
	if err := validKey(key); err != nil {
		return Result{}, err
	}
	ns, err := e.ns(namespace)
	if err != nil {
		return Result{}, err
	}
	return ns.get(ctx, key)
}

// Set writes a key using the namespace write strategy. ttl <= 0 uses the
// namespace default. It returns the store revision (0 for write-behind).
func (e *Engine) Set(ctx context.Context, namespace, key string, value []byte, ttl time.Duration) (int64, error) {
	if err := validKey(key); err != nil {
		return 0, err
	}
	ns, err := e.ns(namespace)
	if err != nil {
		return 0, err
	}
	return ns.set(ctx, key, value, ttl)
}

// Delete removes a key using the namespace write strategy.
func (e *Engine) Delete(ctx context.Context, namespace, key string) (int64, error) {
	if err := validKey(key); err != nil {
		return 0, err
	}
	ns, err := e.ns(namespace)
	if err != nil {
		return 0, err
	}
	return ns.del(ctx, key)
}

// Fill installs a record the caller read from the store (cache-aside). It
// reports whether the record was installed; stale records are rejected.
func (e *Engine) Fill(namespace, key string, rec backend.Record, ttl time.Duration) (bool, error) {
	if err := validKey(key); err != nil {
		return false, err
	}
	ns, err := e.ns(namespace)
	if err != nil {
		return false, err
	}
	if rec.Found && len(rec.Value) > ns.cfg.MaxValueBytes {
		return false, ErrValueTooLarge
	}
	if rec.Revision <= 0 || rec.ModRevision > rec.Revision {
		return false, invalid("fill requires read_revision > 0 and mod_revision <= read_revision")
	}
	return ns.fill(key, rec, ttl), nil
}

// Invalidate drops keys from the cache without touching the store.
func (e *Engine) Invalidate(namespace string, keys ...string) error {
	ns, err := e.ns(namespace)
	if err != nil {
		return err
	}
	ns.invalidate(keys)
	return nil
}

// Purge drops every cached key of a namespace (pending writes are kept).
func (e *Engine) Purge(namespace string) error {
	ns, err := e.ns(namespace)
	if err != nil {
		return err
	}
	ns.purge()
	return nil
}

// Stats returns namespace counters.
func (e *Engine) Stats(namespace string) (Stats, error) {
	ns, err := e.ns(namespace)
	if err != nil {
		return Stats{}, err
	}
	return ns.snapshot(), nil
}

// Flush synchronously persists all pending write-behind entries of a
// namespace (bounded by ctx). It is a no-op for other strategies.
func (e *Engine) Flush(ctx context.Context, namespace string) error {
	ns, err := e.ns(namespace)
	if err != nil {
		return err
	}
	if ns.cfg.Write != protocol.WriteBehind {
		return nil
	}
	return ns.drain(ctx)
}

// Close stops the engine. Pending write-behind entries are flushed until ctx
// expires; whatever is still pending afterwards is lost.
func (e *Engine) Close(ctx context.Context) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	nss := make([]*namespace, 0, len(e.namespaces))
	for _, ns := range e.namespaces {
		nss = append(nss, ns)
	}
	e.mu.Unlock()

	var wg sync.WaitGroup
	errs := make([]error, len(nss))
	for i, ns := range nss {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := ns.close(ctx); err != nil {
				errs[i] = fmt.Errorf("namespace %q: %w", ns.name, err)
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}
