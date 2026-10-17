// Package engine is the proplyd cache engine. It keeps cached entries in
// Redis, loads and writes through a backend.Backend, follows the backend's
// change stream, and publishes invalidations for near-caches.
//
// The engine is stateless apart from in-flight work: everything that must
// survive a restart (namespaces, entries, watch progress, invalidation log)
// lives in Redis. Several engines can share one Redis; a lease makes sure
// only one of them follows a namespace's change stream at a time.
//
// # Consistency
//
// Every entry carries the backend revision it reflects and the namespace
// epoch. All changes go through installScript, which applies a change only
// if it is newer than what Redis holds (see scripts.go). Writes that bypass
// the cache are picked up by the watcher, which turns them into in-place
// updates or fences. A fence blocks loads that read before the change, so a
// slow load cannot install a value older than a concurrent write.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c12s/proplyd/pkg/api"
	"github.com/c12s/proplyd/pkg/backend"
	"github.com/c12s/proplyd/pkg/protocol"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
)

// Options configures an Engine.
type Options struct {
	// Redis holds the cache. Required.
	Redis redis.UniversalClient
	// Backend is the backing store. Required. It must also implement
	// backend.Watcher for namespaces with SyncWatch.
	Backend backend.Backend
	// InstanceID identifies this engine in watcher leases. A restarted
	// instance with the same ID takes its lease back at once. Default: the
	// hostname.
	InstanceID string
	// RedisKeyPrefix is prepended to every Redis key. Default "pd:".
	RedisKeyPrefix string
	// AllowedPrefixes, if set, restricts the backend key prefixes that
	// namespaces may use.
	AllowedPrefixes []string
	// MaxValueSize bounds stored values. Default 1 MiB.
	MaxValueSize int
	// LoadTimeout bounds a backend load. Default 5s.
	LoadTimeout time.Duration
	// UpdateTimeout bounds the cache update that follows a backend write.
	// It runs even if the caller gives up. Default 2s.
	UpdateTimeout time.Duration
	// FenceTTL is how long a fence lives. It must exceed LoadTimeout so
	// that no load that read before the fence can outlive it. Default 30s.
	FenceTTL time.Duration
	// ReadyTimeout is how long a request waits for a namespace that is
	// still catching up with the backend. Default 2s.
	ReadyTimeout time.Duration
	// LeaseTTL is the watcher lease duration. Default 5s.
	LeaseTTL time.Duration
	// InvalidationLogSize is the approximate number of invalidations kept
	// per namespace for subscribers to resume from. Default 10000.
	InvalidationLogSize int64
	// HeartbeatInterval is how often subscribers hear from an idle stream.
	// Default 1s.
	HeartbeatInterval time.Duration
	// RedisCheckInterval is how often Redis is probed for health and
	// restarts. Default 500ms.
	RedisCheckInterval time.Duration
	Logger             *slog.Logger
}

func (o *Options) setDefaults() error {
	if o.Redis == nil || o.Backend == nil {
		return errors.New("engine: Redis and Backend are required")
	}
	if o.InstanceID == "" {
		h, err := os.Hostname()
		if err != nil || h == "" {
			h = fmt.Sprintf("proplyd-%x", rand.Uint64())
		}
		o.InstanceID = h
	}
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	if o.RedisKeyPrefix == "" {
		o.RedisKeyPrefix = "pd:"
	}
	if o.MaxValueSize <= 0 {
		o.MaxValueSize = 1 << 20
	}
	def(&o.LoadTimeout, 5*time.Second)
	def(&o.UpdateTimeout, 2*time.Second)
	def(&o.FenceTTL, 30*time.Second)
	def(&o.ReadyTimeout, 2*time.Second)
	def(&o.LeaseTTL, 5*time.Second)
	def(&o.HeartbeatInterval, time.Second)
	def(&o.RedisCheckInterval, 500*time.Millisecond)
	if o.InvalidationLogSize <= 0 {
		o.InvalidationLogSize = 10000
	}
	if o.FenceTTL <= o.LoadTimeout {
		return fmt.Errorf("engine: FenceTTL (%v) must exceed LoadTimeout (%v)", o.FenceTTL, o.LoadTimeout)
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	return nil
}

// Engine is the cache engine.
type Engine struct {
	opts    Options
	rdb     redis.UniversalClient
	backend backend.Backend
	log     *slog.Logger

	ctx    context.Context // engine lifetime
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu         sync.Mutex
	namespaces map[string]*namespace
	activating singleflight.Group

	redisUp  atomic.Bool
	runID    string // last seen Redis run_id; monitor goroutine only
	draining atomic.Bool

	stats Stats
}

// Stats are engine counters.
type Stats struct {
	Hits, Misses, Loads, LoadErrors   atomic.Int64
	Fills, FillsRejected              atomic.Int64
	Writes, WatchEvents, Purges       atomic.Int64
	RedisRestarts, NamespacesReloaded atomic.Int64
}

// New starts an engine.
func New(opts Options) (*Engine, error) {
	if err := opts.setDefaults(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		opts:       opts,
		rdb:        opts.Redis,
		backend:    opts.Backend,
		log:        opts.Logger,
		ctx:        ctx,
		cancel:     cancel,
		namespaces: map[string]*namespace{},
	}
	e.checkRedis()
	e.wg.Add(1)
	go e.monitorRedis()
	return e, nil
}

// Stats returns the engine counters.
func (e *Engine) Stats() *Stats { return &e.stats }

// RedisUp reports whether the last Redis probe succeeded.
func (e *Engine) RedisUp() bool { return e.redisUp.Load() }

// DrainSubscriptions ends every subscription and refuses new ones, so that
// a graceful shutdown does not wait on streams that never end by
// themselves. Subscribers reconnect (to another replica, or after the
// restart) and resume.
func (e *Engine) DrainSubscriptions() {
	e.draining.Store(true)
	e.mu.Lock()
	nss := make([]*namespace, 0, len(e.namespaces))
	for _, ns := range e.namespaces {
		nss = append(nss, ns)
	}
	e.mu.Unlock()
	for _, ns := range nss {
		ns.tail.closeAll(fmt.Errorf("%w: server shutting down", ErrCacheUnavailable))
	}
}

// Close stops the engine: watchers stop and release their leases, and
// subscriptions end.
func (e *Engine) Close() {
	e.shutdown(true)
}

// Abort stops the engine like a crash: leases are not released.
func (e *Engine) Abort() {
	e.shutdown(false)
}

func (e *Engine) shutdown(graceful bool) {
	e.mu.Lock()
	nss := e.namespaces
	e.namespaces = map[string]*namespace{}
	e.mu.Unlock()
	for _, ns := range nss {
		ns.stop(graceful)
	}
	e.cancel()
	e.wg.Wait()
}

// Register creates a namespace, or checks that an existing one has the same
// spec. It returns the normalized spec.
func (e *Engine) Register(ctx context.Context, name string, spec protocol.Spec) (protocol.Spec, error) {
	if err := protocol.ValidateNamespace(name); err != nil {
		return protocol.Spec{}, invalidf("%v", err)
	}
	spec, err := spec.Normalize()
	if err != nil {
		return protocol.Spec{}, invalidf("%v", err)
	}
	if !e.prefixAllowed(spec.KeyPrefix) {
		return protocol.Spec{}, fmt.Errorf("%w: %q", ErrPrefixNotAllowed, spec.KeyPrefix)
	}
	if spec.Sync == protocol.SyncWatch {
		if _, ok := e.backend.(backend.Watcher); !ok {
			return protocol.Spec{}, invalidf("the backend has no change stream; use SyncNone")
		}
	}
	if err := e.checkUp(); err != nil {
		return protocol.Spec{}, err
	}
	ks := newKeyspace(e.opts.RedisKeyPrefix, name)
	specBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(spec.ToProto())
	if err != nil {
		return protocol.Spec{}, err
	}

	existing, err := e.rdb.HGet(ctx, ks.meta, "spec").Bytes()
	switch {
	case errors.Is(err, redis.Nil):
		// Create it. The backend revision becomes the namespace's starting
		// point: the watcher follows changes after it.
		rev, err := e.backend.Revision(ctx)
		if err != nil {
			return protocol.Spec{}, err
		}
		res, err := registerScript.Run(ctx, e.rdb, []string{ks.meta}, specBytes, epochBytes(newEpoch()), rev).Slice()
		if err != nil {
			return protocol.Spec{}, redisErr("register", err)
		}
		existing = []byte(res[1].(string))
		if res[0].(int64) == 1 {
			e.log.Info("namespace created", "namespace", name, "spec", spec.String(), "revision", rev)
		}
	case err != nil:
		return protocol.Spec{}, redisErr("register", err)
	}

	cur, err := decodeSpec(existing)
	if err != nil {
		return protocol.Spec{}, err
	}
	if !cur.Equal(spec) {
		return protocol.Spec{}, fmt.Errorf("%w: registered %v, requested %v", ErrNamespaceConflict, cur, spec)
	}
	// Activate right away so the watcher starts following the backend.
	if _, err := e.namespace(ctx, name); err != nil {
		return protocol.Spec{}, err
	}
	return spec, nil
}

func (e *Engine) prefixAllowed(p string) bool {
	if len(e.opts.AllowedPrefixes) == 0 {
		return true
	}
	for _, a := range e.opts.AllowedPrefixes {
		if strings.HasPrefix(p, a) {
			return true
		}
	}
	return false
}

// checkUp fails fast while Redis is known to be down.
func (e *Engine) checkUp() error {
	if !e.redisUp.Load() {
		return fmt.Errorf("%w: redis is down", ErrCacheUnavailable)
	}
	return nil
}

// namespace returns the active namespace, activating it from Redis if this
// engine has not used it yet.
func (e *Engine) namespace(ctx context.Context, name string) (*namespace, error) {
	e.mu.Lock()
	ns := e.namespaces[name]
	e.mu.Unlock()
	if ns != nil {
		return ns, nil
	}
	if err := protocol.ValidateNamespace(name); err != nil {
		return nil, invalidf("%v", err)
	}
	if err := e.checkUp(); err != nil {
		return nil, err
	}
	ch := e.activating.DoChan(name, func() (any, error) {
		return e.activate(name)
	})
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.(*namespace), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *Engine) activate(name string) (*namespace, error) {
	e.mu.Lock()
	if ns := e.namespaces[name]; ns != nil {
		e.mu.Unlock()
		return ns, nil
	}
	e.mu.Unlock()

	ctx, cancel := context.WithTimeout(e.ctx, e.opts.ReadyTimeout)
	defer cancel()
	ks := newKeyspace(e.opts.RedisKeyPrefix, name)
	m, err := readMeta(ctx, e.rdb, ks)
	if err != nil {
		return nil, err
	}
	ns := newNamespace(e, name, ks, m)

	e.mu.Lock()
	if e.ctx.Err() != nil {
		e.mu.Unlock()
		return nil, ErrClosed
	}
	e.namespaces[name] = ns
	e.mu.Unlock()
	ns.start()
	e.log.Debug("namespace activated", "namespace", name, "epoch", m.epoch, "wrev", m.wrev)
	return ns, nil
}

// forget drops ns from the active set (it no longer exists in Redis, or
// Redis restarted). The next request activates it again.
func (e *Engine) forget(ns *namespace) {
	e.mu.Lock()
	if e.namespaces[ns.name] == ns {
		delete(e.namespaces, ns.name)
	}
	e.mu.Unlock()
	ns.stop(false)
}

type meta struct {
	spec  protocol.Spec
	epoch uint64
	floor int64
	wrev  int64
}

func readMeta(ctx context.Context, rdb redis.UniversalClient, ks keyspace) (meta, error) {
	res, err := metaScript.Run(ctx, rdb, []string{ks.meta}).Slice()
	if err != nil {
		return meta{}, redisErr("read meta", err)
	}
	if res[0].(int64) == resNamespaceMissing {
		return meta{}, ErrNamespaceNotFound
	}
	spec, err := decodeSpec([]byte(res[1].(string)))
	if err != nil {
		return meta{}, err
	}
	epoch, ok := parseEpoch(res[2].(string))
	if !ok {
		return meta{}, fmt.Errorf("corrupt namespace epoch in %s", ks.meta)
	}
	var m meta
	m.spec, m.epoch = spec, epoch
	if _, err := fmt.Sscan(res[3].(string), &m.floor); err != nil {
		return meta{}, fmt.Errorf("corrupt floor in %s: %w", ks.meta, err)
	}
	if _, err := fmt.Sscan(res[4].(string), &m.wrev); err != nil {
		return meta{}, fmt.Errorf("corrupt wrev in %s: %w", ks.meta, err)
	}
	return m, nil
}

func decodeSpec(b []byte) (protocol.Spec, error) {
	var p api.NamespaceSpec
	if err := proto.Unmarshal(b, &p); err != nil {
		return protocol.Spec{}, fmt.Errorf("corrupt namespace spec: %w", err)
	}
	return protocol.SpecFromProto(&p).Normalize()
}

func newEpoch() uint64 {
	for {
		if e := rand.Uint64(); e != 0 {
			return e
		}
	}
}

// monitorRedis probes Redis. A changed run_id means Redis restarted (or
// failed over) and may have lost or rolled back data. Every namespace is
// then reloaded: its watcher replays from the persisted progress, and
// subscribers reconnect and find out whether they can resume.
func (e *Engine) monitorRedis() {
	defer e.wg.Done()
	t := time.NewTicker(e.opts.RedisCheckInterval)
	defer t.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-t.C:
			e.checkRedis()
		}
	}
}

func (e *Engine) checkRedis() {
	ctx, cancel := context.WithTimeout(e.ctx, max(e.opts.RedisCheckInterval, 500*time.Millisecond))
	defer cancel()
	info, err := e.rdb.Info(ctx, "server").Result()
	if err != nil {
		if e.redisUp.Swap(false) {
			e.log.Warn("redis down", "err", err)
		}
		return
	}
	runID := infoField(info, "run_id")
	if e.runID != "" && runID != e.runID {
		e.stats.RedisRestarts.Add(1)
		e.log.Warn("redis restarted, reloading namespaces", "old_run_id", e.runID, "run_id", runID)
		e.reloadAll()
	}
	e.runID = runID
	if !e.redisUp.Swap(true) {
		if err := loadScripts(ctx, e.rdb); err != nil {
			e.log.Warn("loading scripts", "err", err)
		}
		e.log.Info("redis up", "run_id", runID)
	}
}

func (e *Engine) reloadAll() {
	e.mu.Lock()
	nss := e.namespaces
	e.namespaces = map[string]*namespace{}
	e.mu.Unlock()
	for _, ns := range nss {
		e.stats.NamespacesReloaded.Add(1)
		ns.stop(false)
	}
}

func infoField(info, field string) string {
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), field+":"); ok {
			return v
		}
	}
	return ""
}
