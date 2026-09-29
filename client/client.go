// Package client is the Go client of the proplyd cache.
//
// A Client is bound to one namespace. It talks to the cache over gRPC and
// guarantees that the application keeps working when the cache does not:
//
//   - Every cache call has a short timeout and runs through a circuit breaker.
//     Once the breaker opens, calls skip the cache entirely until it probes
//     healthy again.
//   - Reads fall back to the backing store (Config.Store) directly.
//   - Writes follow the namespace write strategy:
//     write-around writes go to the store directly (every write is recorded;
//     the cache catches up through its store watch),
//     write-through and write-behind writes fail with ErrCacheUnavailable
//     (the cache never diverges from the store, but the write is rejected).
//   - If the cache restarted and lost the namespace, the client registers it
//     again transparently.
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/c12s/proplyd/api/proplydpb"
	"github.com/c12s/proplyd/backend"
	"github.com/c12s/proplyd/protocol"
	"github.com/sony/gobreaker/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// State is the circuit breaker state.
type State int

const (
	// StateClosed: the cache is healthy and used.
	StateClosed State = iota
	// StateHalfOpen: a few probe requests are sent to the cache.
	StateHalfOpen
	// StateOpen: the cache is considered down and bypassed.
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	default:
		return "open"
	}
}

// Source tells where a read was answered from.
type Source int

const (
	SourceCache Source = iota + 1
	SourceStore
)

func (s Source) String() string {
	if s == SourceCache {
		return "cache"
	}
	return "store"
}

// Item is the result of a Lookup.
type Item struct {
	Value []byte
	Found bool
	// ModRevision is the store revision that last modified the key; 0 for
	// write-behind values that are not flushed yet.
	ModRevision int64
	Source      Source
}

// Metrics are client-side counters.
type Metrics struct {
	CacheReads     uint64 // reads answered by the cache
	StoreReads     uint64 // cache-aside loads done by the client
	FallbackReads  uint64 // reads served from the store because the cache failed
	FallbackWrites uint64 // write-around writes done directly on the store
	RejectedWrites uint64 // writes refused because the cache was unavailable
	Reregistered   uint64 // namespace re-registrations (cache restarts)
}

type metrics struct {
	cacheReads, storeReads, fallbackReads, fallbackWrites, rejectedWrites, reregistered atomic.Uint64
}

// Client is a namespace-bound cache client. It is safe for concurrent use.
type Client struct {
	cfg      Config
	conn     *grpc.ClientConn
	ownsConn bool
	rpc      pb.CacheServiceClient
	store    backend.Backend
	cb       *gobreaker.CircuitBreaker[any]
	log      *slog.Logger

	regMu      sync.Mutex
	registered atomic.Bool
	epoch      atomic.Value // string

	closed  atomic.Bool
	metrics metrics
}

// DefaultDialOptions are used when Config.DialOptions is empty.
func DefaultDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		// Reconnect quickly once the cache is back instead of gRPC's default
		// backoff of up to two minutes.
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   2 * time.Second,
			},
			MinConnectTimeout: 2 * time.Second,
		}),
	}
}

// New creates a client. It tries to register the namespace right away; if
// the cache is unreachable the client starts in fallback mode and registers
// once the cache is back. Configuration conflicts are reported immediately.
func New(ctx context.Context, cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("proplyd: invalid config: %w", err)
	}

	c := &Client{cfg: cfg, store: cfg.Store, log: cfg.Logger.With("namespace", cfg.Namespace)}
	if cfg.Conn != nil {
		c.conn = cfg.Conn
	} else {
		opts := cfg.DialOptions
		if len(opts) == 0 {
			opts = DefaultDialOptions()
		}
		conn, err := grpc.NewClient(cfg.Target, opts...)
		if err != nil {
			return nil, fmt.Errorf("proplyd: dial: %w", err)
		}
		c.conn, c.ownsConn = conn, true
	}
	c.rpc = pb.NewCacheServiceClient(c.conn)

	b := cfg.Breaker
	c.cb = gobreaker.NewCircuitBreaker[any](gobreaker.Settings{
		Name:        "proplyd/" + cfg.Namespace,
		MaxRequests: b.HalfOpenRequests,
		Interval:    b.Interval,
		Timeout:     b.OpenTimeout,
		ReadyToTrip: func(cnt gobreaker.Counts) bool {
			if cnt.ConsecutiveFailures >= b.ConsecutiveFailures {
				return true
			}
			return cnt.Requests >= b.MinRequests && float64(cnt.TotalFailures)/float64(cnt.Requests) >= b.FailureRatio
		},
		IsExcluded: func(err error) bool {
			var n *neutral
			return errors.As(err, &n)
		},
		OnStateChange: func(_ string, from, to gobreaker.State) {
			c.log.Warn("cache circuit breaker state changed", "from", from.String(), "to", to.String())
			if cfg.OnStateChange != nil {
				cfg.OnStateChange(fromBreaker(from), fromBreaker(to))
			}
		},
	})

	// Eager registration surfaces configuration errors early. An unreachable
	// cache is not an error.
	_, err := invoke(c, ctx, func(ctx context.Context) (struct{}, error) { return struct{}{}, nil })
	if err != nil && classify(ctx, err) == kindCaller && ctx.Err() == nil {
		_ = c.Close()
		return nil, fmt.Errorf("proplyd: register namespace: %w", err)
	}
	return c, nil
}

func fromBreaker(s gobreaker.State) State {
	switch s {
	case gobreaker.StateClosed:
		return StateClosed
	case gobreaker.StateHalfOpen:
		return StateHalfOpen
	default:
		return StateOpen
	}
}

// State returns the circuit breaker state.
func (c *Client) State() State { return fromBreaker(c.cb.State()) }

// Epoch returns the epoch of the cache instance last registered with, or ""
// if the client never reached the cache.
func (c *Client) Epoch() string {
	s, _ := c.epoch.Load().(string)
	return s
}

// Spec returns the effective namespace configuration.
func (c *Client) Spec() protocol.Namespace { return c.cfg.Spec }

// Metrics returns client-side counters.
func (c *Client) Metrics() Metrics {
	return Metrics{
		CacheReads:     c.metrics.cacheReads.Load(),
		StoreReads:     c.metrics.storeReads.Load(),
		FallbackReads:  c.metrics.fallbackReads.Load(),
		FallbackWrites: c.metrics.fallbackWrites.Load(),
		RejectedWrites: c.metrics.rejectedWrites.Load(),
		Reregistered:   c.metrics.reregistered.Load(),
	}
}

// Close releases the connection (if the client dialed it).
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	if c.ownsConn {
		return c.conn.Close()
	}
	return nil
}

// ---------------------------------------------------------------- reads

// Get returns the value of key and whether it exists.
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	it, err := c.Lookup(ctx, key)
	return it.Value, it.Found, err
}

// Lookup is Get with details about where the answer came from.
func (c *Client) Lookup(ctx context.Context, key string) (Item, error) {
	if c.closed.Load() {
		return Item{}, ErrClosed
	}
	resp, err := invoke(c, ctx, func(ctx context.Context) (*pb.GetResponse, error) {
		return c.rpc.Get(ctx, &pb.GetRequest{Namespace: c.cfg.Namespace, Key: key})
	})
	switch classify(ctx, err) {
	case kindOK:
		if resp.GetOutcome() == pb.Outcome_OUTCOME_MISS {
			return c.loadAside(ctx, key)
		}
		c.metrics.cacheReads.Add(1)
		return Item{Value: resp.GetValue(), Found: resp.GetFound(), ModRevision: resp.GetModRevision(), Source: SourceCache}, nil
	case kindCacheDown, kindStoreDown:
		c.metrics.fallbackReads.Add(1)
		return c.readStore(ctx, key)
	default:
		return Item{}, callerError(ctx, err)
	}
}

// loadAside implements the client half of cache-aside: load from the store,
// then fill the cache (best effort; the cache rejects stale fills).
func (c *Client) loadAside(ctx context.Context, key string) (Item, error) {
	sctx, cancel := c.storeCtx(ctx)
	defer cancel()
	rec, err := c.store.Get(sctx, key)
	if err != nil {
		return Item{}, c.storeErr(ctx, err)
	}
	c.metrics.storeReads.Add(1)

	req := &pb.FillRequest{
		Namespace:    c.cfg.Namespace,
		Key:          key,
		Found:        rec.Found,
		Value:        rec.Value,
		ModRevision:  rec.ModRevision,
		ReadRevision: rec.Revision,
	}
	_, ferr := invoke(c, ctx, func(ctx context.Context) (*pb.FillResponse, error) {
		return c.rpc.Fill(ctx, req)
	})
	if ferr != nil && classify(ctx, ferr) == kindCaller && ctx.Err() == nil {
		c.log.Debug("cache-aside fill failed", "key", key, "err", ferr)
	}
	return Item{Value: rec.Value, Found: rec.Found, ModRevision: rec.ModRevision, Source: SourceStore}, nil
}

func (c *Client) readStore(ctx context.Context, key string) (Item, error) {
	sctx, cancel := c.storeCtx(ctx)
	defer cancel()
	rec, err := c.store.Get(sctx, key)
	if err != nil {
		return Item{}, c.storeErr(ctx, err)
	}
	return Item{Value: rec.Value, Found: rec.Found, ModRevision: rec.ModRevision, Source: SourceStore}, nil
}

// ---------------------------------------------------------------- writes

// WriteOption customises a write.
type WriteOption func(*writeOptions)

type writeOptions struct{ ttl time.Duration }

// WithTTL overrides the namespace TTL for this key.
func WithTTL(d time.Duration) WriteOption { return func(o *writeOptions) { o.ttl = d } }

// Set writes key according to the namespace write strategy.
func (c *Client) Set(ctx context.Context, key string, value []byte, opts ...WriteOption) error {
	if c.closed.Load() {
		return ErrClosed
	}
	var o writeOptions
	for _, opt := range opts {
		opt(&o)
	}
	req := &pb.SetRequest{Namespace: c.cfg.Namespace, Key: key, Value: value}
	if o.ttl > 0 {
		req.Ttl = durationpb.New(o.ttl)
	}
	_, err := invokeWithin(c, ctx, c.cfg.WriteTimeout, func(ctx context.Context) (*pb.SetResponse, error) {
		return c.rpc.Set(ctx, req)
	})
	return c.afterWrite(ctx, err, func(sctx context.Context) error {
		_, err := c.store.Put(sctx, key, value)
		return err
	})
}

// Delete removes key according to the namespace write strategy.
func (c *Client) Delete(ctx context.Context, key string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	_, err := invokeWithin(c, ctx, c.cfg.WriteTimeout, func(ctx context.Context) (*pb.DeleteResponse, error) {
		return c.rpc.Delete(ctx, &pb.DeleteRequest{Namespace: c.cfg.Namespace, Key: key})
	})
	return c.afterWrite(ctx, err, func(sctx context.Context) error {
		_, err := c.store.Delete(sctx, key)
		return err
	})
}

// afterWrite applies the fallback policy of the write strategy.
func (c *Client) afterWrite(ctx context.Context, err error, direct func(context.Context) error) error {
	kind := classify(ctx, err)
	switch kind {
	case kindOK:
		return nil
	case kindCaller:
		return callerError(ctx, err)
	}

	if c.cfg.Spec.Write == protocol.WriteAround {
		// Record the write no matter what; the cache's store watch (or TTL)
		// reconciles its copy.
		sctx, cancel := c.storeCtx(ctx)
		defer cancel()
		if derr := direct(sctx); derr != nil {
			return c.storeErr(ctx, derr)
		}
		c.metrics.fallbackWrites.Add(1)
		return nil
	}

	c.metrics.rejectedWrites.Add(1)
	if kind == kindStoreDown {
		return errors.Join(ErrStoreUnavailable, err)
	}
	return errors.Join(ErrCacheUnavailable, err)
}

// ---------------------------------------------------------------- management

// Invalidate drops keys from the cache (the store is untouched).
func (c *Client) Invalidate(ctx context.Context, keys ...string) error {
	_, err := invoke(c, ctx, func(ctx context.Context) (*pb.InvalidateResponse, error) {
		return c.rpc.Invalidate(ctx, &pb.InvalidateRequest{Namespace: c.cfg.Namespace, Keys: keys})
	})
	return c.mgmtErr(ctx, err)
}

// Purge drops every cached key of the namespace.
func (c *Client) Purge(ctx context.Context) error {
	_, err := invoke(c, ctx, func(ctx context.Context) (*pb.PurgeResponse, error) {
		return c.rpc.Purge(ctx, &pb.PurgeRequest{Namespace: c.cfg.Namespace})
	})
	return c.mgmtErr(ctx, err)
}

// Stats returns the server-side counters of the namespace.
func (c *Client) Stats(ctx context.Context) (*pb.StatsResponse, error) {
	resp, err := invoke(c, ctx, func(ctx context.Context) (*pb.StatsResponse, error) {
		return c.rpc.Stats(ctx, &pb.StatsRequest{Namespace: c.cfg.Namespace})
	})
	return resp, c.mgmtErr(ctx, err)
}

func (c *Client) mgmtErr(ctx context.Context, err error) error {
	switch classify(ctx, err) {
	case kindOK:
		return nil
	case kindCacheDown:
		return errors.Join(ErrCacheUnavailable, err)
	default:
		return callerError(ctx, err)
	}
}

// ---------------------------------------------------------------- plumbing

// invoke runs f against the cache through the circuit breaker, with the
// per-request timeout, (re-)registering the namespace when needed.
func invoke[T any](c *Client, ctx context.Context, f func(context.Context) (T, error)) (T, error) {
	return invokeWithin(c, ctx, c.cfg.RequestTimeout, f)
}

func invokeWithin[T any](c *Client, ctx context.Context, timeout time.Duration, f func(context.Context) (T, error)) (T, error) {
	var zero T
	if c.closed.Load() {
		return zero, ErrClosed
	}
	res, err := c.cb.Execute(func() (any, error) {
		cctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		r, err := attempt(c, cctx, f)
		if err != nil {
			if k := classify(ctx, err); k == kindCaller || k == kindStoreDown {
				return nil, &neutral{err: err}
			}
			return nil, err
		}
		return r, nil
	})
	if err != nil {
		return zero, unwrapNeutral(err)
	}
	return res.(T), nil
}

func attempt[T any](c *Client, ctx context.Context, f func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := c.ensureRegistered(ctx); err != nil {
		return zero, err
	}
	r, err := f(ctx)
	if status.Code(err) == codes.NotFound && hasReason(err, protocol.ReasonNamespaceNotFound) {
		// The cache restarted and lost the namespace: register and retry once.
		c.registered.Store(false)
		c.metrics.reregistered.Add(1)
		if err := c.ensureRegistered(ctx); err != nil {
			return zero, err
		}
		return f(ctx)
	}
	return r, err
}

func (c *Client) ensureRegistered(ctx context.Context) error {
	if c.registered.Load() {
		return nil
	}
	c.regMu.Lock()
	defer c.regMu.Unlock()
	if c.registered.Load() {
		return nil
	}
	resp, err := c.rpc.RegisterNamespace(ctx, &pb.RegisterNamespaceRequest{
		Namespace: c.cfg.Namespace,
		Config:    c.cfg.Spec.ToProto(),
	})
	if err != nil {
		return err
	}
	prev := c.Epoch()
	c.epoch.Store(resp.GetEpoch())
	c.registered.Store(true)
	if prev != "" && prev != resp.GetEpoch() {
		c.log.Info("cache restarted, namespace registered again", "epoch", resp.GetEpoch())
	}
	return nil
}

func (c *Client) storeCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.cfg.StoreTimeout)
}

func (c *Client) storeErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.Join(ErrStoreUnavailable, err)
}
