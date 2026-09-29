package client

import (
	"errors"
	"log/slog"
	"time"

	"github.com/c12s/proplyd/backend"
	"github.com/c12s/proplyd/protocol"
	"google.golang.org/grpc"
)

// Config configures a Client.
type Config struct {
	// Target is the gRPC target of the cache, e.g. "dns:///proplyd:7070".
	// Ignored when Conn is set.
	Target string
	// Conn is an existing connection to use instead of dialing Target. It is
	// not closed by Client.Close.
	Conn *grpc.ClientConn
	// DialOptions replace the default dial options (insecure transport,
	// keepalive, fast reconnect backoff).
	DialOptions []grpc.DialOption

	// Namespace name and its strategy configuration. All clients of a
	// namespace must use the same Spec.
	Namespace string
	Spec      protocol.Namespace

	// Store gives the client direct access to the backing store. It is used
	// for cache-aside loads and whenever the cache is unavailable (reads, and
	// write-around writes). It must be scoped to Spec.KeyPrefix. It is owned
	// by the caller.
	Store backend.Backend

	// RequestTimeout bounds reads and management calls to the cache. A cache
	// slower than this is treated as unavailable and the read falls back to
	// the store. Default 500ms.
	RequestTimeout time.Duration
	// WriteTimeout bounds Set/Delete calls. Write-through and write-around
	// writes include a store write, so this must exceed the store's p99
	// write latency. Default 2s.
	WriteTimeout time.Duration
	// StoreTimeout bounds direct store calls when the caller's context has no
	// deadline. Default 5s.
	StoreTimeout time.Duration

	Breaker BreakerConfig
	Logger  *slog.Logger
	// OnStateChange is called when the circuit breaker changes state.
	OnStateChange func(from, to State)
}

// BreakerConfig configures the circuit breaker guarding cache calls. Only
// cache failures count (unreachable, timeouts, internal errors); store
// failures reported by the cache and caller errors do not.
type BreakerConfig struct {
	// Trip after this many consecutive failures. Default 5.
	ConsecutiveFailures uint32
	// Or trip when the failure ratio within Interval reaches FailureRatio
	// after at least MinRequests requests. Defaults 0.5 and 20.
	FailureRatio float64
	MinRequests  uint32
	// Interval is the rolling window for counting in the closed state.
	// Default 10s.
	Interval time.Duration
	// OpenTimeout is how long the breaker stays open before letting probe
	// requests through. Default 2s.
	OpenTimeout time.Duration
	// HalfOpenRequests is the number of probes allowed in the half-open
	// state; that many consecutive successes close the breaker. Default 3.
	HalfOpenRequests uint32
}

func (c Config) withDefaults() Config {
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 500 * time.Millisecond
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 2 * time.Second
	}
	if c.StoreTimeout <= 0 {
		c.StoreTimeout = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	b := &c.Breaker
	if b.ConsecutiveFailures == 0 {
		b.ConsecutiveFailures = 5
	}
	if b.FailureRatio == 0 {
		b.FailureRatio = 0.5
	}
	if b.MinRequests == 0 {
		b.MinRequests = 20
	}
	if b.Interval <= 0 {
		b.Interval = 10 * time.Second
	}
	if b.OpenTimeout <= 0 {
		b.OpenTimeout = 2 * time.Second
	}
	if b.HalfOpenRequests == 0 {
		b.HalfOpenRequests = 3
	}
	c.Spec = c.Spec.WithDefaults()
	return c
}

func (c Config) validate() error {
	var errs []error
	if c.Conn == nil && c.Target == "" {
		errs = append(errs, errors.New("target or conn is required"))
	}
	if c.Namespace == "" {
		errs = append(errs, errors.New("namespace is required"))
	}
	if c.Store == nil {
		errs = append(errs, errors.New("store is required (used for fallback)"))
	}
	if err := c.Spec.Validate(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
