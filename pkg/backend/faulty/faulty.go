// Package faulty wraps a backend.Backend with fault injection for tests.
// Faults only affect callers that go through the wrapper, so a test can
// break the link between the cache and the store while clients still reach
// it (or the other way around).
package faulty

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/c12s/proplyd/pkg/backend"
)

// ErrInjected is the error returned while the backend is down.
var ErrInjected = errors.New("injected backend fault")

// Backend wraps a backend.Backend (and its backend.Watcher, if any).
type Backend struct {
	inner backend.Backend

	mu      sync.Mutex
	down    bool
	latency time.Duration
	paused  bool
	resumed chan struct{} // closed when watches are unpaused
	broken  chan struct{} // closed to break all current watches
}

var (
	_ backend.Backend = (*Backend)(nil)
	_ backend.Watcher = (*Backend)(nil)
)

// New wraps b.
func New(b backend.Backend) *Backend {
	r := make(chan struct{})
	close(r)
	return &Backend{inner: b, resumed: r, broken: make(chan struct{})}
}

// SetDown makes every call fail with ErrInjected (wrapped as unavailable)
// and breaks current watches.
func (f *Backend) SetDown(down bool) {
	f.mu.Lock()
	f.down = down
	f.mu.Unlock()
	if down {
		f.BreakWatches()
	}
}

// SetLatency delays every call by d.
func (f *Backend) SetLatency(d time.Duration) {
	f.mu.Lock()
	f.latency = d
	f.mu.Unlock()
}

// BreakWatches terminates every active watch with an error, like a dropped
// connection.
func (f *Backend) BreakWatches() {
	f.mu.Lock()
	close(f.broken)
	f.broken = make(chan struct{})
	f.mu.Unlock()
}

// PauseWatches stops (or resumes) delivery of watch batches, like a lagging
// change stream.
func (f *Backend) PauseWatches(paused bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if paused == f.paused {
		return
	}
	f.paused = paused
	if paused {
		f.resumed = make(chan struct{})
	} else {
		close(f.resumed)
	}
}

func (f *Backend) before(ctx context.Context) error {
	f.mu.Lock()
	down, latency := f.down, f.latency
	f.mu.Unlock()
	if latency > 0 {
		t := time.NewTimer(latency)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if down {
		return backend.Unavailable(ErrInjected)
	}
	return nil
}

func (f *Backend) Get(ctx context.Context, key string) (backend.Item, error) {
	if err := f.before(ctx); err != nil {
		return backend.Item{}, err
	}
	return f.inner.Get(ctx, key)
}

func (f *Backend) Put(ctx context.Context, key string, value []byte) (int64, error) {
	if err := f.before(ctx); err != nil {
		return 0, err
	}
	return f.inner.Put(ctx, key, value)
}

func (f *Backend) Delete(ctx context.Context, key string) (int64, bool, error) {
	if err := f.before(ctx); err != nil {
		return 0, false, err
	}
	return f.inner.Delete(ctx, key)
}

// Identity passes through to the wrapped backend, if it has one.
func (f *Backend) Identity(ctx context.Context) (string, error) {
	id, ok := f.inner.(backend.Identifier)
	if !ok {
		return "", nil
	}
	if err := f.before(ctx); err != nil {
		return "", err
	}
	return id.Identity(ctx)
}

func (f *Backend) Revision(ctx context.Context) (int64, error) {
	if err := f.before(ctx); err != nil {
		return 0, err
	}
	return f.inner.Revision(ctx)
}

// Watch implements backend.Watcher. It panics if the wrapped backend has no
// change stream.
func (f *Backend) Watch(ctx context.Context, prefix string, from int64) <-chan backend.WatchBatch {
	out := make(chan backend.WatchBatch)
	f.mu.Lock()
	down, broken := f.down, f.broken
	f.mu.Unlock()
	go func() {
		defer close(out)
		if down {
			select {
			case out <- backend.WatchBatch{Err: backend.Unavailable(ErrInjected)}:
			case <-ctx.Done():
			}
			return
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		in := f.inner.(backend.Watcher).Watch(ctx, prefix, from)
		// A break ends the watch at once: nothing after a dropped batch may
		// be delivered, or the consumer would miss events silently.
		fail := func() {
			select {
			case out <- backend.WatchBatch{Err: backend.Unavailable(errors.New("injected watch break"))}:
			case <-ctx.Done():
			}
		}
		for {
			var b backend.WatchBatch
			var ok bool
			select {
			case b, ok = <-in:
				if !ok {
					return
				}
			case <-broken:
				fail()
				return
			case <-ctx.Done():
				return
			}
			f.mu.Lock()
			resumed := f.resumed
			f.mu.Unlock()
			select {
			case <-resumed:
			case <-broken:
				fail()
				return
			case <-ctx.Done():
				return
			}
			select {
			case <-broken:
				fail()
				return
			default:
			}
			select {
			case out <- b:
			case <-broken:
				fail()
				return
			case <-ctx.Done():
				return
			}
			if b.Err != nil {
				return
			}
		}
	}()
	return out
}
