package testutil

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/c12s/proplyd/backend"
)

// ErrInjected is returned by a FaultyBackend while it is failing.
var ErrInjected = errors.New("testutil: injected backend failure")

// Faults is a switch shared by FaultyBackends. It lets a test cut the link
// between the cache and the store while clients can still reach the store.
type Faults struct {
	failing atomic.Bool
}

// SetFailing turns failures on or off.
func (f *Faults) SetFailing(v bool) { f.failing.Store(v) }

func (f *Faults) check() error {
	if f.failing.Load() {
		return ErrInjected
	}
	return nil
}

// Wrap returns a factory whose backends fail while f is failing.
func (f *Faults) Wrap(inner backend.Factory) backend.Factory {
	return func(prefix string) (backend.Backend, error) {
		b, err := inner(prefix)
		if err != nil {
			return nil, err
		}
		return &FaultyBackend{inner: b, faults: f}, nil
	}
}

// FaultyBackend wraps a backend with switchable failures. Watches are passed
// through untouched: only request/response traffic is affected.
type FaultyBackend struct {
	inner  backend.Backend
	faults *Faults
}

func (b *FaultyBackend) Get(ctx context.Context, key string) (backend.Record, error) {
	if err := b.faults.check(); err != nil {
		return backend.Record{}, err
	}
	return b.inner.Get(ctx, key)
}

func (b *FaultyBackend) Put(ctx context.Context, key string, value []byte) (int64, error) {
	if err := b.faults.check(); err != nil {
		return 0, err
	}
	return b.inner.Put(ctx, key, value)
}

func (b *FaultyBackend) Delete(ctx context.Context, key string) (int64, error) {
	if err := b.faults.check(); err != nil {
		return 0, err
	}
	return b.inner.Delete(ctx, key)
}

func (b *FaultyBackend) Apply(ctx context.Context, ops []backend.Op) (int64, error) {
	if err := b.faults.check(); err != nil {
		return 0, err
	}
	return b.inner.Apply(ctx, ops)
}

func (b *FaultyBackend) Close() error { return b.inner.Close() }

func (b *FaultyBackend) CurrentRevision(ctx context.Context) (int64, error) {
	if err := b.faults.check(); err != nil {
		return 0, err
	}
	return b.inner.(backend.Watcher).CurrentRevision(ctx)
}

func (b *FaultyBackend) Watch(ctx context.Context, fromRev int64) <-chan backend.WatchResponse {
	return b.inner.(backend.Watcher).Watch(ctx, fromRev)
}
