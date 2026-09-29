// Package memory implements an in-process, etcd-like backend. It keeps a
// single revision counter and a change history so that watches, revisions and
// compaction behave like etcd. It is meant for tests and local development and
// supports fault injection (outages, latency, compaction, broken watches).
package memory

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/c12s/proplyd/backend"
)

// ErrUnavailable is returned by every operation while the store is down.
var ErrUnavailable = errors.New("memory: store unavailable")

type kv struct {
	value       []byte
	modRevision int64
}

// Store is the shared "database". Use Backend to obtain prefix-scoped views.
type Store struct {
	mu        sync.Mutex
	rev       int64
	data      map[string]kv
	history   []backend.Event
	compacted int64
	watchers  map[*watcher]struct{}
	down      bool
	paused    bool
	latency   time.Duration

	// counters, guarded by mu
	reads, writes int64
}

// NewStore returns an empty store at revision 1, like a fresh etcd cluster.
func NewStore() *Store {
	return &Store{rev: 1, data: map[string]kv{}, watchers: map[*watcher]struct{}{}}
}

// Backend returns a view of the store scoped to prefix.
func (s *Store) Backend(prefix string) *Backend {
	return &Backend{s: s, prefix: prefix}
}

// Factory returns a backend.Factory creating prefix views of s.
func (s *Store) Factory() backend.Factory {
	return func(prefix string) (backend.Backend, error) { return s.Backend(prefix), nil }
}

// SetDown simulates an outage: every operation fails until SetDown(false).
// Taking the store down also breaks all active watches.
func (s *Store) SetDown(down bool) {
	s.mu.Lock()
	s.down = down
	var ws []*watcher
	if down {
		for w := range s.watchers {
			ws = append(ws, w)
		}
	}
	s.mu.Unlock()
	for _, w := range ws {
		w.fail(ErrUnavailable)
	}
}

// SetLatency adds an artificial delay to every operation.
func (s *Store) SetLatency(d time.Duration) {
	s.mu.Lock()
	s.latency = d
	s.mu.Unlock()
}

// Compact discards all history up to the current revision. Live watches are
// unaffected (they already received every event); new watches starting at or
// below the compacted revision fail with backend.ErrCompacted.
func (s *Store) Compact() {
	s.mu.Lock()
	s.compacted = s.rev
	s.history = nil
	s.mu.Unlock()
}

// PauseWatches breaks every active watch and makes new watches fail until
// PauseWatches(false), while reads and writes keep working. It simulates the
// cache losing its watch stream while other clients still write.
func (s *Store) PauseWatches(paused bool) {
	s.mu.Lock()
	s.paused = paused
	s.mu.Unlock()
	if paused {
		s.BreakWatches(ErrUnavailable)
	}
}

// BreakWatches terminates every active watch with err, simulating a lost
// connection between the cache and the store.
func (s *Store) BreakWatches(err error) {
	s.mu.Lock()
	ws := make([]*watcher, 0, len(s.watchers))
	for w := range s.watchers {
		ws = append(ws, w)
	}
	s.mu.Unlock()
	for _, w := range ws {
		w.fail(err)
	}
}

// Peek reads an absolute key directly, bypassing faults. For assertions.
func (s *Store) Peek(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v.value, ok
}

// Counts returns the number of reads and writes served.
func (s *Store) Counts() (reads, writes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads, s.writes
}

// Revision returns the current revision.
func (s *Store) Revision() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rev
}

func (s *Store) enter(ctx context.Context) error {
	s.mu.Lock()
	lat := s.latency
	s.mu.Unlock()
	if lat > 0 {
		t := time.NewTimer(lat)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// apply must be called with mu held.
func (s *Store) apply(ops []backend.Op) int64 {
	changed := false
	next := s.rev + 1
	var evs []backend.Event
	for _, op := range ops {
		if op.Delete {
			if _, ok := s.data[op.Key]; !ok {
				continue
			}
			delete(s.data, op.Key)
			evs = append(evs, backend.Event{Type: backend.EventDelete, Key: op.Key, ModRevision: next})
		} else {
			val := append([]byte(nil), op.Value...)
			s.data[op.Key] = kv{value: val, modRevision: next}
			evs = append(evs, backend.Event{Type: backend.EventPut, Key: op.Key, Value: val, ModRevision: next})
		}
		changed = true
	}
	if !changed {
		return s.rev
	}
	s.rev = next
	s.writes++
	s.history = append(s.history, evs...)
	for w := range s.watchers {
		w.push(evs)
	}
	return s.rev
}

// Backend is a prefix-scoped view of a Store.
type Backend struct {
	s      *Store
	prefix string
}

var (
	_ backend.Backend = (*Backend)(nil)
	_ backend.Watcher = (*Backend)(nil)
)

func (b *Backend) Get(ctx context.Context, key string) (backend.Record, error) {
	if err := b.s.enter(ctx); err != nil {
		return backend.Record{}, err
	}
	b.s.mu.Lock()
	defer b.s.mu.Unlock()
	if b.s.down {
		return backend.Record{}, ErrUnavailable
	}
	b.s.reads++
	rec := backend.Record{Revision: b.s.rev}
	if v, ok := b.s.data[b.prefix+key]; ok {
		rec.Found = true
		rec.Value = append([]byte(nil), v.value...)
		rec.ModRevision = v.modRevision
	}
	return rec, nil
}

func (b *Backend) Put(ctx context.Context, key string, value []byte) (int64, error) {
	return b.Apply(ctx, []backend.Op{{Key: key, Value: value}})
}

func (b *Backend) Delete(ctx context.Context, key string) (int64, error) {
	return b.Apply(ctx, []backend.Op{{Key: key, Delete: true}})
}

func (b *Backend) Apply(ctx context.Context, ops []backend.Op) (int64, error) {
	if err := b.s.enter(ctx); err != nil {
		return 0, err
	}
	abs := make([]backend.Op, len(ops))
	for i, op := range ops {
		abs[i] = backend.Op{Key: b.prefix + op.Key, Value: op.Value, Delete: op.Delete}
	}
	b.s.mu.Lock()
	defer b.s.mu.Unlock()
	if b.s.down {
		return 0, ErrUnavailable
	}
	return b.s.apply(abs), nil
}

func (b *Backend) CurrentRevision(ctx context.Context) (int64, error) {
	if err := b.s.enter(ctx); err != nil {
		return 0, err
	}
	b.s.mu.Lock()
	defer b.s.mu.Unlock()
	if b.s.down {
		return 0, ErrUnavailable
	}
	return b.s.rev, nil
}

func (b *Backend) Watch(ctx context.Context, fromRev int64) <-chan backend.WatchResponse {
	out := make(chan backend.WatchResponse, 16)
	w := &watcher{prefix: b.prefix, next: fromRev, signal: make(chan struct{}, 1)}

	b.s.mu.Lock()
	switch {
	case b.s.down || b.s.paused:
		w.err = ErrUnavailable
	case fromRev <= b.s.compacted:
		w.err = backend.ErrCompacted
	default:
		// Replay history the watcher asked for.
		var replay []backend.Event
		for _, ev := range b.s.history {
			if ev.ModRevision >= fromRev {
				replay = append(replay, ev)
			}
		}
		w.push(replay)
		b.s.watchers[w] = struct{}{}
	}
	b.s.mu.Unlock()

	go func() {
		defer close(out)
		defer func() {
			b.s.mu.Lock()
			delete(b.s.watchers, w)
			b.s.mu.Unlock()
		}()
		for {
			evs, err := w.drain()
			if len(evs) > 0 {
				select {
				case out <- backend.WatchResponse{Events: evs}:
				case <-ctx.Done():
					return
				}
			}
			if err != nil {
				select {
				case out <- backend.WatchResponse{Err: err}:
				case <-ctx.Done():
				}
				return
			}
			if len(evs) > 0 {
				continue
			}
			select {
			case <-w.signal:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func (b *Backend) Close() error { return nil }

// watcher buffers events without ever blocking writers.
type watcher struct {
	prefix string

	mu     sync.Mutex
	next   int64
	queue  []backend.Event
	err    error
	signal chan struct{}
}

func (w *watcher) push(evs []backend.Event) {
	w.mu.Lock()
	if w.err != nil {
		// A broken watch must not see anything after the break.
		w.mu.Unlock()
		return
	}
	for _, ev := range evs {
		if ev.ModRevision < w.next || !strings.HasPrefix(ev.Key, w.prefix) {
			continue
		}
		e := ev
		e.Key = strings.TrimPrefix(e.Key, w.prefix)
		w.queue = append(w.queue, e)
	}
	w.mu.Unlock()
	w.notify()
}

func (w *watcher) fail(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.mu.Unlock()
	w.notify()
}

func (w *watcher) notify() {
	select {
	case w.signal <- struct{}{}:
	default:
	}
}

func (w *watcher) drain() ([]backend.Event, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	evs := w.queue
	w.queue = nil
	if len(evs) > 0 {
		w.next = evs[len(evs)-1].ModRevision + 1
		return evs, nil
	}
	return nil, w.err
}
