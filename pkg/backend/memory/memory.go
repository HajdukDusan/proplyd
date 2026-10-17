// Package memory is an in-memory backend.Backend with etcd semantics:
// a global revision, a change history, compaction, and prefix watches with
// progress notifications. It is meant for tests and local development.
package memory

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/c12s/proplyd/pkg/backend"
)

// Options configures a Store.
type Options struct {
	// KeepHistory, when positive, compacts the history automatically so that
	// at most this many revisions stay watchable.
	KeepHistory int
	// ProgressInterval is how often idle watches get a progress
	// notification. Default 200ms.
	ProgressInterval time.Duration
}

// Store is an in-memory etcd-like key/value store. It is safe for
// concurrent use.
type Store struct {
	opts Options

	mu      sync.Mutex
	rev     int64
	kv      map[string]entry
	history []backend.Event // revision order, all revisions > compacted
	// compacted is the newest revision that can no longer be watched.
	compacted int64
	// changed is closed and replaced on every change.
	changed chan struct{}
}

type entry struct {
	value  []byte
	modRev int64
}

var (
	_ backend.Backend = (*Store)(nil)
	_ backend.Watcher = (*Store)(nil)
)

// New returns an empty store at revision 1.
func New(opts Options) *Store {
	if opts.ProgressInterval <= 0 {
		opts.ProgressInterval = 200 * time.Millisecond
	}
	return &Store{opts: opts, rev: 1, kv: map[string]entry{}, changed: make(chan struct{})}
}

func (s *Store) Get(ctx context.Context, key string) (backend.Item, error) {
	if err := ctx.Err(); err != nil {
		return backend.Item{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.kv[key]
	if !ok {
		return backend.Item{ReadRevision: s.rev}, nil
	}
	return backend.Item{Found: true, Value: clone(e.value), ModRevision: e.modRev, ReadRevision: s.rev}, nil
}

func (s *Store) Put(ctx context.Context, key string, value []byte) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rev++
	v := clone(value)
	s.kv[key] = entry{value: v, modRev: s.rev}
	s.appendLocked(backend.Event{Key: key, Value: v, Revision: s.rev})
	return s.rev, nil
}

func (s *Store) Delete(ctx context.Context, key string) (int64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.kv[key]; !ok {
		return s.rev, false, nil
	}
	s.rev++
	delete(s.kv, key)
	s.appendLocked(backend.Event{Key: key, Deleted: true, Revision: s.rev})
	return s.rev, true, nil
}

func (s *Store) Revision(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rev, nil
}

func (s *Store) appendLocked(ev backend.Event) {
	s.history = append(s.history, ev)
	if s.opts.KeepHistory > 0 && len(s.history) > 2*s.opts.KeepHistory {
		s.compactLocked(s.rev - int64(s.opts.KeepHistory))
	}
	close(s.changed)
	s.changed = make(chan struct{})
}

// Compact discards history so that revisions below rev can no longer be
// watched, like etcd's compaction.
func (s *Store) Compact(rev int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.compactLocked(rev)
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *Store) compactLocked(rev int64) {
	rev = min(rev, s.rev)
	if rev-1 <= s.compacted {
		return
	}
	s.compacted = rev - 1
	i := sort.Search(len(s.history), func(i int) bool { return s.history[i].Revision > s.compacted })
	s.history = append([]backend.Event(nil), s.history[i:]...)
}

// Snapshot returns a copy of all keys and values.
func (s *Store) Snapshot() map[string][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]byte, len(s.kv))
	for k, e := range s.kv {
		out[k] = clone(e.value)
	}
	return out
}

// Watch implements backend.Watcher.
func (s *Store) Watch(ctx context.Context, prefix string, from int64) <-chan backend.WatchBatch {
	ch := make(chan backend.WatchBatch, 16)
	go s.watch(ctx, prefix, from, ch)
	return ch
}

func (s *Store) watch(ctx context.Context, prefix string, next int64, ch chan<- backend.WatchBatch) {
	defer close(ch)
	ticker := time.NewTicker(s.opts.ProgressInterval)
	defer ticker.Stop()
	send := func(b backend.WatchBatch) bool {
		select {
		case ch <- b:
			return true
		case <-ctx.Done():
			return false
		}
	}
	progress := true // announce the catch-up as soon as history is replayed
	for {
		s.mu.Lock()
		if next <= s.compacted {
			compacted := s.compacted + 1
			s.mu.Unlock()
			send(backend.WatchBatch{Err: &backend.CompactedError{CompactRevision: compacted}})
			return
		}
		i := sort.Search(len(s.history), func(i int) bool { return s.history[i].Revision >= next })
		var events []backend.Event
		for ; i < len(s.history) && len(events) < 256; i++ {
			if ev := s.history[i]; strings.HasPrefix(ev.Key, prefix) {
				events = append(events, ev)
			}
		}
		// Everything up to upTo has been scanned.
		upTo := s.rev
		if i < len(s.history) {
			upTo = s.history[i].Revision - 1
		}
		changed := s.changed
		s.mu.Unlock()

		next = upTo + 1
		if len(events) > 0 {
			if !send(backend.WatchBatch{Events: events, Revision: upTo}) {
				return
			}
			progress = true
			continue
		}
		if progress {
			if !send(backend.WatchBatch{Revision: upTo}) {
				return
			}
			progress = false
		}
		select {
		case <-ctx.Done():
			return
		case <-changed:
		case <-ticker.C:
			progress = true
		}
	}
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
