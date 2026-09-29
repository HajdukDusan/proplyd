package cache

import (
	"bytes"
	"context"
	"hash/maphash"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c12s/proplyd/backend"
	"github.com/c12s/proplyd/protocol"
	"github.com/c12s/proplyd/store"
	"golang.org/x/sync/singleflight"
)

// stripeCount is the number of per-key lock stripes. Every mutation of a
// key's cache state (install, invalidate, write-behind bookkeeping, watch
// updates) happens under its stripe lock, which makes "compare revision then
// set" atomic without a lock per key.
const stripeCount = 256

type stripe struct {
	mu sync.Mutex
	// dirty holds unflushed write-behind entries (nil for other strategies).
	// It is the source of truth for pending writes: the store copy may be
	// evicted at any time, the dirty entry is kept until it is flushed.
	dirty map[string]*dirtyEntry
}

type dirtyEntry struct {
	value   []byte
	deleted bool
	seq     uint64
	ttl     time.Duration
}

// Outcome tells where a Get result came from.
type Outcome int

const (
	OutcomeHit Outcome = iota + 1
	OutcomeLoaded
	OutcomeMiss
)

// Result is the result of a Get.
type Result struct {
	Outcome     Outcome
	Found       bool
	Value       []byte
	ModRevision int64
}

// Stats are per-namespace counters.
type Stats struct {
	Entries       int64
	Hits          uint64
	Misses        uint64
	Loads         uint64
	LoadErrors    uint64
	FillsAccepted uint64
	FillsRejected uint64
	PendingWrites int64
	FlushedWrites uint64
	FlushErrors   uint64
	WatchEvents   uint64
	WatchResyncs  uint64
	Refreshes     uint64
}

type counters struct {
	hits, misses, loads, loadErrors      atomic.Uint64
	fillsAccepted, fillsRejected         atomic.Uint64
	flushed, flushErrors                 atomic.Uint64
	watchEvents, watchResyncs, refreshes atomic.Uint64
}

type namespace struct {
	name    string
	cfg     protocol.Namespace
	opts    *Options
	log     *slog.Logger
	store   store.Store
	be      backend.Backend
	watcher backend.Watcher

	stripes [stripeCount]stripe
	seed    maphash.Seed
	changes *changeLog
	loads   singleflight.Group

	refreshSem chan struct{}

	// write-behind
	seq         atomic.Uint64
	pending     atomic.Int64
	kick        chan struct{}
	flushMu     sync.Mutex
	flushCursor int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	// asyncMu guards starting background goroutines against Close.
	asyncMu sync.RWMutex
	closed  bool

	stats counters
}

func newNamespace(name string, cfg protocol.Namespace, st store.Store, be backend.Backend, opts *Options) (*namespace, error) {
	n := &namespace{
		name:       name,
		cfg:        cfg,
		opts:       opts,
		log:        opts.Logger.With("namespace", name),
		store:      st,
		be:         be,
		seed:       maphash.MakeSeed(),
		changes:    newChangeLog(opts.ChangeLogSize),
		refreshSem: make(chan struct{}, opts.MaxConcurrentRefreshes),
		kick:       make(chan struct{}, 1),
	}
	n.ctx, n.cancel = context.WithCancel(context.Background())

	// All state must be initialised before any background goroutine starts.
	if cfg.Write == protocol.WriteBehind {
		for i := range n.stripes {
			n.stripes[i].dirty = make(map[string]*dirtyEntry)
		}
	}
	if cfg.Sync == protocol.SyncWatch {
		w, ok := be.(backend.Watcher)
		if !ok {
			n.cancel()
			return nil, invalid("sync mode WATCH is not supported by the backend")
		}
		n.watcher = w
		// Nothing may be installed until the watch knows where it starts.
		n.changes.block()
		n.wg.Add(1)
		go n.runWatch()
	}

	if cfg.Write == protocol.WriteBehind {
		n.wg.Add(1)
		go n.runFlusher()
	}
	return n, nil
}

func (n *namespace) stripe(key string) *stripe {
	return &n.stripes[maphash.String(n.seed, key)%stripeCount]
}

func (n *namespace) ttl(override time.Duration) time.Duration {
	if override > 0 {
		return override
	}
	return n.cfg.TTL
}

func (n *namespace) backendErr(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return &BackendError{Err: err}
}

// ---------------------------------------------------------------- reads

func (n *namespace) get(ctx context.Context, key string) (Result, error) {
	if n.cfg.Write == protocol.WriteBehind {
		st := n.stripe(key)
		st.mu.Lock()
		d := st.dirty[key]
		st.mu.Unlock()
		if d != nil {
			n.stats.hits.Add(1)
			return Result{Outcome: OutcomeHit, Found: !d.deleted, Value: d.value}, nil
		}
	}

	if raw, ok := n.store.Get(key); ok {
		if e, ok := decodeEntry(raw); ok {
			n.stats.hits.Add(1)
			n.maybeRefresh(key, e)
			res := Result{Outcome: OutcomeHit, Found: e.found}
			if e.found {
				res.Value = e.value
				if !e.pending {
					res.ModRevision = e.version
				}
			}
			return res, nil
		}
	}

	n.stats.misses.Add(1)
	if n.cfg.Read == protocol.CacheAside {
		return Result{Outcome: OutcomeMiss}, nil
	}

	rec, err := n.load(ctx, key)
	if err != nil {
		return Result{}, err
	}
	return Result{Outcome: OutcomeLoaded, Found: rec.Found, Value: rec.Value, ModRevision: rec.ModRevision}, nil
}

// load reads key from the backend and installs it. Concurrent loads of the
// same key share one backend call. The shared call is detached from the
// caller's context so one impatient caller cannot fail the others.
func (n *namespace) load(ctx context.Context, key string) (backend.Record, error) {
	ch := n.loads.DoChan(key, func() (any, error) {
		lctx, cancel := context.WithTimeout(n.ctx, n.opts.LoadTimeout)
		defer cancel()
		rec, err := n.be.Get(lctx, key)
		if err != nil {
			n.stats.loadErrors.Add(1)
			return nil, &BackendError{Err: err}
		}
		n.stats.loads.Add(1)
		n.install(key, recordEntry(rec), rec.Revision, 0)
		return rec, nil
	})
	select {
	case r := <-ch:
		if r.Err != nil {
			return backend.Record{}, r.Err
		}
		return r.Val.(backend.Record), nil
	case <-ctx.Done():
		return backend.Record{}, ctx.Err()
	}
}

func (n *namespace) maybeRefresh(key string, e entry) {
	if n.cfg.Read != protocol.RefreshAhead || e.pending || e.age(time.Now()) < n.cfg.RefreshAfter {
		return
	}
	select {
	case n.refreshSem <- struct{}{}:
	default:
		return // enough refreshes in flight; a later read will retry
	}
	started := n.goAsync(func() {
		defer func() { <-n.refreshSem }()
		if _, err := n.load(n.ctx, key); err == nil {
			n.stats.refreshes.Add(1)
		}
	})
	if !started {
		<-n.refreshSem
	}
}

// fill installs a record read by a client (cache-aside).
func (n *namespace) fill(key string, rec backend.Record, ttl time.Duration) bool {
	if rec.Found {
		rec.Value = bytes.Clone(rec.Value)
	}
	ok := n.install(key, recordEntry(rec), rec.Revision, ttl)
	if ok {
		n.stats.fillsAccepted.Add(1)
	} else {
		n.stats.fillsRejected.Add(1)
	}
	return ok
}

func recordEntry(rec backend.Record) entry {
	if rec.Found {
		return entry{found: true, version: rec.ModRevision, value: rec.Value}
	}
	return entry{version: rec.Revision}
}

// install stores e if it is not older than what the cache knows. readRev is
// the store revision the data was observed at.
func (n *namespace) install(key string, e entry, readRev int64, ttl time.Duration) bool {
	st := n.stripe(key)
	st.mu.Lock()
	defer st.mu.Unlock()
	return n.installLocked(st, key, e, readRev, ttl)
}

func (n *namespace) installLocked(st *stripe, key string, e entry, readRev int64, ttl time.Duration) bool {
	if st.dirty[key] != nil {
		return false // a pending write-behind value is newer by definition
	}
	if !n.changes.admits(key, readRev) {
		return false
	}
	if raw, ok := n.store.Peek(key); ok {
		if cur, ok := decodeEntry(raw); ok && !cur.pending && cur.version > e.version {
			return false
		}
	}
	if !e.found && !n.cfg.CacheMisses {
		n.store.Delete(key)
		return true
	}
	e.pending = false
	e.storedAt = time.Now().UnixNano()
	n.store.Set(key, encodeEntry(e), n.ttl(ttl))
	return true
}

// ---------------------------------------------------------------- writes

func (n *namespace) set(ctx context.Context, key string, value []byte, ttl time.Duration) (int64, error) {
	if len(value) > n.cfg.MaxValueBytes {
		return 0, ErrValueTooLarge
	}
	switch n.cfg.Write {
	case protocol.WriteBehind:
		return 0, n.writeBehind(key, value, false, ttl)
	case protocol.WriteAround:
		rev, err := n.be.Put(ctx, key, value)
		if err != nil {
			return 0, n.backendErr(ctx, err)
		}
		n.invalidateAt(key, rev)
		return rev, nil
	default: // WriteThrough
		rev, err := n.be.Put(ctx, key, value)
		if err != nil {
			return 0, n.backendErr(ctx, err)
		}
		n.applyOwnWrite(key, entry{found: true, version: rev, value: bytes.Clone(value)}, rev, ttl)
		return rev, nil
	}
}

func (n *namespace) del(ctx context.Context, key string) (int64, error) {
	switch n.cfg.Write {
	case protocol.WriteBehind:
		return 0, n.writeBehind(key, nil, true, 0)
	case protocol.WriteAround:
		rev, err := n.be.Delete(ctx, key)
		if err != nil {
			return 0, n.backendErr(ctx, err)
		}
		n.invalidateAt(key, rev)
		return rev, nil
	default:
		rev, err := n.be.Delete(ctx, key)
		if err != nil {
			return 0, n.backendErr(ctx, err)
		}
		n.applyOwnWrite(key, entry{version: rev}, rev, 0)
		return rev, nil
	}
}

// applyOwnWrite installs the result of a write the cache itself performed at
// rev. If a newer change is already known the entry is not installed, but an
// older cached value must not survive either.
func (n *namespace) applyOwnWrite(key string, e entry, rev int64, ttl time.Duration) {
	st := n.stripe(key)
	st.mu.Lock()
	defer st.mu.Unlock()
	if !n.installLocked(st, key, e, rev, ttl) {
		if raw, ok := n.store.Peek(key); ok {
			if cur, ok := decodeEntry(raw); ok && cur.version < rev {
				n.store.Delete(key)
			}
		}
	}
	n.changes.record(key, rev)
}

// invalidateAt drops key after a change at rev that the cache made itself.
func (n *namespace) invalidateAt(key string, rev int64) {
	st := n.stripe(key)
	st.mu.Lock()
	n.store.Delete(key)
	n.changes.record(key, rev)
	st.mu.Unlock()
}

func (n *namespace) invalidate(keys []string) {
	for _, key := range keys {
		st := n.stripe(key)
		st.mu.Lock()
		if st.dirty[key] == nil {
			n.store.Delete(key)
		}
		st.mu.Unlock()
	}
}

func (n *namespace) purge() {
	n.lockAll()
	defer n.unlockAll()
	n.store.Purge()
	n.restorePendingLocked()
}

// restorePendingLocked re-inserts pending write-behind values after a purge so
// they stay readable. Requires all stripe locks.
func (n *namespace) restorePendingLocked() {
	now := time.Now().UnixNano()
	for i := range n.stripes {
		for key, d := range n.stripes[i].dirty {
			n.store.Set(key, encodeEntry(entry{found: !d.deleted, pending: true, storedAt: now, value: d.value}), n.ttl(d.ttl))
		}
	}
}

func (n *namespace) lockAll() {
	for i := range n.stripes {
		n.stripes[i].mu.Lock()
	}
}

func (n *namespace) unlockAll() {
	for i := range n.stripes {
		n.stripes[i].mu.Unlock()
	}
}

// ---------------------------------------------------------------- lifecycle

// goAsync runs f in a tracked goroutine unless the namespace is closing.
func (n *namespace) goAsync(f func()) bool {
	n.asyncMu.RLock()
	defer n.asyncMu.RUnlock()
	if n.closed {
		return false
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		f()
	}()
	return true
}

func (n *namespace) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-n.ctx.Done():
		return false
	}
}

// close flushes pending write-behind entries (bounded by ctx) and stops all
// background work.
func (n *namespace) close(ctx context.Context) error {
	n.asyncMu.Lock()
	n.closed = true
	n.asyncMu.Unlock()

	var err error
	if n.cfg.Write == protocol.WriteBehind {
		err = n.drain(ctx)
	}
	n.cancel()
	n.wg.Wait()
	if cerr := n.store.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if cerr := n.be.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

func (n *namespace) snapshot() Stats {
	return Stats{
		Entries:       int64(n.store.Len()),
		Hits:          n.stats.hits.Load(),
		Misses:        n.stats.misses.Load(),
		Loads:         n.stats.loads.Load(),
		LoadErrors:    n.stats.loadErrors.Load(),
		FillsAccepted: n.stats.fillsAccepted.Load(),
		FillsRejected: n.stats.fillsRejected.Load(),
		PendingWrites: n.pending.Load(),
		FlushedWrites: n.stats.flushed.Load(),
		FlushErrors:   n.stats.flushErrors.Load(),
		WatchEvents:   n.stats.watchEvents.Load(),
		WatchResyncs:  n.stats.watchResyncs.Load(),
		Refreshes:     n.stats.refreshes.Load(),
	}
}
