// Package tests contains the system-level tests of proplyd: many clients,
// several namespaces with different strategies, one central cache, and
// (in the chaos variant) continuous fault injection.
package tests

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c12s/proplyd/backend/memory"
	"github.com/c12s/proplyd/client"
	"github.com/c12s/proplyd/internal/testutil"
	"github.com/c12s/proplyd/protocol"
)

// scenario is one namespace under test.
type scenario struct {
	name string
	spec protocol.Namespace
}

func scenarios() []scenario {
	return []scenario{
		{"wt-readthrough", protocol.Namespace{Write: protocol.WriteThrough, Read: protocol.ReadThrough, Eviction: protocol.EvictLRU, Capacity: 60}},
		{"wa-readthrough", protocol.Namespace{Write: protocol.WriteAround, Read: protocol.ReadThrough, Eviction: protocol.EvictSIEVE, Capacity: 60}},
		{"wb-readthrough", protocol.Namespace{Write: protocol.WriteBehind, Read: protocol.ReadThrough, Eviction: protocol.EvictLFU, FlushInterval: 20 * time.Millisecond}},
		{"wt-cacheaside", protocol.Namespace{Write: protocol.WriteThrough, Read: protocol.CacheAside, Eviction: protocol.EvictTwoQueue}},
		{"wa-refreshahead", protocol.Namespace{Write: protocol.WriteAround, Read: protocol.RefreshAhead, TTL: time.Minute, RefreshAfter: 100 * time.Millisecond}},
		{"wb-cacheaside", protocol.Namespace{Write: protocol.WriteBehind, Read: protocol.CacheAside, Eviction: protocol.EvictFIFO, FlushInterval: 5 * time.Millisecond, CacheMisses: true}},
	}
}

const deleted = "<deleted>"

// keyHistory is kept by the single owner (writer) of a key.
type keyHistory struct {
	acked     string   // last acknowledged value (or deleted)
	uncertain []string // values of failed writes after acked (may have been applied)
	all       []string // every value ever attempted
}

type worker struct {
	id      int
	ns      scenario
	c       *client.Client
	keys    []string // keys this worker owns
	allKeys []string
	history map[string]*keyHistory
	rng     *rand.Rand
}

type run struct {
	t       *testing.T
	db      *memory.Store
	faults  *testutil.Faults
	node    *testutil.CacheNode
	workers []*worker
	chaos   bool

	ops, reads, writes, tolerated atomic.Uint64
	errMu                         sync.Mutex
	errCounts                     map[string]int
}

func prefixOf(ns string) string { return "/stress/" + ns + "/" }

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func newRun(t *testing.T, clientsPerNS, keysPerNS int, chaos bool) *run {
	db := memory.NewStore()
	faults := &testutil.Faults{}
	node := testutil.StartCacheNode(t, faults.Wrap(db.Factory()))
	r := &run{t: t, db: db, faults: faults, node: node, chaos: chaos, errCounts: map[string]int{}}

	id := 0
	for _, sc := range scenarios() {
		sc.spec.KeyPrefix = prefixOf(sc.name)
		var all []string
		for k := range keysPerNS {
			all = append(all, fmt.Sprintf("k%03d", k))
		}
		for w := range clientsPerNS {
			wk := &worker{
				id:      id,
				ns:      sc,
				allKeys: all,
				history: map[string]*keyHistory{},
				rng:     rand.New(rand.NewPCG(uint64(id), 42)),
			}
			// Each client gets its own gRPC connection, like separate services.
			wk.c = testutil.NewClient(t, node.Addr(), sc.name, sc.spec, db.Backend(sc.spec.KeyPrefix))
			for k := w; k < keysPerNS; k += clientsPerNS {
				key := all[k]
				wk.keys = append(wk.keys, key)
				wk.history[key] = &keyHistory{acked: deleted}
			}
			r.workers = append(r.workers, wk)
			id++
		}
	}
	return r
}

func (r *run) tolerate(err error) bool {
	if !r.chaos {
		return false
	}
	switch {
	case errors.Is(err, client.ErrCacheUnavailable),
		errors.Is(err, client.ErrStoreUnavailable),
		errors.Is(err, client.ErrBackpressure):
		r.tolerated.Add(1)
		r.errMu.Lock()
		switch {
		case errors.Is(err, client.ErrCacheUnavailable):
			r.errCounts["cache unavailable"]++
		case errors.Is(err, client.ErrStoreUnavailable):
			r.errCounts["store unavailable"]++
		default:
			r.errCounts["backpressure"]++
		}
		r.errMu.Unlock()
		return true
	}
	return false
}

// drive runs every worker until stop is closed.
func (r *run) drive(stop <-chan struct{}) {
	var wg sync.WaitGroup
	for _, w := range r.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				r.step(w)
			}
		}()
	}
	wg.Wait()
}

func (r *run) step(w *worker) {
	t := r.t
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.ops.Add(1)

	p := w.rng.IntN(100)
	switch {
	case p < 70: // read, skewed towards low keys
		r.reads.Add(1)
		idx := int(float64(len(w.allKeys)) * w.rng.Float64() * w.rng.Float64())
		key := w.allKeys[idx]
		it, err := w.c.Lookup(ctx, key)
		if err != nil {
			if !r.tolerate(err) {
				t.Errorf("[%s c%d] read %s: %v", w.ns.name, w.id, key, err)
			}
			return
		}
		if h, own := w.history[key]; own && !r.chaos {
			// Single writer per key: the owner must always read its last write.
			got := deleted
			if it.Found {
				got = string(it.Value)
			}
			if got != h.acked {
				t.Errorf("[%s c%d] read-your-writes violated on %s: got %q (from %s) want %q", w.ns.name, w.id, key, got, it.Source, h.acked)
			}
		}
	default: // write or delete an owned key
		r.writes.Add(1)
		key := w.keys[w.rng.IntN(len(w.keys))]
		h := w.history[key]
		var val string
		var err error
		if p < 95 {
			val = fmt.Sprintf("c%d-%d", w.id, len(h.all))
			err = w.c.Set(ctx, key, []byte(val))
		} else {
			val = deleted
			err = w.c.Delete(ctx, key)
		}
		h.all = append(h.all, val)
		if err != nil {
			if !r.tolerate(err) {
				t.Errorf("[%s c%d] write %s: %v", w.ns.name, w.id, key, err)
			}
			h.uncertain = append(h.uncertain, val)
			return
		}
		h.acked, h.uncertain = val, nil
	}
}

// settle brings every component back to health and waits until every client
// is back on the cache.
func (r *run) settle() {
	t := r.t
	r.db.SetDown(false)
	r.db.SetLatency(0)
	r.db.PauseWatches(false)
	r.faults.SetFailing(false)
	r.node.Start()
	r.node.Proxy.SetMode(testutil.Forward)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// Clients first: their calls re-register namespaces lost in a crash.
	for _, w := range r.workers {
		testutil.Eventually(t, 10*time.Second, func() bool {
			_, _ = w.c.Lookup(ctx, w.allKeys[0])
			return w.c.State() == client.StateClosed
		}, "client %d breaker closed", w.id)
	}
	for _, sc := range scenarios() {
		if sc.spec.Write == protocol.WriteBehind {
			testutil.Eventually(t, 10*time.Second, func() bool {
				return r.node.Engine().Flush(ctx, sc.name) == nil
			}, "flush %s", sc.name)
		}
	}
}

// verify checks the final state of every key against the writers' histories
// and checks that the cache agrees with the store.
func (r *run) verify() {
	t := r.t
	ctx := context.Background()
	for _, w := range r.workers {
		for key, h := range w.history {
			raw, ok := r.db.Peek(prefixOf(w.ns.name) + key)
			stored := deleted
			if ok {
				stored = string(raw)
			}

			var allowed []string
			if w.ns.spec.Write == protocol.WriteBehind && r.chaos {
				// Crashes may lose acknowledged write-behind data (documented).
				allowed = append([]string{deleted}, h.all...)
			} else {
				allowed = append([]string{h.acked}, h.uncertain...)
			}
			if !contains(allowed, stored) {
				t.Errorf("[%s] %s: store has %q, allowed %q", w.ns.name, key, stored, allowed)
			}

			// The cache must converge to the store.
			var last string
			converged := false
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
				it, err := w.c.Lookup(ctx, key)
				if err != nil {
					last = err.Error()
					continue
				}
				got := deleted
				if it.Found {
					got = string(it.Value)
				}
				last = fmt.Sprintf("%q from %s", got, it.Source)
				if got == stored && it.Source == client.SourceCache {
					converged = true
					break
				}
				if got == stored && !it.Found && w.ns.spec.Read == protocol.CacheAside && !w.ns.spec.CacheMisses {
					// Absent keys are never cached without negative caching.
					converged = true
					break
				}
				if got == stored && w.ns.spec.Read == protocol.CacheAside {
					// First read after a miss comes from the store; the
					// next one must come from the cache.
					continue
				}
			}
			if !converged {
				t.Errorf("[%s] %s: cache did not converge to store value %q, last read %s", w.ns.name, key, stored, last)
			}
		}
	}
}

func (r *run) report(d time.Duration) {
	t := r.t
	ctx := context.Background()
	ops := r.ops.Load()
	t.Logf("%d clients, %d ops in %v (%.0f ops/s): %d reads, %d writes, %d tolerated errors %v",
		len(r.workers), ops, d.Round(time.Millisecond), float64(ops)/d.Seconds(), r.reads.Load(), r.writes.Load(), r.tolerated.Load(), r.errCounts)
	seen := map[string]bool{}
	for _, w := range r.workers {
		if seen[w.ns.name] {
			continue
		}
		seen[w.ns.name] = true
		st, err := w.c.Stats(ctx)
		if err != nil {
			continue
		}
		hitRate := 0.0
		if st.Hits+st.Misses > 0 {
			hitRate = 100 * float64(st.Hits) / float64(st.Hits+st.Misses)
		}
		t.Logf("  %-16s entries=%-4d hit=%5.1f%% loads=%d fills=%d/%d flushed=%d watchEvents=%d resyncs=%d refreshes=%d",
			w.ns.name, st.Entries, hitRate, st.Loads, st.FillsAccepted, st.FillsRejected, st.FlushedWrites, st.WatchEvents, st.WatchResyncs, st.Refreshes)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
