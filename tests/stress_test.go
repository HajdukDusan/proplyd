package tests

import (
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/c12s/proplyd/internal/testutil"
)

// TestStress hammers one central cache with many clients spread over six
// namespaces that cover every read/write strategy family. Every key has a
// single writer, which lets the test check exact invariants:
//
//   - no errors at all while everything is healthy,
//   - read-your-writes for every owner on every read,
//   - after the run, the store holds each key's last acknowledged value,
//   - the cache agrees with the store for every key.
//
// Tune with PROPLYD_STRESS_DURATION (default 3s), PROPLYD_STRESS_CLIENTS
// (clients per namespace, default 8) and PROPLYD_STRESS_KEYS (default 120).
func TestStress(t *testing.T) {
	duration := envDuration("PROPLYD_STRESS_DURATION", 3*time.Second)
	if testing.Short() {
		duration = time.Second
	}
	r := newRun(t, envInt("PROPLYD_STRESS_CLIENTS", 8), envInt("PROPLYD_STRESS_KEYS", 120), false)

	stop := make(chan struct{})
	start := time.Now()
	time.AfterFunc(duration, func() { close(stop) })
	r.drive(stop)
	elapsed := time.Since(start)

	r.settle()
	r.verify()
	r.report(elapsed)
}

// TestChaos runs the same workload while continuously injecting failures:
// cache crashes (memory and unflushed write-behind data lost), network
// partitions and connection resets between clients and cache, a slow or
// unreachable store, a broken cache→store link, lost watch streams and
// history compaction.
//
// Invariants:
//   - clients only ever see ErrCacheUnavailable / ErrStoreUnavailable /
//     ErrBackpressure, never hangs or other errors,
//   - write-through / write-around: the store holds the last acknowledged
//     write (or a later write whose outcome was unknown to the client),
//   - write-behind: the store holds some value that was written (acknowledged
//     writes may be lost on crashes, as documented),
//   - once faults stop, every client closes its breaker and the cache
//     converges to the store for every key.
func TestChaos(t *testing.T) {
	duration := envDuration("PROPLYD_CHAOS_DURATION", 6*time.Second)
	if testing.Short() {
		duration = 2 * time.Second
	}
	r := newRun(t, envInt("PROPLYD_STRESS_CLIENTS", 6), envInt("PROPLYD_STRESS_KEYS", 60), true)

	stop := make(chan struct{})
	chaosDone := make(chan map[string]int)
	go func() { chaosDone <- r.inject(stop) }()

	start := time.Now()
	time.AfterFunc(duration, func() { close(stop) })
	r.drive(stop)
	elapsed := time.Since(start)
	injected := <-chaosDone
	t.Logf("injected faults: %v", injected)

	r.settle()
	r.verify()
	r.report(elapsed)
}

func (r *run) inject(stop <-chan struct{}) map[string]int {
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 7))
	counts := map[string]int{}
	pause := func(d time.Duration) {
		select {
		case <-time.After(d):
		case <-stop:
		}
	}
	faults := []struct {
		name string
		do   func()
	}{
		{"cache crash+restart", func() {
			r.node.Kill()
			pause(150 * time.Millisecond)
			r.node.Start()
		}},
		{"partition (blackhole)", func() {
			r.node.Proxy.SetMode(testutil.Blackhole)
			pause(300 * time.Millisecond)
			r.node.Proxy.SetMode(testutil.Forward)
		}},
		{"connection reset", func() {
			r.node.Proxy.DropConnections()
		}},
		{"slow store", func() {
			r.db.SetLatency(50 * time.Millisecond)
			pause(300 * time.Millisecond)
			r.db.SetLatency(0)
		}},
		{"cache->store link down", func() {
			r.faults.SetFailing(true)
			pause(250 * time.Millisecond)
			r.faults.SetFailing(false)
		}},
		{"store down", func() {
			r.db.SetDown(true)
			pause(150 * time.Millisecond)
			r.db.SetDown(false)
		}},
		{"watch broken", func() {
			r.db.BreakWatches(errors.New("chaos: stream reset"))
		}},
		{"watch lost + compaction", func() {
			r.db.PauseWatches(true)
			pause(100 * time.Millisecond)
			r.db.Compact()
			r.db.PauseWatches(false)
		}},
	}
	// Shuffled rounds: every fault type is injected once per round.
	for {
		for _, i := range rng.Perm(len(faults)) {
			select {
			case <-stop:
				return counts
			default:
			}
			counts[faults[i].name]++
			faults[i].do()
			pause(time.Duration(50+rng.IntN(150)) * time.Millisecond)
		}
	}
}
