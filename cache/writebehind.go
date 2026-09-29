package cache

import (
	"context"
	"time"

	"github.com/c12s/proplyd/backend"
)

// writeBehind records a pending write. The write is acknowledged as soon as
// it is in memory; the flusher persists it later. Repeated writes to the same
// key before a flush are coalesced into one store write.
func (n *namespace) writeBehind(key string, value []byte, deleted bool, ttl time.Duration) error {
	e := entry{found: !deleted, pending: true, storedAt: time.Now().UnixNano(), value: value}
	enc := encodeEntry(e)
	// The dirty entry shares the encoded buffer; both are immutable.
	d := &dirtyEntry{value: enc[headerLen:], deleted: deleted, ttl: ttl}

	st := n.stripe(key)
	st.mu.Lock()
	if _, exists := st.dirty[key]; !exists {
		if n.pending.Load() >= int64(n.cfg.MaxPending) {
			st.mu.Unlock()
			return ErrBackpressure
		}
		n.pending.Add(1)
	}
	d.seq = n.seq.Add(1)
	st.dirty[key] = d
	n.store.Set(key, enc, n.ttl(ttl))
	st.mu.Unlock()

	if n.pending.Load() >= int64(n.cfg.MaxBatch) {
		select {
		case n.kick <- struct{}{}:
		default:
		}
	}
	return nil
}

type flushItem struct {
	key string
	d   *dirtyEntry
}

func (n *namespace) runFlusher() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.FlushInterval)
	defer t.Stop()
	backoff := newBackoff(n.cfg.FlushInterval, 5*time.Second)
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		case <-n.kick:
		}
		for {
			count, err := n.flushOnce(n.ctx)
			if err != nil {
				if n.ctx.Err() != nil {
					return
				}
				n.log.Warn("write-behind flush failed, will retry", "err", err, "pending", n.pending.Load())
				if !n.sleep(backoff.next()) {
					return
				}
				break
			}
			backoff.reset()
			if count < n.cfg.MaxBatch {
				break
			}
		}
	}
}

// flushOnce persists up to MaxBatch pending entries in one atomic store
// transaction and returns how many were written.
func (n *namespace) flushOnce(ctx context.Context) (int, error) {
	n.flushMu.Lock()
	defer n.flushMu.Unlock()

	batch := n.collectDirty(n.cfg.MaxBatch)
	if len(batch) == 0 {
		return 0, nil
	}
	ops := make([]backend.Op, len(batch))
	for i, it := range batch {
		ops[i] = backend.Op{Key: it.key, Value: it.d.value, Delete: it.d.deleted}
	}

	actx, cancel := context.WithTimeout(ctx, n.opts.FlushTimeout)
	rev, err := n.be.Apply(actx, ops)
	cancel()
	if err != nil {
		n.stats.flushErrors.Add(1)
		return 0, &BackendError{Err: err}
	}

	now := time.Now().UnixNano()
	for _, it := range batch {
		st := n.stripe(it.key)
		st.mu.Lock()
		// Only retire the entry if it was not overwritten while flushing;
		// otherwise the newer value stays dirty for the next round.
		if cur := st.dirty[it.key]; cur != nil && cur.seq == it.d.seq {
			delete(st.dirty, it.key)
			n.pending.Add(-1)
			if raw, ok := n.store.Peek(it.key); ok {
				if e, ok := decodeEntry(raw); ok && e.pending {
					if it.d.deleted && !n.cfg.CacheMisses {
						n.store.Delete(it.key)
					} else {
						committed := entry{found: !it.d.deleted, version: rev, storedAt: now, value: it.d.value}
						n.store.Set(it.key, encodeEntry(committed), n.ttl(it.d.ttl))
					}
				}
			}
		}
		n.changes.record(it.key, rev)
		st.mu.Unlock()
	}
	n.stats.flushed.Add(uint64(len(batch)))
	return len(batch), nil
}

// collectDirty snapshots up to limit dirty entries, starting at a rotating
// stripe so that no stripe is starved under sustained load.
func (n *namespace) collectDirty(limit int) []flushItem {
	batch := make([]flushItem, 0, min(limit, int(n.pending.Load())))
	start := n.flushCursor
	i := 0
	for ; i < stripeCount && len(batch) < limit; i++ {
		st := &n.stripes[(start+i)%stripeCount]
		st.mu.Lock()
		for key, d := range st.dirty {
			if len(batch) >= limit {
				break
			}
			batch = append(batch, flushItem{key: key, d: d})
		}
		st.mu.Unlock()
	}
	n.flushCursor = (start + i) % stripeCount
	return batch
}

// drain flushes until nothing is pending or ctx expires.
func (n *namespace) drain(ctx context.Context) error {
	for n.pending.Load() > 0 {
		if _, err := n.flushOnce(ctx); err != nil {
			if ctx.Err() != nil {
				n.log.Error("write-behind entries lost on shutdown", "pending", n.pending.Load())
				return err
			}
			select {
			case <-time.After(50 * time.Millisecond):
			case <-ctx.Done():
				n.log.Error("write-behind entries lost on shutdown", "pending", n.pending.Load())
				return ctx.Err()
			}
		}
	}
	return nil
}

type backoff struct {
	min, max, cur time.Duration
}

func newBackoff(min, max time.Duration) *backoff {
	return &backoff{min: min, max: max, cur: min}
}

func (b *backoff) next() time.Duration {
	d := b.cur
	b.cur = min(b.cur*2, b.max)
	return d
}

func (b *backoff) reset() { b.cur = b.min }
