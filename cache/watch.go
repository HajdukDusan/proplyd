package cache

import (
	"context"
	"errors"
	"time"

	"github.com/c12s/proplyd/backend"
)

// runWatch keeps the namespace in sync with changes made to the store by
// anyone other than this cache: clients writing directly while the cache was
// unreachable (write-around fallback), other services, operators.
//
// Cached keys are updated in place (not merely invalidated) so hot keys stay
// warm. Keys that are not cached are only recorded in the change log, which
// prevents in-flight loads/fills from installing values older than the change.
//
// If the watch cannot resume from where it stopped (the store compacted the
// history), the namespace is purged and the watch restarts from the current
// revision: after a gap nothing cached can be trusted.
func (n *namespace) runWatch() {
	defer n.wg.Done()
	bo := newBackoff(50*time.Millisecond, 5*time.Second)
	var next int64
	synced := false

	for n.ctx.Err() == nil {
		if !synced {
			rctx, cancel := context.WithTimeout(n.ctx, n.opts.LoadTimeout)
			rev, err := n.watcher.CurrentRevision(rctx)
			cancel()
			if err != nil {
				n.log.Warn("watch: cannot read store revision", "err", err)
				n.sleep(bo.next())
				continue
			}
			n.resync(rev)
			next = rev + 1
			synced = true
		}

		wctx, cancel := context.WithCancel(n.ctx)
		for resp := range n.watcher.Watch(wctx, next) {
			if resp.Err != nil {
				if errors.Is(resp.Err, backend.ErrCompacted) {
					n.log.Warn("watch: history compacted, resynchronising", "from", next)
					n.changes.block()
					synced = false
				} else {
					n.log.Warn("watch: interrupted, resuming", "from", next, "err", resp.Err)
				}
				break
			}
			bo.reset()
			for _, ev := range resp.Events {
				n.applyEvent(ev)
				next = ev.ModRevision + 1
			}
		}
		cancel()
		n.sleep(bo.next())
	}
}

// resync drops every committed entry and restarts change tracking at rev.
func (n *namespace) resync(rev int64) {
	n.lockAll()
	defer n.unlockAll()
	n.changes.block()
	n.store.Purge()
	n.restorePendingLocked()
	n.changes.reset(rev)
	n.stats.watchResyncs.Add(1)
}

func (n *namespace) applyEvent(ev backend.Event) {
	n.stats.watchEvents.Add(1)
	st := n.stripe(ev.Key)
	st.mu.Lock()
	defer st.mu.Unlock()

	n.changes.record(ev.Key, ev.ModRevision)
	if st.dirty[ev.Key] != nil {
		// A pending write-behind value will overwrite this change when it is
		// flushed (last writer wins).
		return
	}
	raw, ok := n.store.Peek(ev.Key)
	if !ok {
		return
	}
	cur, ok := decodeEntry(raw)
	if !ok || cur.pending || cur.version >= ev.ModRevision {
		return
	}
	if ev.Type == backend.EventPut {
		e := entry{found: true, version: ev.ModRevision, storedAt: time.Now().UnixNano(), value: ev.Value}
		n.store.Set(ev.Key, encodeEntry(e), n.cfg.TTL)
		return
	}
	if n.cfg.CacheMisses {
		n.store.Set(ev.Key, encodeEntry(entry{version: ev.ModRevision, storedAt: time.Now().UnixNano()}), n.cfg.TTL)
	} else {
		n.store.Delete(ev.Key)
	}
}
