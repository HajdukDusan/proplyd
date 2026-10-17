package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/c12s/proplyd/pkg/backend"
	"github.com/redis/go-redis/v9"
)

// runWatcher follows the backend change stream for the namespace prefix
// while this engine holds the namespace's watcher lease. Each change is
// applied to Redis (in place for cached keys, as a fence otherwise) and
// published to subscribers. Progress is recorded in meta.wrev, so a new
// watcher (after a restart or a lease handover) resumes where the last one
// stopped.
func (ns *namespace) runWatcher() {
	defer ns.wg.Done()
	w := ns.e.backend.(backend.Watcher)
	retry := ns.e.opts.LeaseTTL / 3
	backoff := 50 * time.Millisecond
	for ns.ctx.Err() == nil {
		held, err := ns.acquireLease(ns.ctx)
		if err != nil || !held {
			sleepCtx(ns.ctx, retry)
			continue
		}
		err = ns.watchSession(w)
		if ns.ctx.Err() != nil {
			return
		}
		if err == nil {
			backoff = 50 * time.Millisecond
			continue
		}
		if errors.Is(err, ErrNamespaceNotFound) {
			return
		}
		ns.e.log.Warn("watch interrupted", "namespace", ns.name, "err", err, "retry_in", backoff)
		sleepCtx(ns.ctx, backoff)
		backoff = min(2*backoff, 2*time.Second)
	}
}

func (ns *namespace) acquireLease(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	n, err := leaseScript.Run(ctx, ns.e.rdb, []string{ns.ks.lease}, ns.e.opts.InstanceID, ns.e.opts.LeaseTTL.Milliseconds()).Int()
	return n == 1, err
}

// watchSession runs one watch while the lease is held. It returns nil when
// the session should restart right away (history was compacted and the
// namespace purged), and an error otherwise.
func (ns *namespace) watchSession(w backend.Watcher) error {
	ctx, cancel := context.WithCancelCause(ns.ctx)
	defer cancel(nil)

	// Keep the lease. Losing it (or failing to renew it) ends the session:
	// another engine may be watching now.
	go func() {
		t := time.NewTicker(ns.e.opts.LeaseTTL / 3)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				held, err := ns.acquireLease(ctx)
				if err != nil || !held {
					cancel(fmt.Errorf("watcher lease lost (err: %v)", err))
					return
				}
			}
		}
	}()

	m, err := readMeta(ctx, ns.e.rdb, ns.ks)
	if err != nil {
		if errors.Is(err, ErrNamespaceNotFound) {
			ns.lost()
		}
		return err
	}
	ns.epoch.Store(m.epoch)
	ns.setWrev(m.wrev)
	ns.e.log.Debug("watch started", "namespace", ns.name, "from", m.wrev+1)

	for batch := range w.Watch(ctx, ns.spec.KeyPrefix, m.wrev+1) {
		if batch.Err != nil {
			var compacted *backend.CompactedError
			if errors.As(batch.Err, &compacted) {
				return ns.resync(ctx, compacted)
			}
			return batch.Err
		}
		if err := ns.applyBatch(ctx, batch); err != nil {
			return err
		}
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return errors.New("watch ended")
}

// resync recovers from lost watch history: changes since wrev cannot be
// replayed, so every entry is dropped and loads that read before the
// current revision are refused.
func (ns *namespace) resync(ctx context.Context, compacted *backend.CompactedError) error {
	rev, err := ns.e.backend.Revision(ctx)
	if err != nil {
		return err
	}
	ns.e.log.Warn("watch history compacted, purging namespace", "namespace", ns.name,
		"wrev", ns.wrev.Load(), "compact_revision", compacted.CompactRevision, "revision", rev)
	if err := ns.purge(ctx, rev); err != nil {
		return err
	}
	ns.setWrev(rev)
	return nil
}

// applyBatch applies a batch of changes, then records the progress.
func (ns *namespace) applyBatch(ctx context.Context, batch backend.WatchBatch) error {
	for attempt := 0; len(batch.Events) > 0; attempt++ {
		if attempt == 3 {
			return fmt.Errorf("%w: namespace epoch keeps changing", ErrCacheUnavailable)
		}
		epoch := ns.epoch.Load()
		cmds, err := ns.e.pipeline(ctx, func(p redis.Pipeliner) {
			for _, ev := range batch.Events {
				key, ok := strings.CutPrefix(ev.Key, ns.spec.KeyPrefix)
				if !ok || key == "" {
					continue
				}
				payload, ttl := ns.eventPayload(ev, epoch)
				args := ns.installArgs("watch", key, epoch, ev.Revision, ev.Revision, payload, ttl, encodeEntry(kindFence, epoch, ev.Revision, time.Now(), nil))
				installScript.EvalSha(ctx, p, []string{ns.ks.meta, ns.ks.entry(key), ns.ks.stream}, args...)
			}
		})
		if err != nil {
			return redisErr("apply watch batch", err)
		}
		retry := false
		for _, c := range cmds {
			res, err := c.(*redis.Cmd).Slice()
			if err != nil {
				return redisErr("apply watch event", err)
			}
			switch res[0].(int64) {
			case resNamespaceMissing:
				ns.lost()
				return ErrNamespaceNotFound
			case resEpochChanged:
				e, _ := parseEpoch(res[1].(string))
				ns.epoch.Store(e)
				retry = true
			}
		}
		if !retry {
			break
		}
		// The namespace was purged meanwhile. Events are idempotent, so
		// apply the whole batch again under the new epoch.
	}
	ns.e.stats.WatchEvents.Add(int64(len(batch.Events)))

	n, err := wrevScript.Run(ctx, ns.e.rdb, []string{ns.ks.meta}, batch.Revision).Int64()
	if err != nil {
		return redisErr("record watch progress", err)
	}
	if n == resNamespaceMissing {
		ns.lost()
		return ErrNamespaceNotFound
	}
	ns.setWrev(batch.Revision)
	return nil
}

func (ns *namespace) eventPayload(ev backend.Event, epoch uint64) ([]byte, time.Duration) {
	now := time.Now()
	switch {
	case !ev.Deleted && len(ev.Value) <= ns.e.opts.MaxValueSize:
		return encodeEntry(kindValue, epoch, ev.Revision, now, ev.Value), ns.ttl()
	case ev.Deleted && ns.spec.NegativeCaching():
		return encodeEntry(kindNegative, epoch, ev.Revision, now, nil), ns.negativeTTL()
	default:
		return encodeEntry(kindFence, epoch, ev.Revision, now, nil), ns.e.opts.FenceTTL
	}
}

// pipeline runs fn in a pipeline. If Redis lost its script cache (it
// restarted), the scripts are loaded and the pipeline runs again; installs
// are idempotent, so repeating commands that already ran is harmless.
func (e *Engine) pipeline(ctx context.Context, fill func(redis.Pipeliner)) ([]redis.Cmder, error) {
	fn := func(p redis.Pipeliner) error { fill(p); return nil }
	cmds, err := e.rdb.Pipelined(ctx, fn)
	if err != nil && slices.ContainsFunc(cmds, func(c redis.Cmder) bool { return isNoScript(c.Err()) }) {
		if err := loadScripts(ctx, e.rdb); err != nil {
			return nil, err
		}
		cmds, err = e.rdb.Pipelined(ctx, fn)
	}
	return cmds, err
}
