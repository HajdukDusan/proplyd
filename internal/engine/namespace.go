package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c12s/proplyd/pkg/backend"
	"github.com/c12s/proplyd/pkg/protocol"
	"golang.org/x/sync/singleflight"
)

// namespace is the engine's handle on an active namespace.
type namespace struct {
	e    *Engine
	name string
	spec protocol.Spec
	ks   keyspace

	epoch atomic.Uint64

	ctx    context.Context // namespace lifetime
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// ready is closed once this engine may serve the namespace: the watcher
	// (here or in another engine) has applied every backend change made
	// before activation.
	ready     chan struct{}
	readyOnce sync.Once
	// wrev is the highest watch progress this engine has seen.
	wrev     atomic.Int64
	progress chan struct{} // signalled when wrev moves

	loads singleflight.Group
	tail  tailer
}

func newNamespace(e *Engine, name string, ks keyspace, m meta) *namespace {
	ctx, cancel := context.WithCancel(e.ctx)
	ns := &namespace{
		e:        e,
		name:     name,
		spec:     m.spec,
		ks:       ks,
		ctx:      ctx,
		cancel:   cancel,
		ready:    make(chan struct{}),
		progress: make(chan struct{}, 1),
	}
	ns.epoch.Store(m.epoch)
	ns.wrev.Store(m.wrev)
	ns.tail.ns = ns
	return ns
}

func (ns *namespace) start() {
	if ns.spec.Write == protocol.WriteBehind {
		ns.wg.Add(1)
		go ns.runFlusher()
	}
	if ns.spec.Sync != protocol.SyncWatch {
		ns.markReady()
		return
	}
	ns.wg.Add(2)
	go ns.awaitSync()
	go ns.runWatcher()
}

// stop ends the namespace's goroutines and subscriptions. With release,
// the watcher lease is handed back so another engine can take over at once.
func (ns *namespace) stop(release bool) {
	ns.cancel()
	ns.tail.closeAll(fmt.Errorf("%w: namespace reloading", ErrCacheUnavailable))
	ns.wg.Wait()
	if release {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if ns.spec.Sync == protocol.SyncWatch {
			_ = releaseScript.Run(ctx, ns.e.rdb, []string{ns.ks.lease}, ns.e.opts.InstanceID).Err()
		}
		if ns.spec.Write == protocol.WriteBehind {
			_ = releaseScript.Run(ctx, ns.e.rdb, []string{ns.ks.flushLease}, ns.e.opts.InstanceID).Err()
		}
	}
}

func (ns *namespace) markReady() {
	ns.readyOnce.Do(func() { close(ns.ready) })
}

func (ns *namespace) setWrev(rev int64) {
	for {
		cur := ns.wrev.Load()
		if rev <= cur || ns.wrev.CompareAndSwap(cur, rev) {
			break
		}
	}
	select {
	case ns.progress <- struct{}{}:
	default:
	}
}

// awaitSync marks the namespace ready once the watch progress recorded in
// Redis reaches the backend revision observed now. Until then, entries may
// miss changes made while no watcher was running (for example while proplyd
// was down and clients wrote the backend directly).
func (ns *namespace) awaitSync() {
	defer ns.wg.Done()
	var target int64
	for backoff := 50 * time.Millisecond; ; backoff = min(2*backoff, 2*time.Second) {
		ctx, cancel := context.WithTimeout(ns.ctx, ns.e.opts.LoadTimeout)
		rev, id, err := ns.e.backendState(ctx)
		var m meta
		if err == nil {
			m, err = readMeta(ctx, ns.e.rdb, ns.ks)
		}
		cancel()
		if errors.Is(err, ErrNamespaceNotFound) {
			go ns.e.forget(ns)
			return
		}
		if err == nil {
			if storeReset(m, rev, id) == "" {
				target = rev
				break
			}
			// The store was wiped or restored; wait for the watcher to
			// rebase the namespace before serving anything from it.
			backoff = 50 * time.Millisecond
		} else {
			ns.e.log.Warn("namespace waiting for backend to sync", "namespace", ns.name, "err", err)
		}
		if !sleepCtx(ns.ctx, backoff) {
			return
		}
	}
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for ns.wrev.Load() < target {
		select {
		case <-ns.ctx.Done():
			return
		case <-ns.progress:
		case <-poll.C:
			// Another engine may hold the watcher lease; read its progress.
			ctx, cancel := context.WithTimeout(ns.ctx, time.Second)
			m, err := readMeta(ctx, ns.e.rdb, ns.ks)
			cancel()
			if errors.Is(err, ErrNamespaceNotFound) {
				go ns.e.forget(ns)
				return
			}
			if err == nil {
				ns.setWrev(m.wrev)
			}
		}
	}
	ns.markReady()
	ns.e.log.Debug("namespace in sync", "namespace", ns.name, "revision", target)
}

func (ns *namespace) waitReady(ctx context.Context) error {
	select {
	case <-ns.ready:
		return nil
	default:
	}
	t := time.NewTimer(ns.e.opts.ReadyTimeout)
	defer t.Stop()
	select {
	case <-ns.ready:
		return nil
	case <-t.C:
		return fmt.Errorf("%w: namespace %q is catching up with the backend", ErrCacheUnavailable, ns.name)
	case <-ns.ctx.Done():
		return fmt.Errorf("%w: namespace %q is reloading", ErrCacheUnavailable, ns.name)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Result of a Get.
type Result struct {
	// Miss: cache-aside miss; the caller loads and fills.
	Miss     bool
	Found    bool
	Value    []byte
	Revision int64
	// FromBackend: loaded for this request.
	FromBackend bool
	// Pending: a write-behind write not flushed yet; Revision is the newest
	// store revision the cache knew for the key when it was written.
	Pending bool
}

// Get reads key. minRev, if positive, is a revision the caller knows the
// key changed at; older cached state is treated as a miss.
func (e *Engine) Get(ctx context.Context, name, key string, minRev int64) (Result, error) {
	ns, err := e.ready(ctx, name, key)
	if err != nil {
		return Result{}, err
	}
	ent, pw, ok, err := ns.read(ctx, key)
	if err != nil {
		return Result{}, err
	}
	if pw != nil {
		e.stats.Hits.Add(1)
		r := Result{Found: !pw.del, Pending: true, Revision: max(pw.seen, pw.base, 0)}
		if !pw.del {
			r.Value = pw.value
		}
		return r, nil
	}
	if ok && (ent.kind == kindValue || ent.kind == kindNegative) && ent.rev >= minRev {
		e.stats.Hits.Add(1)
		if ns.spec.Read == protocol.RefreshAhead && time.Since(ent.storedAt) >= ns.spec.RefreshAfter {
			ns.refresh(key)
		}
		return Result{Found: ent.kind == kindValue, Value: ent.value, Revision: ent.rev}, nil
	}
	e.stats.Misses.Add(1)
	if ns.spec.Read == protocol.CacheAside {
		return Result{Miss: true}, nil
	}
	item, err := ns.load(ctx, key, minRev)
	if err != nil {
		return Result{}, err
	}
	return itemResult(item), nil
}

func itemResult(it backend.Item) Result {
	r := Result{Found: it.Found, Value: it.Value, FromBackend: true, Revision: it.ModRevision}
	if !it.Found {
		r.Revision = it.ReadRevision
	}
	return r
}

// ready validates the request and returns the namespace once it can serve.
func (e *Engine) ready(ctx context.Context, name, key string) (*namespace, error) {
	if err := protocol.ValidateKey(key); err != nil {
		return nil, invalidf("%v", err)
	}
	ns, err := e.namespace(ctx, name)
	if err != nil {
		return nil, err
	}
	if err := e.checkUp(); err != nil {
		return nil, err
	}
	if err := ns.waitReady(ctx); err != nil {
		return nil, err
	}
	return ns, nil
}

// read returns the pending write-behind write of key, if any, and else
// its live entry, if any.
func (ns *namespace) read(ctx context.Context, key string) (entry, *pendingWrite, bool, error) {
	wb := "0"
	if ns.spec.Write == protocol.WriteBehind {
		wb = "1"
	}
	res, err := getScript.Run(ctx, ns.e.rdb, []string{ns.ks.meta, ns.ks.entry(key), ns.ks.pending}, key, wb).Slice()
	if err != nil {
		return entry{}, nil, false, redisErr("get", err)
	}
	if res[0].(int64) == resNamespaceMissing {
		ns.lost()
		return entry{}, nil, false, ErrNamespaceNotFound
	}
	epoch, _ := parseEpoch(res[1].(string))
	ns.epoch.Store(epoch)
	if len(res) > 3 && res[3] != nil {
		if pw, ok := decodePending([]byte(res[3].(string))); ok {
			return entry{}, &pw, false, nil
		}
	}
	if len(res) < 3 || res[2] == nil {
		return entry{}, nil, false, nil
	}
	ent, ok := decodeEntry([]byte(res[2].(string)))
	if !ok || ent.epoch != epoch {
		return entry{}, nil, false, nil
	}
	return ent, nil, true, nil
}

// lost handles a namespace that disappeared from Redis.
func (ns *namespace) lost() {
	ns.e.log.Warn("namespace missing from redis", "namespace", ns.name)
	go ns.e.forget(ns)
}

// load reads key from the backend and installs it. Concurrent loads of a
// key share one backend read.
func (ns *namespace) load(ctx context.Context, key string, minRev int64) (backend.Item, error) {
	ch := ns.loads.DoChan(key, func() (any, error) {
		return ns.loadAndInstall(key)
	})
	var r singleflight.Result
	select {
	case r = <-ch:
	case <-ctx.Done():
		return backend.Item{}, ctx.Err()
	}
	if r.Err != nil {
		return backend.Item{}, r.Err
	}
	item := r.Val.(backend.Item)
	if item.ReadRevision < minRev {
		// The shared load started before the caller's write. Read again.
		return ns.loadAndInstall(key)
	}
	return item, nil
}

func (ns *namespace) loadAndInstall(key string) (backend.Item, error) {
	ctx, cancel := context.WithTimeout(ns.ctx, ns.e.opts.LoadTimeout)
	defer cancel()
	ns.e.stats.Loads.Add(1)
	item, err := ns.e.backend.Get(ctx, ns.spec.KeyPrefix+key)
	if err != nil {
		ns.e.stats.LoadErrors.Add(1)
		return backend.Item{}, err
	}
	if _, err := ns.installItem(ctx, key, item); err != nil && !errors.Is(err, context.Canceled) {
		// The read itself succeeded; serve it uncached.
		ns.e.log.Debug("install after load failed", "namespace", ns.name, "key", key, "err", err)
	}
	return item, nil
}

// refresh reloads key in the background (refresh-ahead).
func (ns *namespace) refresh(key string) {
	ns.loads.DoChan("\x00refresh\x00"+key, func() (any, error) {
		return ns.loadAndInstall(key)
	})
}

// installItem installs a load result with mode fill.
func (ns *namespace) installItem(ctx context.Context, key string, item backend.Item) (bool, error) {
	epoch := ns.epoch.Load()
	now := time.Now()
	if item.Found {
		payload := encodeEntry(kindValue, epoch, item.ModRevision, now, item.Value)
		return ns.install(ctx, "fill", key, item.ModRevision, item.ReadRevision, payload, ns.ttl(), nil)
	}
	if !ns.spec.NegativeCaching() {
		return false, nil
	}
	payload := encodeEntry(kindNegative, epoch, item.ReadRevision, now, nil)
	return ns.install(ctx, "fill", key, item.ReadRevision, item.ReadRevision, payload, ns.negativeTTL(), nil)
}

// ttl returns the value TTL with up to 10% jitter, so entries loaded
// together do not expire together.
func (ns *namespace) ttl() time.Duration { return jitter(ns.spec.TTL) }

func (ns *namespace) negativeTTL() time.Duration { return jitter(ns.spec.NegativeTTL) }

func jitter(d time.Duration) time.Duration {
	return d - time.Duration(rand.Int64N(int64(d)/10+1))
}

// install runs installScript, retrying once if the epoch changed under us.
func (ns *namespace) install(ctx context.Context, mode, key string, rev, readRev int64, payload []byte, ttl time.Duration, fence []byte) (bool, error) {
	for attempt := 0; ; attempt++ {
		// Payloads embed the epoch; stamp the one the script will check.
		epoch := ns.epoch.Load()
		setEpoch(payload, epoch)
		setEpoch(fence, epoch)
		args := ns.installArgs(mode, key, epoch, rev, readRev, payload, ttl, fence)
		res, err := installScript.Run(ctx, ns.e.rdb, []string{ns.ks.meta, ns.ks.entry(key), ns.ks.stream}, args...).Slice()
		if err != nil {
			return false, redisErr("install", err)
		}
		switch res[0].(int64) {
		case resNamespaceMissing:
			ns.lost()
			return false, ErrNamespaceNotFound
		case resEpochChanged:
			newEpoch, _ := parseEpoch(res[1].(string))
			ns.epoch.Store(newEpoch)
			if attempt > 0 {
				return false, fmt.Errorf("%w: namespace epoch keeps changing", ErrCacheUnavailable)
			}
			continue
		case resStored, resRefreshed:
			if mode == "fill" {
				ns.e.stats.Fills.Add(1)
			}
			return true, nil
		default:
			if mode == "fill" {
				ns.e.stats.FillsRejected.Add(1)
			}
			return false, nil
		}
	}
}

func (ns *namespace) installArgs(mode, key string, epoch uint64, rev, readRev int64, payload []byte, ttl time.Duration, fence []byte) []any {
	if payload == nil {
		payload = []byte{}
	}
	if fence == nil {
		fence = []byte{}
	}
	return []any{
		mode, epochBytes(epoch), rev, readRev,
		payload, max(ttl.Milliseconds(), 1),
		fence, ns.e.opts.FenceTTL.Milliseconds(),
		ns.e.opts.InvalidationLogSize, key,
	}
}

// setEpoch stamps epoch into an encoded entry (callers own their payloads).
func setEpoch(payload []byte, epoch uint64) {
	if len(payload) >= headerLen {
		binary.BigEndian.PutUint64(payload[1:9], epoch)
	}
}

func (ns *namespace) fencePayload(rev int64) []byte {
	return encodeEntry(kindFence, ns.epoch.Load(), rev, time.Now(), nil)
}

// Fill installs a value the client loaded itself (cache-aside).
func (e *Engine) Fill(ctx context.Context, name, key string, item backend.Item) (bool, error) {
	ns, err := e.ready(ctx, name, key)
	if err != nil {
		return false, err
	}
	switch {
	case len(item.Value) > e.opts.MaxValueSize:
		return false, invalidf("value of %d bytes exceeds %d", len(item.Value), e.opts.MaxValueSize)
	case item.ReadRevision <= 0:
		return false, invalidf("read revision is required")
	case item.Found && (item.ModRevision <= 0 || item.ModRevision > item.ReadRevision):
		return false, invalidf("mod revision %d must be in (0, read revision %d]", item.ModRevision, item.ReadRevision)
	}
	return ns.installItem(ctx, key, item)
}

// Set writes key to the backend and updates the cache per the write
// strategy. It returns the write's revision.
func (e *Engine) Set(ctx context.Context, name, key string, value []byte) (int64, error) {
	ns, err := e.ready(ctx, name, key)
	if err != nil {
		return 0, err
	}
	if len(value) > e.opts.MaxValueSize {
		return 0, invalidf("value of %d bytes exceeds %d", len(value), e.opts.MaxValueSize)
	}
	if ns.spec.Write == protocol.WriteBehind {
		return 0, ns.writeBehind(ctx, key, false, value)
	}
	rev, err := e.backend.Put(ctx, ns.spec.KeyPrefix+key, value)
	if err != nil {
		return 0, err
	}
	e.stats.Writes.Add(1)
	return rev, ns.afterWrite(ctx, key, rev, func(epoch uint64) []byte {
		return encodeEntry(kindValue, epoch, rev, time.Now(), value)
	}, ns.ttl())
}

// Delete removes key from the backend and updates the cache.
func (e *Engine) Delete(ctx context.Context, name, key string) (int64, bool, error) {
	ns, err := e.ready(ctx, name, key)
	if err != nil {
		return 0, false, err
	}
	if ns.spec.Write == protocol.WriteBehind {
		// Whether the key existed is not known until the flush.
		return 0, false, ns.writeBehind(ctx, key, true, nil)
	}
	rev, existed, err := e.backend.Delete(ctx, ns.spec.KeyPrefix+key)
	if err != nil {
		return 0, false, err
	}
	e.stats.Writes.Add(1)
	var payload func(uint64) []byte
	if ns.spec.NegativeCaching() {
		payload = func(epoch uint64) []byte { return encodeEntry(kindNegative, epoch, rev, time.Now(), nil) }
	}
	return rev, existed, ns.afterWrite(ctx, key, rev, payload, ns.negativeTTL())
}

// afterWrite updates the cache after a backend write at rev. It runs to
// completion even if the caller gives up: the backend already has the
// write, and the cache must not keep the old value.
func (ns *namespace) afterWrite(ctx context.Context, key string, rev int64, payload func(uint64) []byte, ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ns.e.opts.UpdateTimeout)
	defer cancel()
	var err error
	if ns.spec.Write == protocol.WriteThrough && payload != nil {
		_, err = ns.install(ctx, "write", key, rev, rev, payload(ns.epoch.Load()), ttl, nil)
	} else {
		_, err = ns.install(ctx, "fence", key, rev, rev, nil, 0, ns.fencePayload(rev))
	}
	if err != nil {
		// The backend has the write. The watcher (if any) repairs the
		// entry once Redis is reachable; the caller learns that the cache
		// could not be updated.
		ns.e.log.Warn("cache update after write failed", "namespace", ns.name, "key", key, "revision", rev, "err", err)
		return fmt.Errorf("written to the backend at revision %d, but the cache update failed: %w", rev, err)
	}
	return nil
}

// Invalidate drops cached keys, or the whole namespace, without touching
// the backend.
func (e *Engine) Invalidate(ctx context.Context, name string, keys []string, all bool) error {
	ns, err := e.namespace(ctx, name)
	if err != nil {
		return err
	}
	if err := e.checkUp(); err != nil {
		return err
	}
	if all {
		return ns.purge(ctx, 0, false, "")
	}
	for _, k := range keys {
		if err := protocol.ValidateKey(k); err != nil {
			return invalidf("%v", err)
		}
	}
	for _, k := range keys {
		if _, err := ns.install(ctx, "drop", k, 0, 0, nil, 0, nil); err != nil {
			return err
		}
	}
	return nil
}

// purge kills every entry by moving to a new epoch. With floor > 0, loads
// that read below floor are never installed again (used when watch history
// was lost), and the watch restarts from floor. With rebase, floor and
// watch progress may move back (the store was wiped or restored), and the
// store identity is updated to id.
func (ns *namespace) purge(ctx context.Context, floor int64, rebase bool, id string) error {
	floorArg, wrevArg, rebaseArg := "", "", ""
	if floor > 0 || rebase {
		floorArg, wrevArg = fmt.Sprint(floor), fmt.Sprint(floor)
	}
	if rebase {
		rebaseArg = "1"
	}
	for attempt := 0; attempt < 3; attempt++ {
		cur := ns.epoch.Load()
		next := newEpoch()
		res, err := purgeScript.Run(ctx, ns.e.rdb, []string{ns.ks.meta, ns.ks.stream},
			epochBytes(cur), epochBytes(next), floorArg, wrevArg, ns.e.opts.InvalidationLogSize, rebaseArg, id).Slice()
		if err != nil {
			return redisErr("purge", err)
		}
		switch res[0].(int64) {
		case resNamespaceMissing:
			ns.lost()
			return ErrNamespaceNotFound
		case resEpochChanged:
			e, _ := parseEpoch(res[1].(string))
			ns.epoch.Store(e)
			continue
		}
		ns.epoch.Store(next)
		ns.e.stats.Purges.Add(1)
		ns.e.log.Info("namespace purged", "namespace", ns.name, "floor", floor)
		return nil
	}
	return fmt.Errorf("%w: namespace epoch keeps changing", ErrCacheUnavailable)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
