package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/c12s/proplyd/pkg/backend"
	"github.com/redis/go-redis/v9"
)

// Write-behind.
//
// A write is acknowledged once it is recorded as pending in Redis (the
// pending hash, never evicted, plus a queue of keys). Reads see pending
// writes before cached entries. A flusher (one engine per namespace, under
// a lease) writes pending keys to the store in batches; writes to the same
// key between flushes coalesce into one store write.
//
// Every pending write remembers the store mod revision it is based on, and
// a flush is a compare-and-set on it: if the store changed behind the write
// (another writer, or a Redis rolled back to a snapshot replaying old
// writes), the store wins and the write is dropped. When the base is
// unknown because the store was unreachable at write time, the flush uses
// the revision current at flush time (last writer wins for that write).
//
// Pending writes live only in Redis: if Redis loses its data they are lost.

// ErrBackpressure: too many keys are waiting to be flushed.
var ErrBackpressure = errors.New("too many pending write-behind writes")

const unknownBase = -1

// pendingWrite is a decoded pending record (layout in scripts.go).
type pendingWrite struct {
	seq   int64
	base  int64 // revision the flush compares against
	seen  int64 // revision reads report
	del   bool
	value []byte
}

const pendingHeader = 25

func decodePending(b []byte) (pendingWrite, bool) {
	if len(b) < pendingHeader {
		return pendingWrite{}, false
	}
	return pendingWrite{
		seq:   int64(binary.BigEndian.Uint64(b[0:8])),
		base:  int64(binary.BigEndian.Uint64(b[8:16])),
		seen:  int64(binary.BigEndian.Uint64(b[16:24])),
		del:   b[24] == 1,
		value: b[pendingHeader:],
	}, true
}

// writeBehind records a write-behind write (del: a delete).
func (ns *namespace) writeBehind(ctx context.Context, key string, del bool, value []byte) error {
	delArg := "0"
	if del {
		delArg = "1"
	}
	base := ""
	for attempt := 0; attempt < 4; attempt++ {
		args := []any{"", epochBytes(ns.epoch.Load()), key, delArg, value, base,
			ns.e.opts.MaxPending, ns.e.opts.InvalidationLogSize, time.Now().UnixMilli()}
		res, err := wbWriteScript.Run(ctx, ns.e.rdb, []string{ns.ks.meta, ns.ks.entry(key), ns.ks.stream, ns.ks.pending, ns.ks.queue}, args...).Slice()
		if err != nil {
			return redisErr("write-behind", err)
		}
		switch res[0].(int64) {
		case resNamespaceMissing:
			ns.lost()
			return ErrNamespaceNotFound
		case resEpochChanged:
			e, _ := parseEpoch(res[1].(string))
			ns.epoch.Store(e)
		case resBackpressure:
			return fmt.Errorf("%w (%d keys)", ErrBackpressure, ns.e.opts.MaxPending)
		case resNeedBase:
			// The key is not cached: learn its store revision. If the store
			// is unreachable the write is still accepted, with an unknown
			// base.
			item, err := ns.e.backend.Get(ctx, ns.spec.KeyPrefix+key)
			switch {
			case err == nil:
				base = strconv.FormatInt(item.ModRevision, 10)
			case ctx.Err() != nil:
				return ctx.Err()
			default:
				base = strconv.Itoa(unknownBase)
				ns.e.stats.UnknownBases.Add(1)
			}
		default:
			ns.e.stats.PendingWrites.Add(1)
			return nil
		}
	}
	return fmt.Errorf("%w: write-behind kept conflicting with namespace changes", ErrCacheUnavailable)
}

// runFlusher flushes pending writes while this engine holds the
// namespace's flusher lease.
func (ns *namespace) runFlusher() {
	defer ns.wg.Done()
	backoff := 50 * time.Millisecond
	for ns.ctx.Err() == nil {
		held, err := ns.acquire(ns.ctx, ns.ks.flushLease)
		if err != nil || !held {
			sleepCtx(ns.ctx, ns.e.opts.LeaseTTL/3)
			continue
		}
		n, err := ns.flushBatch(ns.ctx)
		switch {
		case ns.ctx.Err() != nil:
			return
		case err != nil:
			ns.e.log.Warn("write-behind flush failed", "namespace", ns.name, "err", err, "retry_in", backoff)
			sleepCtx(ns.ctx, backoff)
			backoff = min(2*backoff, 2*time.Second)
		case n < ns.e.opts.FlushBatch:
			// Queue drained: let writes accumulate (and coalesce) for a while.
			backoff = 50 * time.Millisecond
			sleepCtx(ns.ctx, ns.e.opts.FlushInterval)
		default:
			backoff = 50 * time.Millisecond
		}
	}
}

// flushBatch flushes up to FlushBatch pending keys, oldest first. It stops
// at the first store error, leaving the rest queued.
func (ns *namespace) flushBatch(ctx context.Context) (int, error) {
	keys, err := ns.e.rdb.ZRange(ctx, ns.ks.queue, 0, int64(ns.e.opts.FlushBatch-1)).Result()
	if err != nil {
		return 0, redisErr("read write-behind queue", err)
	}
	cw := ns.e.backend.(backend.CondWriter)
	for _, key := range keys {
		raw, err := ns.e.rdb.HGet(ctx, ns.ks.pending, key).Bytes()
		if errors.Is(err, redis.Nil) {
			ns.e.rdb.ZRem(ctx, ns.ks.queue, key) // already retired
			continue
		}
		if err != nil {
			return 0, redisErr("read pending write", err)
		}
		pw, ok := decodePending(raw)
		if !ok {
			return 0, fmt.Errorf("corrupt pending write for %q", key)
		}
		if err := ns.flushOne(ctx, cw, key, pw); err != nil {
			return 0, err
		}
	}
	return len(keys), nil
}

// flushOne flushes one pending write. Before touching the store it records
// a flush intent, so that if Redis fails after the store write, the next
// attempt recognises its own write instead of mistaking it for a conflict
// (which would drop newer writes to the key).
func (ns *namespace) flushOne(ctx context.Context, cw backend.CondWriter, key string, pw pendingWrite) error {
	full := ns.spec.KeyPrefix + key
	if done, err := ns.finishIntent(ctx, key); err != nil || done {
		// A previous attempt was committed now; the pending record may
		// have moved on, so leave it to the next batch.
		return err
	}
	expect := pw.base
	if expect == unknownBase {
		item, err := ns.e.backend.Get(ctx, full)
		if err != nil {
			return err
		}
		expect = item.ModRevision
	}
	if err := ns.e.rdb.HSet(ctx, ns.ks.intent, key, encodePending(pw)).Err(); err != nil {
		return redisErr("record flush intent", err)
	}
	var (
		rev int64
		ok  bool
		err error
	)
	if pw.del {
		rev, ok, err = cw.DeleteIf(ctx, full, expect)
	} else {
		rev, ok, err = cw.PutIf(ctx, full, pw.value, expect)
	}
	if err != nil {
		return err // the intent stays; the next attempt checks the store
	}
	if !ok {
		ns.e.stats.FlushConflicts.Add(1)
		ns.e.log.Warn("write-behind conflict: the store changed behind a pending write; the store wins",
			"namespace", ns.name, "key", key, "based_on", pw.base)
		return ns.commit(ctx, key, pw, "conflict", rev)
	}
	ns.e.stats.Flushed.Add(1)
	return ns.commit(ctx, key, pw, "ok", rev)
}

// finishIntent resolves a flush whose outcome is unknown (the store write
// may have happened, but its commit did not). If the store holds what the
// intent wrote, the flush is committed now. done reports that.
func (ns *namespace) finishIntent(ctx context.Context, key string) (done bool, err error) {
	raw, err := ns.e.rdb.HGet(ctx, ns.ks.intent, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, redisErr("read flush intent", err)
	}
	in, ok := decodePending(raw)
	if !ok {
		return false, ns.e.rdb.HDel(ctx, ns.ks.intent, key).Err()
	}
	item, err := ns.e.backend.Get(ctx, ns.spec.KeyPrefix+key)
	if err != nil {
		return false, err
	}
	applied := (in.del && !item.Found) || (!in.del && item.Found && string(item.Value) == string(in.value))
	if !applied {
		if err := ns.e.rdb.HDel(ctx, ns.ks.intent, key).Err(); err != nil {
			return false, redisErr("clear flush intent", err)
		}
		return false, nil
	}
	rev := item.ModRevision
	if in.del {
		rev = item.ReadRevision
	}
	ns.e.stats.Flushed.Add(1)
	return true, ns.commit(ctx, key, in, "ok", rev)
}

func encodePending(pw pendingWrite) []byte {
	b := make([]byte, pendingHeader+len(pw.value))
	binary.BigEndian.PutUint64(b[0:8], uint64(pw.seq))
	binary.BigEndian.PutUint64(b[8:16], uint64(pw.base))
	binary.BigEndian.PutUint64(b[16:24], uint64(pw.seen))
	if pw.del {
		b[24] = 1
	}
	copy(b[pendingHeader:], pw.value)
	return b
}

// commit records a flush outcome; the cached entry becomes the flushed
// state.
func (ns *namespace) commit(ctx context.Context, key string, pw pendingWrite, outcome string, rev int64) error {
	now := time.Now()
	payload, ttl := encodeEntry(kindValue, 0, rev, now, pw.value), ns.ttl()
	if pw.del {
		if ns.spec.NegativeCaching() {
			payload, ttl = encodeEntry(kindNegative, 0, rev, now, nil), ns.negativeTTL()
		} else {
			payload, ttl = encodeEntry(kindFence, 0, rev, now, nil), ns.e.opts.FenceTTL
		}
	}
	keys := []string{ns.ks.meta, ns.ks.entry(key), ns.ks.stream, ns.ks.pending, ns.ks.queue, ns.ks.intent}
	for attempt := 0; attempt < 3; attempt++ {
		epoch := ns.epoch.Load()
		setEpoch(payload, epoch)
		res, err := wbCommitScript.Run(ctx, ns.e.rdb, keys,
			outcome, epochBytes(epoch), key, pw.seq, rev, payload, max(ttl.Milliseconds(), 1), ns.e.opts.InvalidationLogSize).Slice()
		if err != nil {
			return redisErr("commit write-behind flush", err)
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
		return nil
	}
	return fmt.Errorf("%w: namespace epoch keeps changing", ErrCacheUnavailable)
}
