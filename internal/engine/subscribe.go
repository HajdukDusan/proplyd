package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/c12s/proplyd/pkg/api"
	"github.com/redis/go-redis/v9"
)

// Invalidations reach near-caches like this:
//
//	installScript ──XADD──▶ Redis stream ──XREAD──▶ tailer ──▶ subscribers ──gRPC──▶ clients
//
// The stream is the source of truth, so subscribers connected to different
// engines see the same events in the same order, and a subscriber that
// reconnects resumes from its last position. Resuming is refused (and the
// subscriber told to reset) when the epoch changed, when entries after its
// position were trimmed, or when the stream is behind its position (Redis
// rolled back).

// streamID is a Redis stream entry ID.
type streamID struct{ ms, seq uint64 }

func parseStreamID(s string) (streamID, bool) {
	a, b, ok := strings.Cut(s, "-")
	if !ok {
		return streamID{}, false
	}
	ms, err1 := strconv.ParseUint(a, 10, 64)
	seq, err2 := strconv.ParseUint(b, 10, 64)
	return streamID{ms, seq}, err1 == nil && err2 == nil
}

func (id streamID) String() string { return fmt.Sprintf("%d-%d", id.ms, id.seq) }

func (id streamID) less(o streamID) bool {
	return id.ms < o.ms || (id.ms == o.ms && id.seq < o.seq)
}

// streamMsg is a parsed stream entry.
type streamMsg struct {
	id    streamID
	key   string
	rev   int64
	reset bool
	epoch uint64 // reset only
}

func parseStreamMsg(m redis.XMessage) (streamMsg, bool) {
	id, ok := parseStreamID(m.ID)
	if !ok {
		return streamMsg{}, false
	}
	if x, ok := m.Values["x"].(string); ok {
		epoch, _ := parseEpoch(x)
		return streamMsg{id: id, reset: true, epoch: epoch}, true
	}
	k, _ := m.Values["k"].(string)
	r, _ := m.Values["r"].(string)
	rev, err := strconv.ParseInt(r, 10, 64)
	if k == "" || err != nil {
		return streamMsg{}, false
	}
	return streamMsg{id: id, key: k, rev: rev}, true
}

// EpochString formats an epoch for the API.
func EpochString(epoch uint64) string { return fmt.Sprintf("%016x", epoch) }

type subscriber struct {
	ch    chan []streamMsg
	ticks chan struct{}
	done  chan struct{}
	once  sync.Once
	err   error
	after streamID
}

func newSubscriber() *subscriber {
	return &subscriber{ch: make(chan []streamMsg, 64), ticks: make(chan struct{}, 1), done: make(chan struct{})}
}

func (s *subscriber) close(err error) {
	s.once.Do(func() {
		s.err = err
		close(s.done)
	})
}

// tailer reads a namespace's stream and fans it out to the subscribers
// connected to this engine. It runs while there are subscribers.
type tailer struct {
	ns *namespace

	mu      sync.Mutex
	subs    map[*subscriber]struct{}
	running bool
	last    streamID // last entry read
	epoch   uint64   // epoch the subscribers were last told about
}

// attach registers sub and returns the position from which the tailer will
// deliver to it.
func (t *tailer) attach(ctx context.Context, sub *subscriber) (streamID, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ns.ctx.Err() != nil {
		return streamID{}, fmt.Errorf("%w: namespace reloading", ErrCacheUnavailable)
	}
	// Checked under t.mu: DrainSubscriptions sets the flag before taking
	// t.mu to close subscribers, so no subscriber slips through.
	if t.ns.e.draining.Load() {
		return streamID{}, fmt.Errorf("%w: server shutting down", ErrCacheUnavailable)
	}
	if !t.running {
		info, err := streamInfo(ctx, t.ns.e.rdb, t.ns.ks.stream)
		if err != nil {
			return streamID{}, err
		}
		epochStr, err := t.ns.e.rdb.HGet(ctx, t.ns.ks.meta, "epoch").Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return streamID{}, ErrNamespaceNotFound
			}
			return streamID{}, redisErr("read epoch", err)
		}
		t.last = info.last
		t.epoch, _ = parseEpoch(epochStr)
		t.running = true
		if t.subs == nil {
			t.subs = map[*subscriber]struct{}{}
		}
		t.ns.wg.Add(1)
		go t.run()
	}
	sub.after = t.last
	t.subs[sub] = struct{}{}
	return t.last, nil
}

func (t *tailer) detach(sub *subscriber) {
	t.mu.Lock()
	delete(t.subs, sub)
	t.mu.Unlock()
}

// closeAll ends every subscription with err. Subscribers reconnect and
// resume.
func (t *tailer) closeAll(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for s := range t.subs {
		s.close(err)
		delete(t.subs, s)
	}
}

const tailerIdle = 5 * time.Second

func (t *tailer) run() {
	defer t.ns.wg.Done()
	ns := t.ns
	var idleSince time.Time
	for {
		t.mu.Lock()
		if len(t.subs) == 0 {
			if idleSince.IsZero() {
				idleSince = time.Now()
			} else if time.Since(idleSince) > tailerIdle {
				t.running = false
				t.mu.Unlock()
				return
			}
		} else {
			idleSince = time.Time{}
		}
		last := t.last
		t.mu.Unlock()

		msgs, err := t.poll(last)
		if err != nil {
			t.mu.Lock()
			t.running = false
			for s := range t.subs {
				s.close(err)
				delete(t.subs, s)
			}
			t.mu.Unlock()
			if ns.ctx.Err() == nil && !errors.Is(err, ErrNamespaceNotFound) {
				ns.e.log.Warn("invalidation stream interrupted", "namespace", ns.name, "err", err)
			}
			return
		}
		t.mu.Lock()
		if len(msgs) > 0 {
			t.last = msgs[len(msgs)-1].id
		}
		for s := range t.subs {
			if !t.deliver(s, msgs) {
				s.close(fmt.Errorf("%w: subscriber too slow", ErrCacheUnavailable))
				delete(t.subs, s)
				continue
			}
			select {
			case s.ticks <- struct{}{}:
			default:
			}
		}
		t.mu.Unlock()
	}
}

// poll waits for entries after last. It also checks that the namespace
// still exists and turns an epoch change without a reset marker (the
// namespace was recreated) into a reset.
func (t *tailer) poll(last streamID) ([]streamMsg, error) {
	ns := t.ns
	ctx := ns.ctx
	epochStr, err := ns.e.rdb.HGet(ctx, ns.ks.meta, "epoch").Result()
	if errors.Is(err, redis.Nil) {
		ns.lost()
		return nil, ErrNamespaceNotFound
	}
	if err != nil {
		return nil, redisErr("read epoch", err)
	}
	var out []streamMsg
	if epoch, _ := parseEpoch(epochStr); epoch != t.epoch {
		out = append(out, streamMsg{id: last, reset: true, epoch: epoch})
		t.epoch = epoch
	}
	res, err := ns.e.rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{ns.ks.stream, last.String()},
		Count:   512,
		Block:   ns.e.opts.HeartbeatInterval,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, redisErr("read invalidations", err)
	}
	for _, st := range res {
		for _, m := range st.Messages {
			msg, ok := parseStreamMsg(m)
			if !ok {
				continue
			}
			if msg.reset {
				if msg.epoch == t.epoch {
					continue // already announced
				}
				t.epoch = msg.epoch
			}
			out = append(out, msg)
		}
	}
	return out, nil
}

func (t *tailer) deliver(s *subscriber, msgs []streamMsg) bool {
	var batch []streamMsg
	for _, m := range msgs {
		if s.after.less(m.id) || (m.reset && !m.id.less(s.after)) {
			batch = append(batch, m)
		}
	}
	if len(batch) == 0 {
		return true
	}
	select {
	case s.ch <- batch:
		return true
	default:
		return false
	}
}

type streamState struct {
	last streamID
	// complete: no entry was ever trimmed.
	complete bool
}

func streamInfo(ctx context.Context, rdb redis.UniversalClient, key string) (streamState, error) {
	info, err := rdb.XInfoStream(ctx, key).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return streamState{complete: true}, nil
		}
		return streamState{}, redisErr("stream info", err)
	}
	var st streamState
	st.last, _ = parseStreamID(info.LastGeneratedID)
	st.complete = info.EntriesAdded == info.Length
	return st, nil
}

// resumable reports whether every entry after pos is still in the stream.
// Positions always name real entries (or 0-0 for "before the first one"),
// and trimming removes the oldest entries first, so it is enough that the
// entry at pos still exists. After a rollback it does not.
//
// (max-deleted-entry-id cannot be used: Redis only advances it on XDEL,
// not when trimming.)
func resumable(ctx context.Context, rdb redis.UniversalClient, key string, pos streamID) (bool, error) {
	if pos == (streamID{}) {
		st, err := streamInfo(ctx, rdb, key)
		return st.complete, err
	}
	res, err := rdb.XRangeN(ctx, key, pos.String(), pos.String(), 1).Result()
	if err != nil {
		return false, redisErr("read invalidations", err)
	}
	return len(res) == 1, nil
}

// Subscribe streams invalidations of a namespace through send until ctx
// ends or the stream breaks. epoch and position are what the subscriber
// last saw ("" for a fresh subscription).
func (e *Engine) Subscribe(ctx context.Context, name, epoch, position string, send func(*api.SubscribeResponse) error) error {
	ns, err := e.namespace(ctx, name)
	if err != nil {
		return err
	}
	if err := e.checkUp(); err != nil {
		return err
	}
	if err := ns.waitReady(ctx); err != nil {
		return err
	}
	sub := newSubscriber()
	start, err := ns.tail.attach(ctx, sub)
	if err != nil {
		return err
	}
	defer ns.tail.detach(sub)

	m, err := readMeta(ctx, e.rdb, ns.ks)
	if err != nil {
		return err
	}
	curEpoch := EpochString(m.epoch)
	pos, posOK := parseStreamID(position)
	resume := posOK && epoch == curEpoch
	if resume {
		if resume, err = resumable(ctx, e.rdb, ns.ks.stream, pos); err != nil {
			return err
		}
	}

	delivered := start
	lastSend := time.Now()
	sendMsgs := func(msgs []streamMsg) error {
		var invs []*api.Invalidation
		var at streamID
		flush := func() error {
			if len(invs) == 0 {
				return nil
			}
			err := send(&api.SubscribeResponse{Kind: api.SubscribeResponse_KIND_INVALIDATIONS, Epoch: curEpoch, Position: at.String(), Invalidations: invs})
			invs = nil
			return err
		}
		for _, msg := range msgs {
			if msg.reset {
				if err := flush(); err != nil {
					return err
				}
				curEpoch = EpochString(msg.epoch)
				if err := send(&api.SubscribeResponse{Kind: api.SubscribeResponse_KIND_RESET, Epoch: curEpoch, Position: msg.id.String()}); err != nil {
					return err
				}
				continue
			}
			invs = append(invs, &api.Invalidation{Key: msg.key, Revision: msg.rev})
			at = msg.id
		}
		lastSend = time.Now()
		return flush()
	}

	if !resume {
		if err := send(&api.SubscribeResponse{Kind: api.SubscribeResponse_KIND_RESET, Epoch: curEpoch, Position: start.String()}); err != nil {
			return err
		}
	} else {
		// Catch up on (pos, start]; the tailer delivers what follows.
		for cursor := pos; cursor.less(start); {
			res, err := e.rdb.XRangeN(ctx, ns.ks.stream, "("+cursor.String(), start.String(), 512).Result()
			if err != nil {
				return redisErr("read invalidations", err)
			}
			if len(res) == 0 {
				break
			}
			var msgs []streamMsg
			for _, m := range res {
				if msg, ok := parseStreamMsg(m); ok {
					msgs = append(msgs, msg)
				}
			}
			cursor, _ = parseStreamID(res[len(res)-1].ID)
			if err := sendMsgs(msgs); err != nil {
				return err
			}
		}
		if start.less(pos) {
			delivered = pos
		}
		// Tell the subscriber it is live, even if nothing changed.
		if err := send(&api.SubscribeResponse{Kind: api.SubscribeResponse_KIND_HEARTBEAT, Epoch: curEpoch, Position: delivered.String()}); err != nil {
			return err
		}
		lastSend = time.Now()
	}

	for {
		select {
		case msgs := <-sub.ch:
			var fresh []streamMsg
			for _, m := range msgs {
				if delivered.less(m.id) || m.reset {
					fresh = append(fresh, m)
				}
			}
			if len(fresh) == 0 {
				continue
			}
			if err := sendMsgs(fresh); err != nil {
				return err
			}
			if last := fresh[len(fresh)-1].id; delivered.less(last) {
				delivered = last
			}
		case <-sub.ticks:
			// Ticks come once per tailer poll (about one per interval).
			if time.Since(lastSend) < e.opts.HeartbeatInterval/2 {
				continue
			}
			if err := send(&api.SubscribeResponse{Kind: api.SubscribeResponse_KIND_HEARTBEAT, Epoch: curEpoch, Position: delivered.String()}); err != nil {
				return err
			}
			lastSend = time.Now()
		case <-sub.done:
			return sub.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
