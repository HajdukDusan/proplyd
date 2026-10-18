// Package etcd implements backend.Backend and backend.Watcher on etcd v3.
package etcd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/c12s/proplyd/pkg/backend"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Backend is an etcd-backed store. Keys are used as given; wrap it with
// backend.Prefixed to scope it to a prefix.
type Backend struct {
	cli *clientv3.Client
	// ProgressInterval is how often an idle watch asks etcd for a progress
	// notification. Default 1s.
	ProgressInterval time.Duration
}

var (
	_ backend.Backend    = (*Backend)(nil)
	_ backend.Watcher    = (*Backend)(nil)
	_ backend.Identifier = (*Backend)(nil)
	_ backend.CondWriter = (*Backend)(nil)
)

// New returns a backend that uses cli. The caller keeps ownership of cli.
func New(cli *clientv3.Client) *Backend {
	return &Backend{cli: cli, ProgressInterval: time.Second}
}

func (b *Backend) Get(ctx context.Context, key string) (backend.Item, error) {
	resp, err := b.cli.Get(ctx, key)
	if err != nil {
		return backend.Item{}, mapErr(ctx, err)
	}
	item := backend.Item{ReadRevision: resp.Header.Revision}
	if len(resp.Kvs) > 0 {
		kv := resp.Kvs[0]
		item.Found = true
		item.Value = kv.Value
		item.ModRevision = kv.ModRevision
	}
	return item, nil
}

func (b *Backend) Put(ctx context.Context, key string, value []byte) (int64, error) {
	resp, err := b.cli.Put(ctx, key, string(value))
	if err != nil {
		return 0, mapErr(ctx, err)
	}
	return resp.Header.Revision, nil
}

func (b *Backend) Delete(ctx context.Context, key string) (int64, bool, error) {
	resp, err := b.cli.Delete(ctx, key)
	if err != nil {
		return 0, false, mapErr(ctx, err)
	}
	return resp.Header.Revision, resp.Deleted > 0, nil
}

func (b *Backend) Revision(ctx context.Context) (int64, error) {
	// A count-only range on a key that cannot exist is the cheapest way to
	// read the header revision.
	resp, err := b.cli.Get(ctx, "\x00", clientv3.WithCountOnly())
	if err != nil {
		return 0, mapErr(ctx, err)
	}
	return resp.Header.Revision, nil
}

// PutIf implements backend.CondWriter with a transaction on the key's mod
// revision.
func (b *Backend) PutIf(ctx context.Context, key string, value []byte, expect int64) (int64, bool, error) {
	resp, err := b.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", expect)).
		Then(clientv3.OpPut(key, string(value))).
		Commit()
	if err != nil {
		return 0, false, mapErr(ctx, err)
	}
	return resp.Header.Revision, resp.Succeeded, nil
}

// DeleteIf implements backend.CondWriter.
func (b *Backend) DeleteIf(ctx context.Context, key string, expect int64) (int64, bool, error) {
	resp, err := b.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", expect)).
		Then(clientv3.OpDelete(key)).
		Commit()
	if err != nil {
		return 0, false, mapErr(ctx, err)
	}
	return resp.Header.Revision, resp.Succeeded, nil
}

// Identity returns the etcd cluster ID.
func (b *Backend) Identity(ctx context.Context) (string, error) {
	resp, err := b.cli.Get(ctx, "\x00", clientv3.WithCountOnly())
	if err != nil {
		return "", mapErr(ctx, err)
	}
	return fmt.Sprintf("etcd-%x", resp.Header.ClusterId), nil
}

// Watch implements backend.Watcher.
func (b *Backend) Watch(ctx context.Context, prefix string, from int64) <-chan backend.WatchBatch {
	out := make(chan backend.WatchBatch, 16)
	go func() {
		defer close(out)
		send := func(batch backend.WatchBatch) bool {
			select {
			case out <- batch:
				return true
			case <-ctx.Done():
				return false
			}
		}
		wctx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
		defer cancel()
		wch := b.cli.Watch(wctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(from), clientv3.WithProgressNotify())

		interval := b.ProgressInterval
		if interval <= 0 {
			interval = time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		// Ask for a progress notification right away so the consumer learns
		// when the watch has caught up with history.
		_ = b.cli.RequestProgress(wctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = b.cli.RequestProgress(wctx)
			case resp, ok := <-wch:
				if !ok {
					if ctx.Err() == nil {
						send(backend.WatchBatch{Err: backend.Unavailable(errors.New("etcd watch closed"))})
					}
					return
				}
				if resp.CompactRevision != 0 {
					send(backend.WatchBatch{Err: &backend.CompactedError{CompactRevision: resp.CompactRevision}})
					return
				}
				if err := resp.Err(); err != nil {
					send(backend.WatchBatch{Err: backend.Unavailable(err)})
					return
				}
				// Only a progress notification promises that everything up
				// to the header revision was delivered. For an event batch
				// (possibly part of a history replay) the safe bound is the
				// last event's revision.
				var batch backend.WatchBatch
				if resp.IsProgressNotify() {
					batch.Revision = resp.Header.Revision
				}
				for _, ev := range resp.Events {
					e := backend.Event{Key: string(ev.Kv.Key), Revision: ev.Kv.ModRevision}
					if ev.Type == clientv3.EventTypeDelete {
						e.Deleted = true
					} else {
						e.Value = ev.Kv.Value
					}
					batch.Events = append(batch.Events, e)
					batch.Revision = e.Revision
				}
				if batch.Revision == 0 {
					continue // e.g. the "created" response
				}
				if !send(batch) {
					return
				}
			}
		}
	}()
	return out
}

func mapErr(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return ctxErr
	}
	if errors.Is(err, rpctypes.ErrEmptyKey) {
		return err
	}
	return backend.Unavailable(err)
}
