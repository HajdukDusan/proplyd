// Package etcd implements backend.Backend on top of an etcd v3 cluster.
package etcd

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/c12s/proplyd/backend"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Backend stores keys under a fixed prefix of an etcd cluster.
type Backend struct {
	cli    *clientv3.Client
	prefix string
	owned  bool
}

var (
	_ backend.Backend = (*Backend)(nil)
	_ backend.Watcher = (*Backend)(nil)
)

// New returns a backend that stores keys under prefix. The client is shared
// and not closed by Close.
func New(cli *clientv3.Client, prefix string) *Backend {
	return &Backend{cli: cli, prefix: prefix}
}

// Dial connects to etcd and returns a backend that owns the connection.
func Dial(cfg clientv3.Config, prefix string) (*Backend, error) {
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("etcd: dial: %w", err)
	}
	return &Backend{cli: cli, prefix: prefix, owned: true}, nil
}

// NewFactory returns a backend.Factory sharing one etcd client across all
// namespaces.
func NewFactory(cli *clientv3.Client) backend.Factory {
	return func(prefix string) (backend.Backend, error) {
		return New(cli, prefix), nil
	}
}

func (b *Backend) Get(ctx context.Context, key string) (backend.Record, error) {
	resp, err := b.cli.Get(ctx, b.prefix+key)
	if err != nil {
		return backend.Record{}, err
	}
	rec := backend.Record{Revision: resp.Header.Revision}
	if len(resp.Kvs) > 0 {
		kv := resp.Kvs[0]
		rec.Found = true
		rec.Value = kv.Value
		rec.ModRevision = kv.ModRevision
	}
	return rec, nil
}

func (b *Backend) Put(ctx context.Context, key string, value []byte) (int64, error) {
	resp, err := b.cli.Put(ctx, b.prefix+key, string(value))
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

func (b *Backend) Delete(ctx context.Context, key string) (int64, error) {
	resp, err := b.cli.Delete(ctx, b.prefix+key)
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

func (b *Backend) Apply(ctx context.Context, ops []backend.Op) (int64, error) {
	if len(ops) == 0 {
		return b.CurrentRevision(ctx)
	}
	etcdOps := make([]clientv3.Op, len(ops))
	for i, op := range ops {
		if op.Delete {
			etcdOps[i] = clientv3.OpDelete(b.prefix + op.Key)
		} else {
			etcdOps[i] = clientv3.OpPut(b.prefix+op.Key, string(op.Value))
		}
	}
	resp, err := b.cli.Txn(ctx).Then(etcdOps...).Commit()
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

func (b *Backend) CurrentRevision(ctx context.Context) (int64, error) {
	// A point read of a single key is the cheapest way to obtain the header.
	resp, err := b.cli.Get(ctx, b.prefix, clientv3.WithCountOnly())
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

func (b *Backend) Watch(ctx context.Context, fromRev int64) <-chan backend.WatchResponse {
	out := make(chan backend.WatchResponse, 16)
	go func() {
		defer close(out)
		send := func(r backend.WatchResponse) bool {
			select {
			case out <- r:
				return true
			case <-ctx.Done():
				return false
			}
		}

		wctx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
		defer cancel()
		wch := b.cli.Watch(wctx, b.prefix, clientv3.WithPrefix(), clientv3.WithRev(fromRev))
		for wr := range wch {
			if wr.CompactRevision != 0 || errors.Is(wr.Err(), rpctypes.ErrCompacted) {
				send(backend.WatchResponse{Err: backend.ErrCompacted})
				return
			}
			if err := wr.Err(); err != nil {
				send(backend.WatchResponse{Err: err})
				return
			}
			if wr.IsProgressNotify() || len(wr.Events) == 0 {
				continue
			}
			events := make([]backend.Event, 0, len(wr.Events))
			for _, ev := range wr.Events {
				e := backend.Event{
					Key:         strings.TrimPrefix(string(ev.Kv.Key), b.prefix),
					ModRevision: ev.Kv.ModRevision,
				}
				if ev.Type == clientv3.EventTypeDelete {
					e.Type = backend.EventDelete
				} else {
					e.Type = backend.EventPut
					e.Value = ev.Kv.Value
				}
				events = append(events, e)
			}
			if !send(backend.WatchResponse{Events: events}) {
				return
			}
		}
		if ctx.Err() == nil {
			send(backend.WatchResponse{Err: errors.New("etcd: watch channel closed")})
		}
	}()
	return out
}

// Close closes the etcd client if the backend owns it.
func (b *Backend) Close() error {
	if b.owned {
		return b.cli.Close()
	}
	return nil
}
