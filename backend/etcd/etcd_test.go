package etcd_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/c12s/proplyd/backend"
	"github.com/c12s/proplyd/backend/etcd"
	"github.com/c12s/proplyd/internal/testutil"
)

func TestBackend(t *testing.T) {
	if testing.Short() {
		t.Skip("embedded etcd")
	}
	cli := testutil.StartEtcd(t)
	b := etcd.New(cli, "/ns/")
	ctx := context.Background()

	rec, err := b.Get(ctx, "k")
	if err != nil || rec.Found || rec.Revision == 0 {
		t.Fatalf("missing key: %+v %v", rec, err)
	}
	rev1, err := b.Put(ctx, "k", []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	rec, _ = b.Get(ctx, "k")
	if !rec.Found || string(rec.Value) != "v1" || rec.ModRevision != rev1 || rec.Revision < rev1 {
		t.Fatalf("get: %+v", rec)
	}
	// Keys are scoped to the prefix.
	if other, _ := etcd.New(cli, "/other/").Get(ctx, "k"); other.Found {
		t.Fatal("prefix leak")
	}

	rev2, err := b.Apply(ctx, []backend.Op{{Key: "a", Value: []byte("1")}, {Key: "k", Delete: true}})
	if err != nil || rev2 <= rev1 {
		t.Fatalf("apply: %d %v", rev2, err)
	}
	if rec, _ := b.Get(ctx, "a"); rec.ModRevision != rev2 {
		t.Fatalf("batch not atomic at one revision: %+v", rec)
	}
	if cur, _ := b.CurrentRevision(ctx); cur != rev2 {
		t.Fatalf("current revision %d, want %d", cur, rev2)
	}
}

func TestWatch(t *testing.T) {
	if testing.Short() {
		t.Skip("embedded etcd")
	}
	cli := testutil.StartEtcd(t)
	b := etcd.New(cli, "/ns/")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start, _ := b.CurrentRevision(ctx)
	b.Put(ctx, "a", []byte("1"))
	b.Delete(ctx, "a")
	etcd.New(cli, "/other/").Put(ctx, "x", []byte("ignored"))

	// Watching from an old revision replays history.
	ch := b.Watch(ctx, start+1)
	var got []backend.Event
	for len(got) < 2 {
		select {
		case resp := <-ch:
			if resp.Err != nil {
				t.Fatal(resp.Err)
			}
			got = append(got, resp.Events...)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout, got %+v", got)
		}
	}
	if got[0].Type != backend.EventPut || got[0].Key != "a" || string(got[0].Value) != "1" || got[1].Type != backend.EventDelete {
		t.Fatalf("events %+v", got)
	}

	// Compaction past the requested revision is reported as ErrCompacted.
	cur, _ := b.CurrentRevision(ctx)
	if _, err := cli.Compact(ctx, cur); err != nil {
		t.Fatal(err)
	}
	select {
	case resp := <-b.Watch(ctx, start+1):
		if !errors.Is(resp.Err, backend.ErrCompacted) {
			t.Fatalf("want ErrCompacted, got %+v", resp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for compaction error")
	}
}
