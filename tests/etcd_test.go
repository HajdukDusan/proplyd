package tests

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/c12s/proplyd/backend/etcd"
	"github.com/c12s/proplyd/client"
	"github.com/c12s/proplyd/internal/testutil"
	"github.com/c12s/proplyd/protocol"
)

// TestEndToEndWithEtcd runs the full stack against a real (embedded) etcd:
// gRPC cache server with the etcd backend, and clients that fall back to
// etcd directly.
func TestEndToEndWithEtcd(t *testing.T) {
	if testing.Short() {
		t.Skip("embedded etcd")
	}
	cli := testutil.StartEtcd(t)
	node := testutil.StartCacheNode(t, etcd.NewFactory(cli))
	ctx := context.Background()

	newClient := func(t *testing.T, ns string, spec protocol.Namespace) *client.Client {
		spec.KeyPrefix = "/" + ns + "/"
		return testutil.NewClient(t, node.Addr(), ns, spec, etcd.New(cli, spec.KeyPrefix))
	}
	stored := func(ns, key string) string {
		resp, err := cli.Get(ctx, "/"+ns+"/"+key)
		if err != nil || len(resp.Kvs) == 0 {
			return ""
		}
		return string(resp.Kvs[0].Value)
	}

	t.Run("write-through", func(t *testing.T) {
		c := newClient(t, "wt", protocol.Namespace{Write: protocol.WriteThrough})
		if err := c.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		if stored("wt", "k") != "v" {
			t.Fatal("not in etcd")
		}
		if it, _ := c.Lookup(ctx, "k"); it.Source != client.SourceCache || string(it.Value) != "v" {
			t.Fatalf("%+v", it)
		}
	})

	t.Run("write-behind batches into etcd transactions", func(t *testing.T) {
		c := newClient(t, "wb", protocol.Namespace{Write: protocol.WriteBehind, FlushInterval: 20 * time.Millisecond})
		for i := range 300 {
			if err := c.Set(ctx, "k", []byte(strconv.Itoa(i))); err != nil {
				t.Fatal(err)
			}
			c.Set(ctx, string(rune('a'+i%26)), []byte("x"))
		}
		testutil.Eventually(t, 5*time.Second, func() bool {
			st, _ := c.Stats(ctx)
			return st != nil && st.PendingWrites == 0
		}, "flushed")
		if stored("wb", "k") != "299" {
			t.Fatal("last write-behind value not persisted")
		}
	})

	// A client partitioned from the cache writes to etcd directly; the
	// cache's etcd watch updates the cached entry for everyone else.
	t.Run("write-around partition fallback + watch", func(t *testing.T) {
		spec := protocol.Namespace{Write: protocol.WriteAround}
		link, err := testutil.NewProxy(node.Addr())
		if err != nil {
			t.Fatal(err)
		}
		defer link.Close()
		spec.KeyPrefix = "/wa/"
		a := testutil.NewClient(t, link.Addr(), "wa", spec, etcd.New(cli, spec.KeyPrefix))
		b := newClient(t, "wa", protocol.Namespace{Write: protocol.WriteAround})

		a.Set(ctx, "k", []byte("v1"))
		b.Get(ctx, "k")
		if it, _ := b.Lookup(ctx, "k"); it.Source != client.SourceCache || string(it.Value) != "v1" {
			t.Fatalf("%+v", it)
		}

		link.SetMode(testutil.Blackhole)
		if err := a.Set(ctx, "k", []byte("v2")); err != nil {
			t.Fatal(err)
		}
		testutil.Eventually(t, 3*time.Second, func() bool {
			it, _ := b.Lookup(ctx, "k")
			return it.Source == client.SourceCache && string(it.Value) == "v2"
		}, "watch applied the direct write")
	})

	t.Run("write-through rejects while cache is down", func(t *testing.T) {
		c := newClient(t, "wt2", protocol.Namespace{Write: protocol.WriteThrough})
		c.Set(ctx, "k", []byte("v1"))
		node.Kill()
		defer node.Start()
		if err := c.Set(ctx, "k", []byte("v2")); !errors.Is(err, client.ErrCacheUnavailable) {
			t.Fatalf("want ErrCacheUnavailable, got %v", err)
		}
		if stored("wt2", "k") != "v1" {
			t.Fatal("rejected write reached etcd")
		}
		if v, _, err := c.Get(ctx, "k"); err != nil || string(v) != "v1" {
			t.Fatalf("fallback read: %q %v", v, err)
		}
	})

	t.Run("cache follows etcd across compaction", func(t *testing.T) {
		c := newClient(t, "cmp", protocol.Namespace{Write: protocol.WriteAround})
		c.Set(ctx, "k", []byte("v1"))
		c.Get(ctx, "k")
		st0, _ := c.Stats(ctx)

		// A live watch keeps working across compaction. (A watch that must
		// reconnect past the compacted revision resyncs; that path is covered
		// by backend/etcd TestWatch and cache TestWatchResyncsAfterCompaction.)
		resp, _ := cli.Put(ctx, "/cmp/k", "v2")
		if _, err := cli.Compact(ctx, resp.Header.Revision); err != nil {
			t.Fatal(err)
		}
		testutil.Eventually(t, 3*time.Second, func() bool {
			v, _, _ := c.Get(ctx, "k")
			return string(v) == "v2"
		}, "cache follows etcd")
		st1, _ := c.Stats(ctx)
		if st1.WatchEvents <= st0.WatchEvents {
			t.Fatalf("expected watch events, before %d after %d", st0.WatchEvents, st1.WatchEvents)
		}
	})
}
