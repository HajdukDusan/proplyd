// Package backendtest is a conformance suite for backend.Backend
// implementations: every backend must behave like etcd in the ways the
// cache relies on.
package backendtest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/c12s/proplyd/pkg/backend"
)

// Watching is a backend with a change stream and conditional writes.
type Watching interface {
	backend.Backend
	backend.Watcher
	backend.CondWriter
}

// Run checks reads, writes and revisions, history replay and progress
// notifications of watches, and that watching compacted history fails with
// a CompactedError. compact must compact the history up to rev.
func Run(t *testing.T, b Watching, compact func(rev int64)) {
	t.Helper()
	ctx := context.Background()

	rev, err := b.Put(ctx, "/p/a", []byte("1"))
	if err != nil {
		t.Fatal(err)
	}
	it, err := b.Get(ctx, "/p/a")
	if err != nil || !it.Found || string(it.Value) != "1" || it.ModRevision != rev || it.ReadRevision < rev {
		t.Fatalf("get: %+v %v", it, err)
	}
	if cur, err := b.Revision(ctx); err != nil || cur != rev {
		t.Fatalf("revision %d %v, want %d", cur, err, rev)
	}
	missing, err := b.Get(ctx, "/p/none")
	if err != nil || missing.Found || missing.ReadRevision != rev {
		t.Fatalf("missing: %+v %v", missing, err)
	}

	// The watch replays history from a revision, then reports progress.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := b.Watch(wctx, "/p/", rev)
	drev, existed, err := b.Delete(ctx, "/p/a")
	if err != nil || !existed {
		t.Fatalf("delete: %v %v", existed, err)
	}
	if _, err := b.Put(ctx, "/other/x", []byte("ignored")); err != nil {
		t.Fatal(err)
	}
	var events []backend.Event
	var progress int64
	deadline := time.After(10 * time.Second)
	for len(events) < 2 || progress < drev+1 {
		select {
		case batch := <-ch:
			if batch.Err != nil {
				t.Fatal(batch.Err)
			}
			events = append(events, batch.Events...)
			progress = max(progress, batch.Revision)
		case <-deadline:
			t.Fatalf("events %+v, progress %d", events, progress)
		}
	}
	if events[0].Revision != rev || events[0].Deleted || !events[1].Deleted || events[1].Revision != drev {
		t.Fatalf("events %+v", events)
	}

	// Watching compacted history fails with CompactedError.
	compact(drev)
	batch := <-b.Watch(ctx, "/p/", rev)
	var ce *backend.CompactedError
	if !errors.As(batch.Err, &ce) {
		t.Fatalf("watch from compacted revision: %+v", batch)
	}

	// Conditional writes compare the key's mod revision.
	if _, ok, err := b.PutIf(ctx, "/c/x", []byte("1"), 5); err != nil || ok {
		t.Fatalf("PutIf on a missing key expecting 5: ok=%v err=%v", ok, err)
	}
	r1, ok, err := b.PutIf(ctx, "/c/x", []byte("1"), 0)
	if err != nil || !ok {
		t.Fatalf("PutIf create: ok=%v err=%v", ok, err)
	}
	if _, ok, _ := b.PutIf(ctx, "/c/x", []byte("2"), 0); ok {
		t.Fatal("PutIf expecting absence overwrote an existing key")
	}
	r2, ok, err := b.PutIf(ctx, "/c/x", []byte("2"), r1)
	if err != nil || !ok || r2 <= r1 {
		t.Fatalf("PutIf update: rev=%d ok=%v err=%v", r2, ok, err)
	}
	if it, _ := b.Get(ctx, "/c/x"); string(it.Value) != "2" || it.ModRevision != r2 {
		t.Fatalf("after PutIf: %+v", it)
	}
	if _, ok, _ := b.DeleteIf(ctx, "/c/x", r1); ok {
		t.Fatal("DeleteIf with a stale revision deleted")
	}
	if _, ok, err := b.DeleteIf(ctx, "/c/x", r2); err != nil || !ok {
		t.Fatalf("DeleteIf: ok=%v err=%v", ok, err)
	}
	if _, ok, err := b.DeleteIf(ctx, "/c/x", 0); err != nil || !ok {
		t.Fatalf("DeleteIf on a missing key expecting absence: ok=%v err=%v", ok, err)
	}
}
