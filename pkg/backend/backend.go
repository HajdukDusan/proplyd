// Package backend abstracts the backing store (the database the cache sits
// in front of). The model is etcd's: a flat key space with a global,
// monotonically increasing revision and a change stream.
//
// The server uses a Backend to load misses, write and follow changes. Clients
// use one to keep working when the cache is unreachable.
package backend

import (
	"context"
	"errors"
	"fmt"
)

// Item is the result of a read.
type Item struct {
	Found bool
	Value []byte
	// ModRevision is the revision of the last change to the key (0 when not
	// found).
	ModRevision int64
	// ReadRevision is the store revision the read observed. It is at least
	// ModRevision.
	ReadRevision int64
}

// Backend is a key/value store with revisions.
//
// Errors caused by the store being unreachable or failing must wrap
// ErrUnavailable. Context errors are returned as they are.
type Backend interface {
	Get(ctx context.Context, key string) (Item, error)
	// Put writes key and returns the revision of the write.
	Put(ctx context.Context, key string, value []byte) (int64, error)
	// Delete removes key. It returns the store revision after the delete and
	// whether the key existed.
	Delete(ctx context.Context, key string) (rev int64, existed bool, err error)
	// Revision returns the current store revision.
	Revision(ctx context.Context) (int64, error)
}

// Event is a change to a key.
type Event struct {
	Key     string
	Value   []byte
	Deleted bool
	// Revision of the change.
	Revision int64
}

// WatchBatch is one message of a watch: events in revision order, or a
// progress notification (no events), or a terminal error.
type WatchBatch struct {
	Events []Event
	// Every event at or below Revision has been delivered.
	Revision int64
	Err      error
}

// Watcher is implemented by backends with a change stream.
type Watcher interface {
	// Watch streams changes to keys under prefix, starting at revision from
	// (inclusive). The channel is closed when ctx is done or after a batch
	// with Err set. An error wrapping *CompactedError means from is no
	// longer available.
	//
	// Implementations must send a progress batch soon after the watch has
	// caught up with history, and periodically afterwards, so that a
	// consumer learns how far it is synced even when nothing changes.
	Watch(ctx context.Context, prefix string, from int64) <-chan WatchBatch
}

// ErrUnavailable marks errors of an unreachable or failing store.
var ErrUnavailable = errors.New("backend unavailable")

// Unavailable wraps err so that it matches ErrUnavailable.
func Unavailable(err error) error {
	if err == nil || errors.Is(err, ErrUnavailable) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrUnavailable, err)
}

// CompactedError reports that a watch start revision was compacted away.
type CompactedError struct {
	// CompactRevision is the oldest revision still available.
	CompactRevision int64
}

func (e *CompactedError) Error() string {
	return fmt.Sprintf("revision compacted (oldest available: %d)", e.CompactRevision)
}

// Prefixed returns a Backend that maps key to prefix+key. Clients use it to
// access their namespace's keys directly.
func Prefixed(b Backend, prefix string) Backend {
	return prefixed{b: b, prefix: prefix}
}

type prefixed struct {
	b      Backend
	prefix string
}

func (p prefixed) Get(ctx context.Context, key string) (Item, error) {
	return p.b.Get(ctx, p.prefix+key)
}

func (p prefixed) Put(ctx context.Context, key string, value []byte) (int64, error) {
	return p.b.Put(ctx, p.prefix+key, value)
}

func (p prefixed) Delete(ctx context.Context, key string) (int64, bool, error) {
	return p.b.Delete(ctx, p.prefix+key)
}

func (p prefixed) Revision(ctx context.Context) (int64, error) { return p.b.Revision(ctx) }
