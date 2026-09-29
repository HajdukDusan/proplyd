// Package backend defines the persistent store the cache sits in front of.
//
// A Backend is scoped to a single key prefix: keys passed to and returned by
// it are relative to that prefix. Every mutation yields a monotonically
// increasing store revision (etcd's cluster revision), which the cache uses to
// order concurrent writes, loads and change notifications.
package backend

import (
	"context"
	"errors"
)

var (
	// ErrCompacted is returned on a watch channel when the requested start
	// revision is no longer available. The watcher must resynchronise.
	ErrCompacted = errors.New("backend: revision compacted")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("backend: closed")
)

// Record is the result of a read.
type Record struct {
	Value []byte
	Found bool
	// ModRevision is the revision that last modified the key (0 if not found).
	ModRevision int64
	// Revision is the store revision the read was served at. Any change with a
	// higher revision is not reflected in this record.
	Revision int64
}

// Op is a single mutation inside an atomic batch.
type Op struct {
	Key    string
	Value  []byte
	Delete bool
}

// EventType is the kind of change reported by a watch.
type EventType int

const (
	EventPut EventType = iota
	EventDelete
)

// Event is a single change observed by a watch.
type Event struct {
	Type        EventType
	Key         string
	Value       []byte
	ModRevision int64
}

// WatchResponse carries a batch of events or a terminal error. After a
// response with a non-nil Err the channel is closed.
type WatchResponse struct {
	Events []Event
	Err    error
}

// Backend is the persistent store. Implementations must be safe for
// concurrent use.
type Backend interface {
	Get(ctx context.Context, key string) (Record, error)
	// Put stores a value and returns the revision of the write.
	Put(ctx context.Context, key string, value []byte) (int64, error)
	// Delete removes a key and returns the store revision after the call.
	Delete(ctx context.Context, key string) (int64, error)
	// Apply atomically applies all ops (keys must be unique) and returns the
	// revision of the batch.
	Apply(ctx context.Context, ops []Op) (int64, error)
	Close() error
}

// Watcher is implemented by backends that can stream changes.
type Watcher interface {
	// CurrentRevision returns the latest store revision.
	CurrentRevision(ctx context.Context) (int64, error)
	// Watch streams every change under the prefix with revision >= fromRev.
	// The channel is closed when ctx is done or after a response with Err set.
	Watch(ctx context.Context, fromRev int64) <-chan WatchResponse
}

// Factory creates a backend for a namespace key prefix. The cache server uses
// it to open one backend per registered namespace.
type Factory func(prefix string) (Backend, error)
