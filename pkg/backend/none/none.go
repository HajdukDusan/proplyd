// Package none is a backend for a proplyd that cannot reach the data it
// caches, for example metrics computed from Prometheus queries. Every read
// and write fails with backend.ErrUnavailable, so such a deployment serves
// cache-aside namespaces with SyncNone only: clients load values themselves,
// fill the cache, and staleness is bounded by the namespace TTL. Read-through
// namespaces still work, but every miss falls back to the client's own
// backend.
package none

import (
	"context"
	"fmt"

	"github.com/c12s/proplyd/pkg/backend"
)

// Backend is the backend with no store behind it.
type Backend struct{}

// New returns the backend.
func New() Backend { return Backend{} }

var errNone = fmt.Errorf("%w: proplyd runs without a backing store", backend.ErrUnavailable)

// Get implements backend.Backend; it always fails.
func (Backend) Get(context.Context, string) (backend.Item, error) { return backend.Item{}, errNone }

// Put implements backend.Backend; it always fails.
func (Backend) Put(context.Context, string, []byte) (int64, error) { return 0, errNone }

// Delete implements backend.Backend; it always fails.
func (Backend) Delete(context.Context, string) (int64, bool, error) { return 0, false, errNone }

// Revision implements backend.Backend. There is no store, so its revision
// is 0: a namespace starts with floor 0 and accepts fills at any revision.
func (Backend) Revision(context.Context) (int64, error) { return 0, nil }
