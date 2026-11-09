package none_test

import (
	"context"
	"errors"
	"testing"

	"github.com/c12s/proplyd/pkg/backend"
	"github.com/c12s/proplyd/pkg/backend/none"
)

func TestEverythingUnavailable(t *testing.T) {
	ctx := context.Background()
	b := none.New()
	if _, err := b.Get(ctx, "k"); !errors.Is(err, backend.ErrUnavailable) {
		t.Errorf("Get: %v", err)
	}
	if _, err := b.Put(ctx, "k", []byte("v")); !errors.Is(err, backend.ErrUnavailable) {
		t.Errorf("Put: %v", err)
	}
	if _, _, err := b.Delete(ctx, "k"); !errors.Is(err, backend.ErrUnavailable) {
		t.Errorf("Delete: %v", err)
	}
	if rev, err := b.Revision(ctx); rev != 0 || err != nil {
		t.Errorf("Revision: %d %v", rev, err)
	}
}
