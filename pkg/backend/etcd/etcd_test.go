package etcd_test

import (
	"context"
	"testing"
	"time"

	"github.com/c12s/proplyd/pkg/backend/backendtest"
	"github.com/c12s/proplyd/pkg/backend/etcd"
	"github.com/c12s/proplyd/proplydtest/etcdtest"
)

func TestConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("embedded etcd")
	}
	cli := etcdtest.Start(t)
	b := etcd.New(cli)
	b.ProgressInterval = 100 * time.Millisecond
	backendtest.Run(t, b, func(rev int64) {
		if _, err := cli.Compact(context.Background(), rev); err != nil {
			t.Fatal(err)
		}
	})
}
