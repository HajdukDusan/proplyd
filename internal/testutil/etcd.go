package testutil

import (
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// StartEtcd starts a single-node embedded etcd and returns a connected
// client. Both are torn down when the test ends.
func StartEtcd(t testing.TB) *clientv3.Client {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Name = "proplyd-test"
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "panic" // etcd logs shutdown noise at error level
	peer, client := freeURL(t), freeURL(t)
	cfg.ListenPeerUrls = []url.URL{peer}
	cfg.AdvertisePeerUrls = []url.URL{peer}
	cfg.ListenClientUrls = []url.URL{client}
	cfg.AdvertiseClientUrls = []url.URL{client}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("start etcd: %v", err)
	}
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		e.Close()
		t.Fatal("etcd did not become ready")
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{client.String()}, DialTimeout: 5 * time.Second})
	if err != nil {
		e.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cli.Close()
		e.Close()
	})
	return cli
}

func freeURL(t testing.TB) url.URL {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	u, _ := url.Parse(fmt.Sprintf("http://%s", addr))
	return *u
}
