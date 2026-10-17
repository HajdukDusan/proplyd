// Package etcdtest runs an embedded etcd for tests. It is separate from
// proplydtest because it pulls in the etcd server.
package etcdtest

import (
	"net"
	"net/url"
	"strconv"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

// Start runs a single-node etcd and returns a client for it. Both are
// stopped when the test ends.
func Start(t testing.TB) *clientv3.Client {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "panic" // shutdown is noisy otherwise
	clientURL := mustURL(t, "http://127.0.0.1:"+strconv.Itoa(freePort(t)))
	peerURL := mustURL(t, "http://127.0.0.1:"+strconv.Itoa(freePort(t)))
	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.AdvertiseClientUrls = []url.URL{clientURL}
	cfg.ListenPeerUrls = []url.URL{peerURL}
	cfg.AdvertisePeerUrls = []url.URL{peerURL}
	cfg.InitialCluster = cfg.Name + "=" + peerURL.String()

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("starting etcd: %v", err)
	}
	t.Cleanup(e.Close)
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("etcd did not start")
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL.String()},
		DialTimeout: 5 * time.Second,
		Logger:      zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func mustURL(t testing.TB, s string) url.URL {
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return *u
}

func freePort(t testing.TB) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
