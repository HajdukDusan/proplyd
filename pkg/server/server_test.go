package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/c12s/proplyd/pkg/api"
	"github.com/c12s/proplyd/pkg/backend"
	"github.com/c12s/proplyd/pkg/backend/etcd"
	"github.com/c12s/proplyd/pkg/backend/faulty"
	"github.com/c12s/proplyd/pkg/backend/memory"
	"github.com/c12s/proplyd/pkg/protocol"
	"github.com/c12s/proplyd/proplydtest"
	"github.com/c12s/proplyd/proplydtest/etcdtest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type fixture struct {
	redis *proplydtest.Redis
	node  *proplydtest.Node
	api   api.CacheServiceClient
	conn  *grpc.ClientConn
}

func start(t *testing.T, be backend.Backend) *fixture {
	t.Helper()
	r := proplydtest.StartRedis(t, proplydtest.NoPersistence)
	node := proplydtest.StartNode(t, proplydtest.NodeConfig{RedisAddr: r.Addr(), Backend: be})
	conn, err := grpc.NewClient(node.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &fixture{redis: r, node: node, api: api.NewCacheServiceClient(conn), conn: conn}
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

func expectStatus(t *testing.T, err error, code codes.Code, reason string) {
	t.Helper()
	if status.Code(err) != code || protocol.Reason(err) != reason {
		t.Fatalf("got %v (reason %q), want %v %s", err, protocol.Reason(err), code, reason)
	}
}

func register(t *testing.T, f *fixture, name string, spec protocol.Spec) {
	t.Helper()
	if _, err := f.api.Register(ctx(t), &api.RegisterRequest{Namespace: name, Spec: spec.ToProto()}); err != nil {
		t.Fatal(err)
	}
}

func TestErrorMapping(t *testing.T) {
	be := faulty.New(memory.New(memory.Options{}))
	f := start(t, be)
	register(t, f, "ns", protocol.Spec{KeyPrefix: "/ns/"})

	_, err := f.api.Get(ctx(t), &api.GetRequest{Namespace: "unknown", Key: "k"})
	expectStatus(t, err, codes.NotFound, protocol.ReasonNamespaceNotFound)

	_, err = f.api.Register(ctx(t), &api.RegisterRequest{Namespace: "ns", Spec: protocol.Spec{KeyPrefix: "/other/"}.ToProto()})
	expectStatus(t, err, codes.FailedPrecondition, protocol.ReasonNamespaceConflict)

	_, err = f.api.Get(ctx(t), &api.GetRequest{Namespace: "ns", Key: ""})
	expectStatus(t, err, codes.InvalidArgument, protocol.ReasonInvalidArgument)

	be.SetDown(true)
	_, err = f.api.Get(ctx(t), &api.GetRequest{Namespace: "ns", Key: "k"})
	expectStatus(t, err, codes.Unavailable, protocol.ReasonBackendUnavailable)
	be.SetDown(false)

	f.redis.Kill()
	proplydtest.Eventually(t, 5*time.Second, func() error {
		_, err := f.api.Get(context.Background(), &api.GetRequest{Namespace: "ns", Key: "k"})
		if protocol.Reason(err) != protocol.ReasonCacheUnavailable {
			return err
		}
		return nil
	})
}

func TestHealthFollowsRedis(t *testing.T) {
	f := start(t, memory.New(memory.Options{}))
	health := healthpb.NewHealthClient(f.conn)
	check := func(want healthpb.HealthCheckResponse_ServingStatus) {
		t.Helper()
		proplydtest.Eventually(t, 5*time.Second, func() error {
			resp, err := health.Check(context.Background(), &healthpb.HealthCheckRequest{Service: api.CacheService_ServiceDesc.ServiceName})
			if err != nil {
				return err
			}
			if resp.GetStatus() != want {
				return proplydtest.Errorf("status %v", resp.GetStatus())
			}
			return nil
		})
	}
	check(healthpb.HealthCheckResponse_SERVING)
	f.redis.Kill()
	check(healthpb.HealthCheckResponse_NOT_SERVING)
	f.redis.Start()
	check(healthpb.HealthCheckResponse_SERVING)
}

// TestEndToEndWithEtcd runs the cache on a real etcd: writes made directly
// to etcd reach the cache and its subscribers through the watch.
func TestEndToEndWithEtcd(t *testing.T) {
	if testing.Short() {
		t.Skip("embedded etcd")
	}
	cli := etcdtest.Start(t)
	f := start(t, etcd.New(cli))
	register(t, f, "nodes", protocol.Spec{KeyPrefix: "/magnetar/nodes/", Write: protocol.WriteAround})

	if _, err := cli.Put(ctx(t), "/magnetar/nodes/n1", "v1"); err != nil {
		t.Fatal(err)
	}
	get := func() *api.GetResponse {
		resp, err := f.api.Get(ctx(t), &api.GetRequest{Namespace: "nodes", Key: "n1"})
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if r := get(); string(r.GetValue()) != "v1" || r.GetSource() != api.Source_SOURCE_BACKEND {
		t.Fatalf("first read: %v", r)
	}
	if r := get(); r.GetSource() != api.Source_SOURCE_CACHE {
		t.Fatalf("second read: %v", r)
	}

	sctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := f.api.Subscribe(sctx, &api.SubscribeRequest{Namespace: "nodes"})
	if err != nil {
		t.Fatal(err)
	}
	if first, err := stream.Recv(); err != nil || first.GetKind() != api.SubscribeResponse_KIND_RESET {
		t.Fatalf("first event: %v %v", first, err)
	}

	// A write that bypasses the cache.
	resp, err := cli.Put(ctx(t), "/magnetar/nodes/n1", "v2")
	if err != nil {
		t.Fatal(err)
	}
	for {
		ev, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if invs := ev.GetInvalidations(); len(invs) > 0 {
			if invs[0].GetKey() != "n1" || invs[0].GetRevision() != resp.Header.Revision {
				t.Fatalf("invalidation %v, want n1@%d", invs[0], resp.Header.Revision)
			}
			break
		}
	}
	if r := get(); string(r.GetValue()) != "v2" {
		t.Fatalf("after direct write: %v", r)
	}
}

func TestGracefulShutdownEndsSubscriptions(t *testing.T) {
	f := start(t, memory.New(memory.Options{}))
	register(t, f, "ns", protocol.Spec{KeyPrefix: "/ns/"})
	stream, err := f.api.Subscribe(context.Background(), &api.SubscribeRequest{Namespace: "ns"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	f.node.Shutdown()
	if d := time.Since(began); d > time.Second {
		t.Fatalf("graceful shutdown took %v with an open subscription", d)
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("subscription ended with %v", err)
			}
			break
		}
	}
}
