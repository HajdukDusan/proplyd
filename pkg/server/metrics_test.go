package server_test

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/c12s/proplyd/pkg/api"
	"github.com/c12s/proplyd/pkg/backend/memory"
	"github.com/c12s/proplyd/pkg/protocol"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"google.golang.org/grpc/codes"
)

// scrape reads the node's metrics and returns them by name.
func scrape(t *testing.T, f *fixture) map[string]*dto.MetricFamily {
	t.Helper()
	rec := httptest.NewRecorder()
	f.node.Server().MetricsHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	var p expfmt.TextParser
	mfs, err := p.TextToMetricFamilies(strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("parse metrics: %v\n%s", err, body)
	}
	return mfs
}

// value returns a counter or gauge value, or a histogram's sample count, of
// the series with the given labels (name=value pairs).
func value(t *testing.T, mfs map[string]*dto.MetricFamily, name string, labels ...string) float64 {
	t.Helper()
	mf, ok := mfs[name]
	if !ok {
		t.Fatalf("metric %s missing", name)
	}
next:
	for _, m := range mf.GetMetric() {
		have := map[string]string{}
		for _, l := range m.GetLabel() {
			have[l.GetName()] = l.GetValue()
		}
		for i := 0; i+1 < len(labels); i += 2 {
			if have[labels[i]] != labels[i+1] {
				continue next
			}
		}
		switch {
		case m.GetCounter() != nil:
			return m.GetCounter().GetValue()
		case m.GetGauge() != nil:
			return m.GetGauge().GetValue()
		case m.GetHistogram() != nil:
			return float64(m.GetHistogram().GetSampleCount())
		}
	}
	return 0
}

func TestMetrics(t *testing.T) {
	be := memory.New(memory.Options{})
	f := start(t, be)
	register(t, f, "nodes", protocol.Spec{KeyPrefix: "nodes/"})
	if _, err := be.Put(ctx(t), "nodes/n1", []byte("v1")); err != nil {
		t.Fatal(err)
	}

	sctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := f.api.Subscribe(sctx, &api.SubscribeRequest{Namespace: "nodes"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil { // the initial reset
		t.Fatal(err)
	}
	for range 3 {
		if _, err := f.api.Get(ctx(t), &api.GetRequest{Namespace: "nodes", Key: "n1"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.api.Get(ctx(t), &api.GetRequest{Namespace: "missing", Key: "n1"}); err == nil {
		t.Fatal("read from an unknown namespace succeeded")
	}
	if _, err := f.api.Set(ctx(t), &api.SetRequest{Namespace: "nodes", Key: "n1", Value: []byte("v2")}); err != nil {
		t.Fatal(err)
	}
	ev, err := stream.Recv()
	if err != nil || len(ev.GetInvalidations()) == 0 {
		t.Fatalf("invalidation: %v %v", ev, err)
	}

	mfs := scrape(t, f)
	checks := []struct {
		name   string
		labels []string
		want   float64
	}{
		{"proplyd_grpc_requests_total", []string{"method", "Get", "code", "OK"}, 3},
		{"proplyd_grpc_requests_total", []string{"method", "Get", "code", codes.NotFound.String()}, 1},
		{"proplyd_grpc_requests_total", []string{"method", "Set", "code", "OK"}, 1},
		{"proplyd_grpc_request_duration_seconds", []string{"method", "Get"}, 4},
		{"proplyd_backend_loads_total", nil, 1},
		{"proplyd_backend_load_duration_seconds", nil, 1},
		{"proplyd_cache_hits_total", nil, 2},
		{"proplyd_writes_total", nil, 1},
		{"proplyd_subscribers", nil, 1},
		{"proplyd_redis_up", nil, 1},
	}
	for _, c := range checks {
		if got := value(t, mfs, c.name, c.labels...); got != c.want {
			t.Errorf("%s%v = %v, want %v", c.name, c.labels, got, c.want)
		}
	}
	// The watch may also announce the first, direct write.
	if got := value(t, mfs, "proplyd_invalidations_sent_total"); got < float64(len(ev.GetInvalidations())) {
		t.Errorf("invalidations sent = %v, want at least %d", got, len(ev.GetInvalidations()))
	}
	if value(t, mfs, "proplyd_subscribe_messages_total", "kind", "reset") != 1 {
		t.Error("the initial reset was not counted")
	}

	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for value(t, scrape(t, f), "proplyd_subscribers") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("closed stream still counted as a subscriber")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
