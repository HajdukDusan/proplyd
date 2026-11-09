package server

import (
	"context"
	"net/http"
	"path"
	"time"

	"github.com/c12s/proplyd/pkg/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// LatencyBuckets are the histogram buckets (seconds) of the latency metrics.
// Cache hits take well under a millisecond, so the low end is fine-grained.
var LatencyBuckets = []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

// metrics are the server's Prometheus metrics. Every server has its own
// registry, so several servers can run in one process (tests).
type metrics struct {
	reg           *prometheus.Registry
	calls         *prometheus.CounterVec
	duration      *prometheus.HistogramVec
	subscribers   prometheus.Gauge
	messages      *prometheus.CounterVec
	invalidations prometheus.Counter
	loads         prometheus.Histogram
}

func newMetrics(s *Server) *metrics {
	m := &metrics{
		reg: prometheus.NewRegistry(),
		calls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "proplyd_grpc_requests_total",
			Help: "gRPC calls handled, by method and status code.",
		}, []string{"method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "proplyd_grpc_request_duration_seconds",
			Help:    "Duration of unary gRPC calls, by method.",
			Buckets: LatencyBuckets,
		}, []string{"method"}),
		subscribers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "proplyd_subscribers",
			Help: "Open invalidation streams.",
		}),
		messages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "proplyd_subscribe_messages_total",
			Help: "Messages sent on invalidation streams, by kind (invalidations, reset, heartbeat).",
		}, []string{"kind"}),
		invalidations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "proplyd_invalidations_sent_total",
			Help: "Key invalidations sent to subscribers (one per key per subscriber).",
		}),
		loads: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "proplyd_backend_load_duration_seconds",
			Help:    "Time to load a missing key from the backing store and install it in Redis.",
			Buckets: LatencyBuckets,
		}),
	}
	m.reg.MustRegister(
		m.calls, m.duration, m.subscribers, m.messages, m.invalidations, m.loads,
		engineCollector{s},
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

func (m *metrics) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	method := path.Base(info.FullMethod)
	m.duration.WithLabelValues(method).Observe(time.Since(start).Seconds())
	m.calls.WithLabelValues(method, status.Code(err).String()).Inc()
	return resp, err
}

func (m *metrics) stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	m.subscribers.Inc()
	defer m.subscribers.Dec()
	err := handler(srv, ss)
	m.calls.WithLabelValues(path.Base(info.FullMethod), status.Code(err).String()).Inc()
	return err
}

// sent counts a message sent on an invalidation stream.
func (m *metrics) sent(r *api.SubscribeResponse) {
	kind := "invalidations"
	switch r.GetKind() {
	case api.SubscribeResponse_KIND_RESET:
		kind = "reset"
	case api.SubscribeResponse_KIND_HEARTBEAT:
		kind = "heartbeat"
	}
	m.messages.WithLabelValues(kind).Inc()
	m.invalidations.Add(float64(len(r.GetInvalidations())))
}

// MetricsHandler serves the server's metrics in the Prometheus text format.
func (s *Server) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(s.metrics.reg, promhttp.HandlerOpts{})
}

// engineCollector exposes the engine counters, read at scrape time.
type engineCollector struct{ s *Server }

var (
	descRedisUp = prometheus.NewDesc("proplyd_redis_up", "Whether the last Redis probe succeeded.", nil, nil)
	engineDescs = []struct {
		desc *prometheus.Desc
		get  func(*Server) int64
	}{
		{counterDesc("cache_hits", "Reads served from Redis."), func(s *Server) int64 { return s.engine.Stats().Hits.Load() }},
		{counterDesc("cache_misses", "Reads that found no entry in Redis."), func(s *Server) int64 { return s.engine.Stats().Misses.Load() }},
		{counterDesc("backend_loads", "Keys loaded from the backing store (read-through and refresh-ahead)."), func(s *Server) int64 { return s.engine.Stats().Loads.Load() }},
		{counterDesc("backend_load_errors", "Failed loads from the backing store."), func(s *Server) int64 { return s.engine.Stats().LoadErrors.Load() }},
		{counterDesc("fills", "Cache-aside fills received."), func(s *Server) int64 { return s.engine.Stats().Fills.Load() }},
		{counterDesc("fills_rejected", "Cache-aside fills ignored because the cache knew a newer change."), func(s *Server) int64 { return s.engine.Stats().FillsRejected.Load() }},
		{counterDesc("writes", "Writes and deletes."), func(s *Server) int64 { return s.engine.Stats().Writes.Load() }},
		{counterDesc("watch_events", "Changes received from the backing store's change stream."), func(s *Server) int64 { return s.engine.Stats().WatchEvents.Load() }},
		{counterDesc("purges", "Namespace purges (new epoch)."), func(s *Server) int64 { return s.engine.Stats().Purges.Load() }},
		{counterDesc("redis_restarts", "Redis restarts detected."), func(s *Server) int64 { return s.engine.Stats().RedisRestarts.Load() }},
		{counterDesc("namespaces_reloaded", "Namespaces reloaded after Redis lost them."), func(s *Server) int64 { return s.engine.Stats().NamespacesReloaded.Load() }},
		{counterDesc("store_resets", "Namespaces rebased after the backing store was wiped or restored."), func(s *Server) int64 { return s.engine.Stats().StoreResets.Load() }},
		{counterDesc("writebehind_writes", "Write-behind writes accepted."), func(s *Server) int64 { return s.engine.Stats().PendingWrites.Load() }},
		{counterDesc("writebehind_flushed", "Write-behind writes flushed to the backing store."), func(s *Server) int64 { return s.engine.Stats().Flushed.Load() }},
		{counterDesc("writebehind_conflicts", "Write-behind writes dropped because the store changed behind them."), func(s *Server) int64 { return s.engine.Stats().FlushConflicts.Load() }},
	}
)

func counterDesc(name, help string) *prometheus.Desc {
	return prometheus.NewDesc("proplyd_"+name+"_total", help, nil, nil)
}

func (c engineCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descRedisUp
	for _, d := range engineDescs {
		ch <- d.desc
	}
}

func (c engineCollector) Collect(ch chan<- prometheus.Metric) {
	up := 0.0
	if c.s.engine.RedisUp() {
		up = 1
	}
	ch <- prometheus.MustNewConstMetric(descRedisUp, prometheus.GaugeValue, up)
	for _, d := range engineDescs {
		ch <- prometheus.MustNewConstMetric(d.desc, prometheus.CounterValue, float64(d.get(c.s)))
	}
}
