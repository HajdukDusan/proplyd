// Package server exposes the proplyd cache engine over gRPC.
//
// It is public so that the server can be embedded, for example by the
// client library's integration tests.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/c12s/proplyd/internal/engine"
	"github.com/c12s/proplyd/pkg/api"
	"github.com/c12s/proplyd/pkg/backend"
	"github.com/c12s/proplyd/pkg/protocol"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// Options configures a Server. Zero values take the engine defaults.
type Options struct {
	Redis   redis.UniversalClient
	Backend backend.Backend

	InstanceID          string
	RedisKeyPrefix      string
	AllowedPrefixes     []string
	MaxValueSize        int
	LoadTimeout         time.Duration
	UpdateTimeout       time.Duration
	FenceTTL            time.Duration
	ReadyTimeout        time.Duration
	LeaseTTL            time.Duration
	InvalidationLogSize int64
	HeartbeatInterval   time.Duration
	RedisCheckInterval  time.Duration

	// TLS, if set, secures the listener.
	TLS    credentials.TransportCredentials
	Logger *slog.Logger
}

// Server is a proplyd gRPC server.
type Server struct {
	api.UnimplementedCacheServiceServer

	engine *engine.Engine
	grpc   *grpc.Server
	health *health.Server
	log    *slog.Logger

	stopHealth context.CancelFunc
	healthDone chan struct{}
	stopOnce   sync.Once
}

// New creates a server. Call Serve to start it.
func New(opts Options) (*Server, error) {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	eng, err := engine.New(engine.Options{
		Redis:               opts.Redis,
		Backend:             opts.Backend,
		InstanceID:          opts.InstanceID,
		RedisKeyPrefix:      opts.RedisKeyPrefix,
		AllowedPrefixes:     opts.AllowedPrefixes,
		MaxValueSize:        opts.MaxValueSize,
		LoadTimeout:         opts.LoadTimeout,
		UpdateTimeout:       opts.UpdateTimeout,
		FenceTTL:            opts.FenceTTL,
		ReadyTimeout:        opts.ReadyTimeout,
		LeaseTTL:            opts.LeaseTTL,
		InvalidationLogSize: opts.InvalidationLogSize,
		HeartbeatInterval:   opts.HeartbeatInterval,
		RedisCheckInterval:  opts.RedisCheckInterval,
		Logger:              log,
	})
	if err != nil {
		return nil, err
	}
	maxMsg := opts.MaxValueSize
	if maxMsg <= 0 {
		maxMsg = 1 << 20
	}
	gopts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMsg + 64<<10),
		grpc.MaxSendMsgSize(maxMsg + 64<<10),
		// Clients on bad networks ping to detect dead connections; allow it.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
	}
	if opts.TLS != nil {
		gopts = append(gopts, grpc.Creds(opts.TLS))
	}
	s := &Server{
		engine:     eng,
		grpc:       grpc.NewServer(gopts...),
		health:     health.NewServer(),
		log:        log,
		healthDone: make(chan struct{}),
	}
	api.RegisterCacheServiceServer(s.grpc, s)
	healthpb.RegisterHealthServer(s.grpc, s.health)

	ctx, cancel := context.WithCancel(context.Background())
	s.stopHealth = cancel
	go s.reportHealth(ctx)
	return s, nil
}

// Serve accepts connections on lis until Shutdown or Stop.
func (s *Server) Serve(lis net.Listener) error {
	err := s.grpc.Serve(lis)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// Shutdown stops gracefully: no new RPCs, in-flight ones finish (until ctx
// ends), watchers hand back their leases.
func (s *Server) Shutdown(ctx context.Context) {
	s.stopOnce.Do(func() {
		s.health.Shutdown()
		// Subscriptions never end by themselves; end them so that
		// GracefulStop only waits for unary calls.
		s.engine.DrainSubscriptions()
		done := make(chan struct{})
		go func() {
			s.grpc.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			s.grpc.Stop()
		}
		s.closeHealth()
		s.engine.Close()
	})
}

// Stop stops at once, like a crash: connections are cut and leases are left
// to expire.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		s.grpc.Stop()
		s.closeHealth()
		s.engine.Abort()
	})
}

func (s *Server) closeHealth() {
	s.stopHealth()
	<-s.healthDone
}

// Stats returns the engine counters.
func (s *Server) Stats() *engine.Stats { return s.engine.Stats() }

// reportHealth serves NOT_SERVING while Redis is unreachable.
func (s *Server) reportHealth(ctx context.Context) {
	defer close(s.healthDone)
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		st := healthpb.HealthCheckResponse_SERVING
		if !s.engine.RedisUp() {
			st = healthpb.HealthCheckResponse_NOT_SERVING
		}
		s.health.SetServingStatus("", st)
		s.health.SetServingStatus(api.CacheService_ServiceDesc.ServiceName, st)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Server) Register(ctx context.Context, req *api.RegisterRequest) (*api.RegisterResponse, error) {
	spec, err := s.engine.Register(ctx, req.GetNamespace(), protocol.SpecFromProto(req.GetSpec()))
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &api.RegisterResponse{Spec: spec.ToProto()}, nil
}

func (s *Server) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	r, err := s.engine.Get(ctx, req.GetNamespace(), req.GetKey(), req.GetMinRevision())
	if err != nil {
		return nil, s.toStatus(err)
	}
	src := api.Source_SOURCE_CACHE
	if r.FromBackend {
		src = api.Source_SOURCE_BACKEND
	}
	return &api.GetResponse{Miss: r.Miss, Found: r.Found, Value: r.Value, Revision: r.Revision, Source: src}, nil
}

func (s *Server) Fill(ctx context.Context, req *api.FillRequest) (*api.FillResponse, error) {
	stored, err := s.engine.Fill(ctx, req.GetNamespace(), req.GetKey(), backend.Item{
		Found:        req.GetFound(),
		Value:        req.GetValue(),
		ModRevision:  req.GetModRevision(),
		ReadRevision: req.GetReadRevision(),
	})
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &api.FillResponse{Stored: stored}, nil
}

func (s *Server) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	rev, err := s.engine.Set(ctx, req.GetNamespace(), req.GetKey(), req.GetValue())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &api.SetResponse{Revision: rev}, nil
}

func (s *Server) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	rev, existed, err := s.engine.Delete(ctx, req.GetNamespace(), req.GetKey())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &api.DeleteResponse{Revision: rev, Existed: existed}, nil
}

func (s *Server) Invalidate(ctx context.Context, req *api.InvalidateRequest) (*api.InvalidateResponse, error) {
	if err := s.engine.Invalidate(ctx, req.GetNamespace(), req.GetKeys(), req.GetAll()); err != nil {
		return nil, s.toStatus(err)
	}
	return &api.InvalidateResponse{}, nil
}

func (s *Server) Subscribe(req *api.SubscribeRequest, stream grpc.ServerStreamingServer[api.SubscribeResponse]) error {
	err := s.engine.Subscribe(stream.Context(), req.GetNamespace(), req.GetEpoch(), req.GetPosition(), stream.Send)
	if err != nil {
		return s.toStatus(err)
	}
	return nil
}

// toStatus maps engine and backend errors to gRPC status errors with an
// ErrorInfo reason.
func (s *Server) toStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	msg := err.Error()
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, msg)
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, msg)
	case errors.Is(err, engine.ErrNamespaceNotFound):
		return protocol.Status(codes.NotFound, protocol.ReasonNamespaceNotFound, msg)
	case errors.Is(err, engine.ErrNamespaceConflict):
		return protocol.Status(codes.FailedPrecondition, protocol.ReasonNamespaceConflict, msg)
	case errors.Is(err, engine.ErrPrefixNotAllowed):
		return protocol.Status(codes.PermissionDenied, protocol.ReasonPrefixNotAllowed, msg)
	case errors.Is(err, engine.ErrInvalidArgument):
		return protocol.Status(codes.InvalidArgument, protocol.ReasonInvalidArgument, msg)
	case errors.Is(err, engine.ErrCacheUnavailable), errors.Is(err, engine.ErrClosed):
		return protocol.Status(codes.Unavailable, protocol.ReasonCacheUnavailable, msg)
	case errors.Is(err, backend.ErrUnavailable):
		return protocol.Status(codes.Unavailable, protocol.ReasonBackendUnavailable, msg)
	}
	s.log.Error("internal error", "err", err)
	return status.Error(codes.Internal, msg)
}
