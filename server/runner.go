package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/c12s/proplyd/cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

// DefaultServerOptions are the gRPC options used by Start: keepalive so that
// dead peers are detected quickly on flaky edge networks.
func DefaultServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.MaxRecvMsgSize(8 << 20),
	}
}

// Server is a running gRPC cache server.
type Server struct {
	gs     *grpc.Server
	hs     *health.Server
	engine *cache.Engine
	log    *slog.Logger
	done   chan error
}

// Start serves engine on lis in a background goroutine.
func Start(lis net.Listener, engine *cache.Engine, log *slog.Logger, opts ...grpc.ServerOption) *Server {
	if log == nil {
		log = slog.Default()
	}
	if opts == nil {
		opts = DefaultServerOptions()
	}
	gs := grpc.NewServer(opts...)
	hs := New(engine, log).Register(gs)
	s := &Server{gs: gs, hs: hs, engine: engine, log: log, done: make(chan error, 1)}
	go func() {
		err := gs.Serve(lis)
		if errors.Is(err, grpc.ErrServerStopped) {
			err = nil
		}
		s.done <- err
	}()
	return s
}

// Shutdown stops gracefully: it reports NOT_SERVING, lets in-flight requests
// finish, flushes write-behind data and closes the engine. If ctx expires the
// remaining requests are aborted and unflushed data is lost.
func (s *Server) Shutdown(ctx context.Context) error {
	s.hs.Shutdown()
	stopped := make(chan struct{})
	go func() {
		s.gs.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-ctx.Done():
		s.gs.Stop()
		<-stopped
	}
	return errors.Join(s.engine.Close(ctx), <-s.done)
}

// Kill stops the server abruptly, like a crash: connections are dropped and
// pending write-behind data is discarded.
func (s *Server) Kill() {
	s.hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	s.gs.Stop()
	<-s.done
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // expired context: close without flushing
	_ = s.engine.Close(ctx)
}
