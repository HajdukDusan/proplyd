// Package server exposes a cache.Engine over gRPC.
package server

import (
	"context"
	"errors"
	"log/slog"

	pb "github.com/c12s/proplyd/api/proplydpb"
	"github.com/c12s/proplyd/backend"
	"github.com/c12s/proplyd/cache"
	"github.com/c12s/proplyd/protocol"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// Service implements pb.CacheServiceServer.
type Service struct {
	pb.UnimplementedCacheServiceServer
	engine *cache.Engine
	log    *slog.Logger
}

// New returns a gRPC service backed by engine.
func New(engine *cache.Engine, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{engine: engine, log: log}
}

// Register registers the cache service and a standard gRPC health service on
// s. The returned health server should be set to NOT_SERVING before shutdown.
func (s *Service) Register(gs *grpc.Server) *health.Server {
	pb.RegisterCacheServiceServer(gs, s)
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus(pb.CacheService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(gs, hs)
	return hs
}

func (s *Service) RegisterNamespace(_ context.Context, req *pb.RegisterNamespaceRequest) (*pb.RegisterNamespaceResponse, error) {
	cfg, err := protocol.FromProto(req.GetConfig())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	eff, created, err := s.engine.Register(req.GetNamespace(), cfg)
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.RegisterNamespaceResponse{Config: eff.ToProto(), Epoch: s.engine.Epoch(), Created: created}, nil
}

func (s *Service) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	res, err := s.engine.Get(ctx, req.GetNamespace(), req.GetKey())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.GetResponse{
		Outcome:     outcomeToProto(res.Outcome),
		Found:       res.Found,
		Value:       res.Value,
		ModRevision: res.ModRevision,
	}, nil
}

func (s *Service) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	rev, err := s.engine.Set(ctx, req.GetNamespace(), req.GetKey(), req.GetValue(), req.GetTtl().AsDuration())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.SetResponse{Revision: rev}, nil
}

func (s *Service) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	rev, err := s.engine.Delete(ctx, req.GetNamespace(), req.GetKey())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.DeleteResponse{Revision: rev}, nil
}

func (s *Service) Fill(_ context.Context, req *pb.FillRequest) (*pb.FillResponse, error) {
	rec := backend.Record{
		Found:       req.GetFound(),
		Value:       req.GetValue(),
		ModRevision: req.GetModRevision(),
		Revision:    req.GetReadRevision(),
	}
	ok, err := s.engine.Fill(req.GetNamespace(), req.GetKey(), rec, req.GetTtl().AsDuration())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.FillResponse{Accepted: ok}, nil
}

func (s *Service) Invalidate(_ context.Context, req *pb.InvalidateRequest) (*pb.InvalidateResponse, error) {
	if err := s.engine.Invalidate(req.GetNamespace(), req.GetKeys()...); err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.InvalidateResponse{}, nil
}

func (s *Service) Purge(_ context.Context, req *pb.PurgeRequest) (*pb.PurgeResponse, error) {
	if err := s.engine.Purge(req.GetNamespace()); err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.PurgeResponse{}, nil
}

func (s *Service) Stats(_ context.Context, req *pb.StatsRequest) (*pb.StatsResponse, error) {
	st, err := s.engine.Stats(req.GetNamespace())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &pb.StatsResponse{
		Entries:       st.Entries,
		Hits:          st.Hits,
		Misses:        st.Misses,
		Loads:         st.Loads,
		LoadErrors:    st.LoadErrors,
		FillsAccepted: st.FillsAccepted,
		FillsRejected: st.FillsRejected,
		PendingWrites: st.PendingWrites,
		FlushedWrites: st.FlushedWrites,
		FlushErrors:   st.FlushErrors,
		WatchEvents:   st.WatchEvents,
		WatchResyncs:  st.WatchResyncs,
		Refreshes:     st.Refreshes,
	}, nil
}

func outcomeToProto(o cache.Outcome) pb.Outcome {
	switch o {
	case cache.OutcomeHit:
		return pb.Outcome_OUTCOME_HIT
	case cache.OutcomeLoaded:
		return pb.Outcome_OUTCOME_LOADED
	case cache.OutcomeMiss:
		return pb.Outcome_OUTCOME_MISS
	default:
		return pb.Outcome_OUTCOME_UNSPECIFIED
	}
}

// toStatus maps engine errors to gRPC statuses carrying an ErrorInfo reason,
// so clients can tell cache failures from backing-store failures.
func (s *Service) toStatus(err error) error {
	var be *cache.BackendError
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, cache.ErrNamespaceNotFound):
		return withReason(codes.NotFound, err, protocol.ReasonNamespaceNotFound)
	case errors.Is(err, cache.ErrNamespaceConflict):
		return withReason(codes.FailedPrecondition, err, protocol.ReasonNamespaceConflict)
	case errors.Is(err, cache.ErrValueTooLarge):
		return withReason(codes.InvalidArgument, err, protocol.ReasonValueTooLarge)
	case errors.Is(err, cache.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, cache.ErrBackpressure):
		return withReason(codes.ResourceExhausted, err, protocol.ReasonBackpressure)
	case errors.Is(err, cache.ErrClosed):
		return withReason(codes.Unavailable, err, protocol.ReasonShuttingDown)
	case errors.As(err, &be):
		return withReason(codes.Unavailable, err, protocol.ReasonBackendUnavailable)
	default:
		s.log.Error("unexpected error", "err", err)
		return status.Error(codes.Internal, err.Error())
	}
}

func withReason(code codes.Code, err error, reason string) error {
	st := status.New(code, err.Error())
	if det, derr := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: protocol.ErrorDomain}); derr == nil {
		return det.Err()
	}
	return st.Err()
}
