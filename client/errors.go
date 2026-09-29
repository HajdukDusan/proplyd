package client

import (
	"context"
	"errors"

	"github.com/c12s/proplyd/protocol"
	"github.com/sony/gobreaker/v2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrCacheUnavailable is returned by writes whose strategy forbids
	// bypassing the cache (write-through, write-behind) while the cache is
	// unreachable. The write was not applied, unless the cache timed out after
	// receiving it; write-through writes are idempotent, so retrying is safe.
	ErrCacheUnavailable = errors.New("proplyd: cache unavailable")
	// ErrStoreUnavailable is returned when the backing store failed, either
	// behind the cache or when accessed directly.
	ErrStoreUnavailable = errors.New("proplyd: backing store unavailable")
	// ErrBackpressure is returned when the write-behind queue of the cache is
	// full. Retry later.
	ErrBackpressure = errors.New("proplyd: write-behind queue full")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("proplyd: client closed")
)

// failureKind classifies the outcome of a cache call.
type failureKind int

const (
	kindOK failureKind = iota
	// The cache itself is unreachable or broken: counts against the breaker
	// and triggers the fallback path.
	kindCacheDown
	// The cache is fine but its store is not: fallback path, breaker neutral.
	kindStoreDown
	// Caller cancelled, invalid request, backpressure...: returned as is.
	kindCaller
)

func classify(ctx context.Context, err error) failureKind {
	if err == nil {
		return kindOK
	}
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return kindCacheDown
	}
	if ctx.Err() != nil {
		return kindCaller
	}
	st, ok := status.FromError(err)
	if !ok {
		return kindCacheDown
	}
	reason := errorReason(st)
	switch st.Code() {
	case codes.Unavailable:
		if reason == protocol.ReasonBackendUnavailable {
			return kindStoreDown
		}
		return kindCacheDown
	case codes.ResourceExhausted:
		if reason == protocol.ReasonBackpressure {
			return kindCaller
		}
		return kindCacheDown
	case codes.DeadlineExceeded, codes.Internal, codes.Unknown, codes.Aborted, codes.Canceled, codes.Unimplemented:
		return kindCacheDown
	default:
		return kindCaller
	}
}

func errorReason(st *status.Status) string {
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetDomain() == protocol.ErrorDomain {
			return info.GetReason()
		}
	}
	return ""
}

func hasReason(err error, reason string) bool {
	st, ok := status.FromError(err)
	return ok && errorReason(st) == reason
}

// neutral marks errors that must not affect the circuit breaker.
type neutral struct{ err error }

func (n *neutral) Error() string { return n.err.Error() }
func (n *neutral) Unwrap() error { return n.err }

func unwrapNeutral(err error) error {
	var n *neutral
	if errors.As(err, &n) {
		return n.err
	}
	return err
}

// callerError converts a gRPC error the caller should see into a friendlier
// error: the caller's own context error, or a matching sentinel.
func callerError(ctx context.Context, err error) error {
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case hasReason(err, protocol.ReasonBackpressure):
		return errors.Join(ErrBackpressure, err)
	default:
		return err
	}
}
