package protocol

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorDomain is the google.rpc.ErrorInfo domain of proplyd errors.
const ErrorDomain = "proplyd.c12s.io"

// Error reasons carried in google.rpc.ErrorInfo.
const (
	// The namespace is not registered (or Redis lost it). Register again.
	ReasonNamespaceNotFound = "NAMESPACE_NOT_FOUND"
	// The namespace exists with a different spec.
	ReasonNamespaceConflict = "NAMESPACE_CONFLICT"
	// The key prefix is not in the server's allow list.
	ReasonPrefixNotAllowed = "PREFIX_NOT_ALLOWED"
	// The request is malformed (bad key, oversized value, ...).
	ReasonInvalidArgument = "INVALID_ARGUMENT"
	// The cache cannot serve the request: Redis is unreachable or the
	// namespace is still catching up with the backing store.
	ReasonCacheUnavailable = "CACHE_UNAVAILABLE"
	// The cache cannot reach the backing store.
	ReasonBackendUnavailable = "BACKEND_UNAVAILABLE"
)

// Status builds a gRPC status error with an ErrorInfo detail.
func Status(code codes.Code, reason, msg string) error {
	st := status.New(code, msg)
	if withDetails, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: ErrorDomain}); err == nil {
		st = withDetails
	}
	return st.Err()
}

// Reason returns the proplyd ErrorInfo reason of a gRPC error, or "".
func Reason(err error) string {
	var se interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &se) {
		return ""
	}
	for _, d := range se.GRPCStatus().Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetDomain() == ErrorDomain {
			return info.GetReason()
		}
	}
	return ""
}
