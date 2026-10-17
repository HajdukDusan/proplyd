package engine

import (
	"errors"
	"fmt"
)

// Sentinel errors. The server maps them to gRPC status codes and reasons.
var (
	ErrNamespaceNotFound = errors.New("namespace not found")
	ErrNamespaceConflict = errors.New("namespace registered with a different spec")
	ErrPrefixNotAllowed  = errors.New("key prefix not allowed")
	ErrInvalidArgument   = errors.New("invalid argument")
	// ErrCacheUnavailable: Redis is unreachable, or the namespace has not
	// caught up with the backend yet.
	ErrCacheUnavailable = errors.New("cache unavailable")
	ErrClosed           = errors.New("engine closed")
)

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}

// redisErr marks a Redis failure as a cache failure.
func redisErr(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: redis %s: %w", ErrCacheUnavailable, op, err)
}
