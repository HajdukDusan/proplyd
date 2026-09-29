package cache

import (
	"errors"
	"fmt"
)

var (
	ErrNamespaceNotFound = errors.New("cache: namespace not found")
	ErrNamespaceConflict = errors.New("cache: namespace already registered with a different configuration")
	ErrInvalidArgument   = errors.New("cache: invalid argument")
	ErrValueTooLarge     = errors.New("cache: value too large")
	ErrBackpressure      = errors.New("cache: write-behind queue full")
	ErrClosed            = errors.New("cache: closed")
)

// BackendError wraps a failure of the backing store.
type BackendError struct{ Err error }

func (e *BackendError) Error() string { return fmt.Sprintf("cache: backend: %v", e.Err) }
func (e *BackendError) Unwrap() error { return e.Err }

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}
