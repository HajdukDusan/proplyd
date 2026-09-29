package protocol

// ErrorDomain is the google.rpc.ErrorInfo domain used by the cache server.
const ErrorDomain = "proplyd.c12s.io"

// ErrorInfo reasons attached to gRPC errors returned by the cache server.
// Clients use them to tell cache failures apart from backing-store failures
// and from caller mistakes.
const (
	// The namespace does not exist (e.g. the cache restarted). The client
	// re-registers and retries. Code: NOT_FOUND.
	ReasonNamespaceNotFound = "NAMESPACE_NOT_FOUND"
	// The namespace exists with a different configuration.
	// Code: FAILED_PRECONDITION.
	ReasonNamespaceConflict = "NAMESPACE_CONFLICT"
	// The cache is healthy but could not reach the backing store.
	// Code: UNAVAILABLE. Must not trip the client's circuit breaker.
	ReasonBackendUnavailable = "BACKEND_UNAVAILABLE"
	// The write-behind queue is full. Code: RESOURCE_EXHAUSTED.
	ReasonBackpressure = "WRITE_BEHIND_BACKPRESSURE"
	// The value exceeds max_value_bytes. Code: INVALID_ARGUMENT.
	ReasonValueTooLarge = "VALUE_TOO_LARGE"
	// The cache is shutting down. Code: UNAVAILABLE.
	ReasonShuttingDown = "SHUTTING_DOWN"
)
