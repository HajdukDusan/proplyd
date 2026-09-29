// Package store defines the storage layer of the cache: a bounded, byte-slice
// key/value map with per-entry TTL and an eviction policy.
//
// The cache engine only depends on the Store interface, so the in-process
// implementation (Hot, backed by samber/hot) can later be replaced by a
// distributed one (e.g. Olric, Redis, groupcache-style peers) without
// touching the strategy logic.
//
// Values are opaque []byte: the engine encodes its entry metadata into the
// same slice so the store holds a single pointer-free allocation per entry,
// which keeps GC scanning cheap even with millions of entries.
package store

import "time"

// Store is a bounded key/value map. Implementations must be safe for
// concurrent use. Stored slices must be treated as immutable by both sides.
type Store interface {
	// Get returns the value and records the access for the eviction policy.
	Get(key string) ([]byte, bool)
	// Peek returns the value without affecting the eviction policy.
	Peek(key string) ([]byte, bool)
	// Set stores a value. ttl <= 0 means the entry never expires.
	Set(key string, value []byte, ttl time.Duration)
	// Delete removes a key and reports whether it was present.
	Delete(key string) bool
	// Purge removes every key.
	Purge()
	// Len returns the number of stored keys.
	Len() int
	// Close releases background resources.
	Close() error
}

// Policy is an eviction algorithm name.
type Policy string

const (
	LRU      Policy = "lru"
	LFU      Policy = "lfu"
	TinyLFU  Policy = "tinylfu"
	WTinyLFU Policy = "wtinylfu"
	TwoQueue Policy = "2q"
	ARC      Policy = "arc"
	FIFO     Policy = "fifo"
	SIEVE    Policy = "sieve"
)

// Config configures a store.
type Config struct {
	Policy   Policy
	Capacity int
	// JanitorInterval, when > 0, periodically removes expired entries.
	// Expired entries are otherwise removed lazily on access or by eviction.
	JanitorInterval time.Duration
}

// Factory builds a store for a namespace.
type Factory func(namespace string, cfg Config) (Store, error)
