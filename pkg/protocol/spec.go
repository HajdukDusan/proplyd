// Package protocol holds the types shared by the proplyd server and its
// clients: namespace specs, their validation and proto conversion, and the
// error reasons carried in gRPC status details.
//
// It must stay free of server dependencies (Redis, the engine) so that
// client binaries on constrained devices stay small.
package protocol

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/c12s/proplyd/pkg/api"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ReadStrategy decides who loads a key that is not cached.
type ReadStrategy int

const (
	// ReadThrough: the cache loads misses from the backing store. Concurrent
	// misses for a key share one load.
	ReadThrough ReadStrategy = iota + 1
	// CacheAside: the cache reports a miss; the client loads the key from
	// the backing store and fills the cache.
	CacheAside
	// RefreshAhead: read-through, and entries older than RefreshAfter are
	// reloaded in the background while the cached value is still served.
	RefreshAhead
)

func (s ReadStrategy) String() string {
	switch s {
	case ReadThrough:
		return "read-through"
	case CacheAside:
		return "cache-aside"
	case RefreshAhead:
		return "refresh-ahead"
	}
	return fmt.Sprintf("ReadStrategy(%d)", int(s))
}

// WriteStrategy decides what a write does to the cached entry.
type WriteStrategy int

const (
	// WriteThrough: the cache writes the backing store, then caches the new
	// value. When the cache is unreachable, writes are rejected.
	WriteThrough WriteStrategy = iota + 1
	// WriteAround: the cache writes the backing store and invalidates the
	// entry. When the cache is unreachable, clients write the backing store
	// directly and the cache catches up through its watch.
	WriteAround
	// WriteBehind: the cache acknowledges once the write is pending in Redis
	// and flushes it to the backing store later, coalescing writes per key.
	// A flush is a compare-and-set on the revision the write was based on:
	// if the store changed meanwhile, the store wins and the write is
	// dropped. When the cache is unreachable, writes are rejected.
	WriteBehind
)

func (s WriteStrategy) String() string {
	switch s {
	case WriteThrough:
		return "write-through"
	case WriteAround:
		return "write-around"
	case WriteBehind:
		return "write-behind"
	}
	return fmt.Sprintf("WriteStrategy(%d)", int(s))
}

// SyncMode decides whether the cache follows the backing store's changes.
type SyncMode int

const (
	// SyncWatch follows the backing store's change stream for the key
	// prefix, so writes that bypass the cache invalidate cached entries.
	SyncWatch SyncMode = iota + 1
	// SyncNone relies on writes going through the cache. Writes that bypass
	// it stay invisible until the entry expires.
	SyncNone
)

func (m SyncMode) String() string {
	switch m {
	case SyncWatch:
		return "watch"
	case SyncNone:
		return "none"
	}
	return fmt.Sprintf("SyncMode(%d)", int(m))
}

// Defaults applied by Normalize.
const (
	DefaultTTL         = 10 * time.Minute
	DefaultNegativeTTL = 30 * time.Second
	// RefreshAfter defaults to this fraction of the TTL.
	DefaultRefreshRatio = 0.75

	MinTTL = 100 * time.Millisecond
	MaxTTL = 30 * 24 * time.Hour

	MaxKeyLen       = 1024
	MaxKeyPrefixLen = 512
)

// Spec configures a namespace. Every client of a namespace must register
// the same spec.
type Spec struct {
	Read  ReadStrategy
	Write WriteStrategy
	Sync  SyncMode
	// KeyPrefix is the backing store prefix of the namespace. Client keys
	// are relative to it.
	KeyPrefix string
	// TTL of cached values. Zero means DefaultTTL.
	TTL time.Duration
	// NegativeTTL is how long "key does not exist" is cached. Zero means
	// min(TTL, DefaultNegativeTTL); a negative value disables negative
	// caching.
	NegativeTTL time.Duration
	// RefreshAfter applies to RefreshAhead only. Zero means
	// DefaultRefreshRatio of the TTL.
	RefreshAfter time.Duration
}

// NegativeCaching reports whether not-found results are cached.
func (s Spec) NegativeCaching() bool { return s.NegativeTTL > 0 }

// Normalize applies defaults and validates the spec.
func (s Spec) Normalize() (Spec, error) {
	if s.Read == 0 {
		s.Read = ReadThrough
	}
	if s.Write == 0 {
		s.Write = WriteThrough
	}
	if s.Sync == 0 {
		s.Sync = SyncWatch
	}
	if s.TTL == 0 {
		s.TTL = DefaultTTL
	}
	if s.NegativeTTL == 0 {
		s.NegativeTTL = min(s.TTL, DefaultNegativeTTL)
	}
	if s.NegativeTTL < 0 {
		s.NegativeTTL = -1
	}
	if s.Read == RefreshAhead {
		if s.RefreshAfter == 0 {
			s.RefreshAfter = time.Duration(float64(s.TTL) * DefaultRefreshRatio)
		}
	} else {
		s.RefreshAfter = 0
	}
	return s, s.validate()
}

func (s Spec) validate() error {
	switch s.Read {
	case ReadThrough, CacheAside, RefreshAhead:
	default:
		return fmt.Errorf("unknown read strategy %d", int(s.Read))
	}
	switch s.Write {
	case WriteThrough, WriteAround, WriteBehind:
	default:
		return fmt.Errorf("unknown write strategy %d", int(s.Write))
	}
	switch s.Sync {
	case SyncWatch, SyncNone:
	default:
		return fmt.Errorf("unknown sync mode %d", int(s.Sync))
	}
	if s.KeyPrefix == "" {
		return errors.New("key prefix is required")
	}
	if len(s.KeyPrefix) > MaxKeyPrefixLen || !utf8.ValidString(s.KeyPrefix) {
		return fmt.Errorf("key prefix must be valid UTF-8 of at most %d bytes", MaxKeyPrefixLen)
	}
	if s.TTL < MinTTL || s.TTL > MaxTTL {
		return fmt.Errorf("ttl %v outside [%v, %v]", s.TTL, MinTTL, MaxTTL)
	}
	if s.NegativeTTL > s.TTL {
		return fmt.Errorf("negative ttl %v exceeds ttl %v", s.NegativeTTL, s.TTL)
	}
	if s.Read == RefreshAhead && (s.RefreshAfter <= 0 || s.RefreshAfter >= s.TTL) {
		return fmt.Errorf("refresh-after %v must be in (0, ttl)", s.RefreshAfter)
	}
	return nil
}

// Equal reports whether two normalized specs are the same.
func (s Spec) Equal(o Spec) bool { return s == o }

func (s Spec) String() string {
	return fmt.Sprintf("{read=%v write=%v sync=%v prefix=%q ttl=%v negative_ttl=%v refresh_after=%v}",
		s.Read, s.Write, s.Sync, s.KeyPrefix, s.TTL, s.NegativeTTL, s.RefreshAfter)
}

// ToProto converts the spec to its wire form.
func (s Spec) ToProto() *api.NamespaceSpec {
	p := &api.NamespaceSpec{
		Read:      api.ReadStrategy(s.Read),
		Write:     api.WriteStrategy(s.Write),
		Sync:      api.SyncMode(s.Sync),
		KeyPrefix: s.KeyPrefix,
	}
	if s.TTL != 0 {
		p.Ttl = durationpb.New(s.TTL)
	}
	if s.NegativeTTL != 0 {
		p.NegativeTtl = durationpb.New(s.NegativeTTL)
	}
	if s.RefreshAfter != 0 {
		p.RefreshAfter = durationpb.New(s.RefreshAfter)
	}
	return p
}

// SpecFromProto converts the wire form to a spec. It does not normalize.
func SpecFromProto(p *api.NamespaceSpec) Spec {
	if p == nil {
		return Spec{}
	}
	return Spec{
		Read:         ReadStrategy(p.GetRead()),
		Write:        WriteStrategy(p.GetWrite()),
		Sync:         SyncMode(p.GetSync()),
		KeyPrefix:    p.GetKeyPrefix(),
		TTL:          durationOrZero(p.GetTtl()),
		NegativeTTL:  durationOrZero(p.GetNegativeTtl()),
		RefreshAfter: durationOrZero(p.GetRefreshAfter()),
	}
}

func durationOrZero(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

var namespaceRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

// ValidateNamespace checks a namespace name: 1-63 characters of lowercase
// letters, digits, '.', '_' and '-', starting with a letter or digit.
func ValidateNamespace(name string) error {
	if !namespaceRE.MatchString(name) {
		return fmt.Errorf("invalid namespace %q: want %s", name, namespaceRE)
	}
	return nil
}

// ValidateKey checks a key relative to a namespace prefix.
func ValidateKey(key string) error {
	switch {
	case key == "":
		return errors.New("empty key")
	case len(key) > MaxKeyLen:
		return fmt.Errorf("key longer than %d bytes", MaxKeyLen)
	case !utf8.ValidString(key):
		return errors.New("key is not valid UTF-8")
	case strings.ContainsRune(key, 0):
		return errors.New("key contains NUL")
	}
	return nil
}
