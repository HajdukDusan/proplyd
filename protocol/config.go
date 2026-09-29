// Package protocol holds the types shared by the cache server and its
// clients: namespace configuration (with defaults, validation and protobuf
// conversion) and the error reasons carried in gRPC status details.
//
// It intentionally has no dependency on the cache engine so that client
// binaries stay small.
package protocol

import (
	"errors"
	"fmt"
	"time"

	pb "github.com/c12s/proplyd/api/proplydpb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ReadStrategy selects how cache misses are handled.
type ReadStrategy int

const (
	// ReadThrough: the cache loads missing keys from the store itself.
	ReadThrough ReadStrategy = iota
	// CacheAside: the client loads missing keys and fills the cache.
	CacheAside
	// RefreshAhead: read-through, plus entries older than RefreshAfter are
	// reloaded in the background while the cached value is still served.
	RefreshAhead
)

// WriteStrategy selects how writes reach the store.
type WriteStrategy int

const (
	// WriteThrough: the cache writes to the store, then updates itself. When
	// the cache is unreachable, writes fail instead of bypassing it.
	WriteThrough WriteStrategy = iota
	// WriteAround: the cache writes to the store and invalidates its entry.
	// When the cache is unreachable, clients write to the store directly.
	WriteAround
	// WriteBehind: the cache acknowledges immediately and flushes to the
	// store asynchronously. Unflushed writes are lost if the cache dies.
	WriteBehind
)

// SyncMode controls whether the cache follows the store's change stream.
type SyncMode int

const (
	// SyncAuto resolves to SyncWatch for WriteAround and SyncNone otherwise.
	SyncAuto SyncMode = iota
	SyncWatch
	SyncNone
)

// Eviction policies, mirroring store.Policy names.
const (
	EvictLRU      = "lru"
	EvictLFU      = "lfu"
	EvictTinyLFU  = "tinylfu"
	EvictWTinyLFU = "wtinylfu"
	EvictTwoQueue = "2q"
	EvictARC      = "arc"
	EvictFIFO     = "fifo"
	EvictSIEVE    = "sieve"
)

// Defaults.
const (
	DefaultCapacity      = 10_000
	DefaultFlushInterval = 100 * time.Millisecond
	DefaultMaxBatch      = 128 // etcd's default --max-txn-ops
	DefaultMaxPending    = 100_000
	DefaultMaxValueBytes = 1 << 20
)

// Namespace is the configuration of a cache namespace.
type Namespace struct {
	Read     ReadStrategy
	Write    WriteStrategy
	Eviction string
	Capacity int
	// TTL is the default entry lifetime; 0 disables expiry.
	TTL time.Duration
	// KeyPrefix is where the namespace lives in the store, e.g. "/magnetar/".
	KeyPrefix string
	// RefreshAfter is the entry age after which RefreshAhead reloads it.
	RefreshAfter  time.Duration
	FlushInterval time.Duration
	MaxBatch      int
	MaxPending    int
	Sync          SyncMode
	// CacheMisses caches "key does not exist" answers.
	CacheMisses   bool
	MaxValueBytes int
}

// WithDefaults returns a copy with every unset field defaulted and SyncAuto
// resolved.
func (n Namespace) WithDefaults() Namespace {
	if n.Eviction == "" {
		n.Eviction = EvictSIEVE
	}
	if n.Capacity == 0 {
		n.Capacity = DefaultCapacity
	}
	if n.Read == RefreshAhead && n.RefreshAfter == 0 && n.TTL > 0 {
		n.RefreshAfter = n.TTL * 3 / 4
	}
	if n.Write == WriteBehind {
		if n.FlushInterval == 0 {
			n.FlushInterval = DefaultFlushInterval
		}
		if n.MaxBatch == 0 {
			n.MaxBatch = DefaultMaxBatch
		}
		if n.MaxPending == 0 {
			n.MaxPending = DefaultMaxPending
		}
	}
	if n.Sync == SyncAuto {
		if n.Write == WriteAround {
			n.Sync = SyncWatch
		} else {
			n.Sync = SyncNone
		}
	}
	if n.MaxValueBytes == 0 {
		n.MaxValueBytes = DefaultMaxValueBytes
	}
	return n
}

// Validate checks a configuration that already had defaults applied.
func (n Namespace) Validate() error {
	var errs []error
	if n.KeyPrefix == "" {
		errs = append(errs, errors.New("key_prefix is required"))
	}
	if n.Read < ReadThrough || n.Read > RefreshAhead {
		errs = append(errs, fmt.Errorf("unknown read strategy %d", n.Read))
	}
	if n.Write < WriteThrough || n.Write > WriteBehind {
		errs = append(errs, fmt.Errorf("unknown write strategy %d", n.Write))
	}
	switch n.Eviction {
	case EvictLRU, EvictLFU, EvictTinyLFU, EvictWTinyLFU, EvictTwoQueue, EvictFIFO, EvictSIEVE:
	case EvictARC:
		// samber/hot v0.13.1's ARC does not enforce its capacity (it grows
		// without bound), which is unacceptable for a shared cache.
		errs = append(errs, errors.New("eviction policy arc is disabled: the underlying implementation does not bound memory"))
	default:
		errs = append(errs, fmt.Errorf("unknown eviction policy %q", n.Eviction))
	}
	if n.Capacity < 0 {
		errs = append(errs, errors.New("capacity must be positive"))
	}
	if n.TTL < 0 || n.RefreshAfter < 0 || n.FlushInterval < 0 {
		errs = append(errs, errors.New("durations must not be negative"))
	}
	if n.Read == RefreshAhead && n.RefreshAfter <= 0 {
		errs = append(errs, errors.New("refresh-ahead requires ttl or refresh_after"))
	}
	if n.TTL > 0 && n.RefreshAfter >= n.TTL {
		errs = append(errs, errors.New("refresh_after must be shorter than ttl"))
	}
	if n.MaxBatch < 0 || n.MaxPending < 0 || n.MaxValueBytes < 0 {
		errs = append(errs, errors.New("limits must not be negative"))
	}
	return errors.Join(errs...)
}

// ToProto converts to the wire representation.
func (n Namespace) ToProto() *pb.NamespaceConfig {
	out := &pb.NamespaceConfig{
		ReadStrategy:   readToProto[n.Read],
		WriteStrategy:  writeToProto[n.Write],
		EvictionPolicy: evictionToProto[n.Eviction],
		Capacity:       int64(n.Capacity),
		KeyPrefix:      n.KeyPrefix,
		SyncMode:       syncToProto[n.Sync],
		CacheMisses:    n.CacheMisses,
		MaxValueBytes:  int64(n.MaxValueBytes),
	}
	if n.TTL > 0 {
		out.Ttl = durationpb.New(n.TTL)
	}
	if n.RefreshAfter > 0 {
		out.RefreshAfter = durationpb.New(n.RefreshAfter)
	}
	if n.FlushInterval > 0 || n.MaxBatch > 0 || n.MaxPending > 0 {
		out.WriteBehind = &pb.WriteBehindConfig{
			MaxBatch:   int32(n.MaxBatch),
			MaxPending: int64(n.MaxPending),
		}
		if n.FlushInterval > 0 {
			out.WriteBehind.FlushInterval = durationpb.New(n.FlushInterval)
		}
	}
	return out
}

// FromProto converts from the wire representation. Unknown enum values are
// rejected.
func FromProto(c *pb.NamespaceConfig) (Namespace, error) {
	if c == nil {
		return Namespace{}, errors.New("config is required")
	}
	n := Namespace{
		Capacity:      int(c.GetCapacity()),
		KeyPrefix:     c.GetKeyPrefix(),
		CacheMisses:   c.GetCacheMisses(),
		MaxValueBytes: int(c.GetMaxValueBytes()),
		TTL:           c.GetTtl().AsDuration(),
		RefreshAfter:  c.GetRefreshAfter().AsDuration(),
	}
	var ok bool
	if n.Read, ok = readFromProto[c.GetReadStrategy()]; !ok {
		return Namespace{}, fmt.Errorf("unknown read strategy %v", c.GetReadStrategy())
	}
	if n.Write, ok = writeFromProto[c.GetWriteStrategy()]; !ok {
		return Namespace{}, fmt.Errorf("unknown write strategy %v", c.GetWriteStrategy())
	}
	if n.Eviction, ok = evictionFromProto[c.GetEvictionPolicy()]; !ok {
		return Namespace{}, fmt.Errorf("unknown eviction policy %v", c.GetEvictionPolicy())
	}
	if n.Sync, ok = syncFromProto[c.GetSyncMode()]; !ok {
		return Namespace{}, fmt.Errorf("unknown sync mode %v", c.GetSyncMode())
	}
	if wb := c.GetWriteBehind(); wb != nil {
		n.FlushInterval = wb.GetFlushInterval().AsDuration()
		n.MaxBatch = int(wb.GetMaxBatch())
		n.MaxPending = int(wb.GetMaxPending())
	}
	return n, nil
}

var (
	readToProto = map[ReadStrategy]pb.ReadStrategy{
		ReadThrough:  pb.ReadStrategy_READ_STRATEGY_READ_THROUGH,
		CacheAside:   pb.ReadStrategy_READ_STRATEGY_CACHE_ASIDE,
		RefreshAhead: pb.ReadStrategy_READ_STRATEGY_REFRESH_AHEAD,
	}
	readFromProto = map[pb.ReadStrategy]ReadStrategy{
		pb.ReadStrategy_READ_STRATEGY_UNSPECIFIED:   ReadThrough,
		pb.ReadStrategy_READ_STRATEGY_READ_THROUGH:  ReadThrough,
		pb.ReadStrategy_READ_STRATEGY_CACHE_ASIDE:   CacheAside,
		pb.ReadStrategy_READ_STRATEGY_REFRESH_AHEAD: RefreshAhead,
	}
	writeToProto = map[WriteStrategy]pb.WriteStrategy{
		WriteThrough: pb.WriteStrategy_WRITE_STRATEGY_WRITE_THROUGH,
		WriteAround:  pb.WriteStrategy_WRITE_STRATEGY_WRITE_AROUND,
		WriteBehind:  pb.WriteStrategy_WRITE_STRATEGY_WRITE_BEHIND,
	}
	writeFromProto = map[pb.WriteStrategy]WriteStrategy{
		pb.WriteStrategy_WRITE_STRATEGY_UNSPECIFIED:   WriteThrough,
		pb.WriteStrategy_WRITE_STRATEGY_WRITE_THROUGH: WriteThrough,
		pb.WriteStrategy_WRITE_STRATEGY_WRITE_AROUND:  WriteAround,
		pb.WriteStrategy_WRITE_STRATEGY_WRITE_BEHIND:  WriteBehind,
	}
	evictionToProto = map[string]pb.EvictionPolicy{
		EvictLRU:      pb.EvictionPolicy_EVICTION_POLICY_LRU,
		EvictLFU:      pb.EvictionPolicy_EVICTION_POLICY_LFU,
		EvictTinyLFU:  pb.EvictionPolicy_EVICTION_POLICY_TINY_LFU,
		EvictWTinyLFU: pb.EvictionPolicy_EVICTION_POLICY_W_TINY_LFU,
		EvictTwoQueue: pb.EvictionPolicy_EVICTION_POLICY_TWO_QUEUE,
		EvictARC:      pb.EvictionPolicy_EVICTION_POLICY_ARC,
		EvictFIFO:     pb.EvictionPolicy_EVICTION_POLICY_FIFO,
		EvictSIEVE:    pb.EvictionPolicy_EVICTION_POLICY_SIEVE,
	}
	evictionFromProto = map[pb.EvictionPolicy]string{
		pb.EvictionPolicy_EVICTION_POLICY_UNSPECIFIED: "",
		pb.EvictionPolicy_EVICTION_POLICY_LRU:         EvictLRU,
		pb.EvictionPolicy_EVICTION_POLICY_LFU:         EvictLFU,
		pb.EvictionPolicy_EVICTION_POLICY_TINY_LFU:    EvictTinyLFU,
		pb.EvictionPolicy_EVICTION_POLICY_W_TINY_LFU:  EvictWTinyLFU,
		pb.EvictionPolicy_EVICTION_POLICY_TWO_QUEUE:   EvictTwoQueue,
		pb.EvictionPolicy_EVICTION_POLICY_ARC:         EvictARC,
		pb.EvictionPolicy_EVICTION_POLICY_FIFO:        EvictFIFO,
		pb.EvictionPolicy_EVICTION_POLICY_SIEVE:       EvictSIEVE,
	}
	syncToProto = map[SyncMode]pb.SyncMode{
		SyncAuto:  pb.SyncMode_SYNC_MODE_UNSPECIFIED,
		SyncWatch: pb.SyncMode_SYNC_MODE_WATCH,
		SyncNone:  pb.SyncMode_SYNC_MODE_NONE,
	}
	syncFromProto = map[pb.SyncMode]SyncMode{
		pb.SyncMode_SYNC_MODE_UNSPECIFIED: SyncAuto,
		pb.SyncMode_SYNC_MODE_WATCH:       SyncWatch,
		pb.SyncMode_SYNC_MODE_NONE:        SyncNone,
	}
)
