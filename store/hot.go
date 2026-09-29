package store

import (
	"fmt"
	"hash/maphash"
	"time"

	"github.com/samber/hot"
)

// shardThreshold is the capacity from which the store is split into shards
// to reduce lock contention.
const (
	shardThreshold = 4096
	shardCount     = 16
)

// Hot is an in-process Store built on samber/hot.
//
// hot's own sharding (v0.13) builds its shards without locking, so Hot shards
// itself: each shard is an independent, internally locked hot cache holding
// capacity/shards entries.
type Hot struct {
	shards []*hot.HotCache[string, []byte]
	seed   maphash.Seed
}

var _ Store = (*Hot)(nil)

// NewHot builds a Hot store.
func NewHot(cfg Config) (*Hot, error) {
	algo, err := hotAlgorithm(cfg.Policy)
	if err != nil {
		return nil, err
	}
	if cfg.Capacity <= 0 {
		return nil, fmt.Errorf("store: capacity must be positive, got %d", cfg.Capacity)
	}

	n := 1
	if cfg.Capacity >= shardThreshold {
		n = shardCount
	}
	perShard := (cfg.Capacity + n - 1) / n

	h := &Hot{shards: make([]*hot.HotCache[string, []byte], n), seed: maphash.MakeSeed()}
	for i := range h.shards {
		b := hot.NewHotCache[string, []byte](algo, perShard)
		if cfg.JanitorInterval > 0 {
			// hot runs its janitor at the default-TTL interval. The namespace
			// always passes explicit TTLs, so the default only drives the janitor.
			b = b.WithTTL(cfg.JanitorInterval).WithJanitor()
		}
		h.shards[i] = b.Build()
	}
	return h, nil
}

// HotFactory is a Factory producing Hot stores.
func HotFactory(_ string, cfg Config) (Store, error) { return NewHot(cfg) }

func (h *Hot) shard(key string) *hot.HotCache[string, []byte] {
	if len(h.shards) == 1 {
		return h.shards[0]
	}
	return h.shards[maphash.String(h.seed, key)%uint64(len(h.shards))]
}

func (h *Hot) Get(key string) ([]byte, bool) {
	v, ok, _ := h.shard(key).Get(key)
	return v, ok
}

func (h *Hot) Peek(key string) ([]byte, bool) {
	return h.shard(key).Peek(key)
}

func (h *Hot) Set(key string, value []byte, ttl time.Duration) {
	if ttl < 0 {
		ttl = 0
	}
	h.shard(key).SetWithTTL(key, value, ttl)
}

func (h *Hot) Delete(key string) bool {
	return h.shard(key).Delete(key)
}

func (h *Hot) Purge() {
	for _, s := range h.shards {
		s.Purge()
	}
}

func (h *Hot) Len() int {
	n := 0
	for _, s := range h.shards {
		n += s.Len()
	}
	return n
}

func (h *Hot) Close() error {
	for _, s := range h.shards {
		s.StopJanitor()
	}
	return nil
}

func hotAlgorithm(p Policy) (hot.EvictionAlgorithm, error) {
	switch p {
	case LRU:
		return hot.LRU, nil
	case LFU:
		return hot.LFU, nil
	case TinyLFU:
		return hot.TinyLFU, nil
	case WTinyLFU:
		return hot.WTinyLFU, nil
	case TwoQueue:
		return hot.TwoQueue, nil
	case ARC:
		return hot.ARC, nil
	case FIFO:
		return hot.FIFO, nil
	case SIEVE, "":
		return hot.SIEVE, nil
	default:
		return "", fmt.Errorf("store: unknown eviction policy %q", p)
	}
}
