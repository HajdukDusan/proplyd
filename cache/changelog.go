package cache

import (
	"math"
	"sync"
)

// changeLog remembers the latest known change revision of recently modified
// keys. It protects the cache against the classic cache-aside race:
//
//	reader: load k at rev 10 ─────────────────────────── fill(k, rev 10)
//	writer:                    put k at rev 11 (k not cached)
//
// Without it the reader would install the value from rev 10 after the write.
// A read served at revision R may be installed only if no change to the key
// with revision > R is known. The log is bounded: when an old change is
// forgotten, the floor rises to its revision and reads older than the floor
// are rejected, since the forgotten change might affect them.
type changeLog struct {
	mu    sync.Mutex
	last  map[string]int64
	ring  []change
	head  int
	size  int
	floor int64
}

type change struct {
	key string
	rev int64
}

func newChangeLog(capacity int) *changeLog {
	return &changeLog{
		last: make(map[string]int64, capacity),
		ring: make([]change, capacity),
	}
}

// record notes that key changed at rev.
func (c *changeLog) record(key string, rev int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.last[key]; ok && r >= rev {
		return
	}
	if c.size == len(c.ring) {
		old := c.ring[c.head]
		if c.last[old.key] == old.rev {
			delete(c.last, old.key)
		}
		if old.rev > c.floor {
			c.floor = old.rev
		}
		c.size--
		c.head = (c.head + 1) % len(c.ring)
	}
	c.ring[(c.head+c.size)%len(c.ring)] = change{key: key, rev: rev}
	c.size++
	c.last[key] = rev
}

// admits reports whether a read of key served at readRev may be installed.
func (c *changeLog) admits(key string, readRev int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if readRev < c.floor {
		return false
	}
	if r, ok := c.last[key]; ok && r > readRev {
		return false
	}
	return true
}

// reset forgets everything and rejects all reads older than floor.
func (c *changeLog) reset(floor int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.last)
	c.head, c.size = 0, 0
	c.floor = floor
}

// block rejects every read until the next reset.
func (c *changeLog) block() { c.reset(math.MaxInt64) }
