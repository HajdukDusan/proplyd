package cache

import (
	"encoding/binary"
	"time"
)

// Entries are stored as a single []byte so the store holds exactly one
// pointer-free allocation per key:
//
//	[0]      flags
//	[1:9]    version   (int64, big endian)
//	[9:17]   storedAt  (unix nanoseconds, big endian)
//	[17:]    value
//
// version is the store revision the entry reflects: the key's ModRevision
// for present values, or the revision at which absence was observed for
// negative entries. Pending (unflushed write-behind) entries have version 0.
const (
	headerLen   = 17
	flagFound   = 1 << 0
	flagPending = 1 << 1
)

type entry struct {
	found    bool
	pending  bool
	version  int64
	storedAt int64
	value    []byte
}

func encodeEntry(e entry) []byte {
	b := make([]byte, headerLen+len(e.value))
	var flags byte
	if e.found {
		flags |= flagFound
	}
	if e.pending {
		flags |= flagPending
	}
	b[0] = flags
	binary.BigEndian.PutUint64(b[1:9], uint64(e.version))
	binary.BigEndian.PutUint64(b[9:17], uint64(e.storedAt))
	copy(b[headerLen:], e.value)
	return b
}

// decodeEntry returns a view into b; value aliases b and must not be mutated.
func decodeEntry(b []byte) (entry, bool) {
	if len(b) < headerLen {
		return entry{}, false
	}
	return entry{
		found:    b[0]&flagFound != 0,
		pending:  b[0]&flagPending != 0,
		version:  int64(binary.BigEndian.Uint64(b[1:9])),
		storedAt: int64(binary.BigEndian.Uint64(b[9:17])),
		value:    b[headerLen:],
	}, true
}

func (e entry) age(now time.Time) time.Duration {
	return now.Sub(time.Unix(0, e.storedAt))
}
