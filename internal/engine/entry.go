package engine

import (
	"encoding/binary"
	"time"
)

// kind is the type of a cached entry.
type kind byte

const (
	// kindValue: the key exists; the entry holds its value.
	kindValue kind = 1
	// kindNegative: the key does not exist.
	kindNegative kind = 2
	// kindFence: the key changed at rev. Nothing is cached, and a load that
	// read before rev must not be installed.
	kindFence kind = 3
)

// Entry layout in Redis. The Lua scripts read the header with GETRANGE and
// struct.unpack, so it must stay in sync with scripts.go.
//
//	[0]      kind; bit 0x80 is the "quiet" flag (installed by a load whose
//	         change may not be published yet, see installScript)
//	[1:9]    namespace epoch (entries of another epoch are dead)
//	[9:17]   rev, int64 big endian: mod revision of a value, the revision
//	         at which absence was observed, or the fence revision
//	[17:25]  storedAt, unix milliseconds, int64 big endian
//	[25:]    value
const headerLen = 25

const quietFlag = 0x80

type entry struct {
	kind     kind
	epoch    uint64
	rev      int64
	storedAt time.Time
	value    []byte
}

func encodeEntry(k kind, epoch uint64, rev int64, storedAt time.Time, value []byte) []byte {
	b := make([]byte, headerLen+len(value))
	b[0] = byte(k)
	binary.BigEndian.PutUint64(b[1:9], epoch)
	binary.BigEndian.PutUint64(b[9:17], uint64(rev))
	binary.BigEndian.PutUint64(b[17:25], uint64(storedAt.UnixMilli()))
	copy(b[headerLen:], value)
	return b
}

func decodeEntry(b []byte) (entry, bool) {
	if len(b) < headerLen {
		return entry{}, false
	}
	k := kind(b[0] &^ quietFlag)
	if k != kindValue && k != kindNegative && k != kindFence {
		return entry{}, false
	}
	return entry{
		kind:     k,
		epoch:    binary.BigEndian.Uint64(b[1:9]),
		rev:      int64(binary.BigEndian.Uint64(b[9:17])),
		storedAt: time.UnixMilli(int64(binary.BigEndian.Uint64(b[17:25]))),
		value:    b[headerLen:],
	}, true
}

func epochBytes(epoch uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], epoch)
	return string(b[:])
}

func parseEpoch(s string) (uint64, bool) {
	if len(s) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64([]byte(s)), true
}
