package cache

import (
	"math"
	"testing"
)

func TestChangeLogAdmits(t *testing.T) {
	c := newChangeLog(4)
	if !c.admits("k", 1) {
		t.Fatal("empty log must admit")
	}
	c.record("k", 10)
	if c.admits("k", 9) {
		t.Fatal("read older than a known change admitted")
	}
	if !c.admits("k", 10) || !c.admits("other", 1) {
		t.Fatal("unrelated or up-to-date reads rejected")
	}
	c.record("k", 5) // out of order: ignored
	if c.admits("k", 9) {
		t.Fatal("older record lowered the bar")
	}
}

func TestChangeLogOverflowRaisesFloor(t *testing.T) {
	c := newChangeLog(2)
	c.record("a", 1)
	c.record("b", 2)
	c.record("c", 3) // forgets a@1
	if c.admits("a", 0) {
		t.Fatal("read older than a forgotten change admitted")
	}
	if !c.admits("a", 1) {
		t.Fatal("read at the floor rejected")
	}
	if c.admits("c", 2) {
		t.Fatal("known change ignored")
	}
}

func TestChangeLogBlockAndReset(t *testing.T) {
	c := newChangeLog(4)
	c.block()
	if c.admits("k", math.MaxInt64-1) {
		t.Fatal("blocked log admitted")
	}
	c.reset(100)
	if c.admits("k", 99) || !c.admits("k", 100) {
		t.Fatal("reset floor")
	}
}

func TestEntryRoundTrip(t *testing.T) {
	for _, e := range []entry{
		{found: true, version: 42, storedAt: 7, value: []byte("hello")},
		{found: false, version: 9},
		{found: true, pending: true, value: []byte{}},
	} {
		got, ok := decodeEntry(encodeEntry(e))
		if !ok || got.found != e.found || got.pending != e.pending || got.version != e.version || got.storedAt != e.storedAt || string(got.value) != string(e.value) {
			t.Fatalf("round trip %+v -> %+v", e, got)
		}
	}
	if _, ok := decodeEntry([]byte{1, 2}); ok {
		t.Fatal("short buffer decoded")
	}
}
