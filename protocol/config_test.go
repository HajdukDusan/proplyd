package protocol

import (
	"testing"
	"time"
)

func TestProtoRoundTrip(t *testing.T) {
	in := Namespace{
		Read: RefreshAhead, Write: WriteBehind, Eviction: EvictLFU, Capacity: 42,
		TTL: time.Minute, KeyPrefix: "/x/", RefreshAfter: 30 * time.Second,
		FlushInterval: time.Second, MaxBatch: 64, MaxPending: 1000,
		Sync: SyncWatch, CacheMisses: true, MaxValueBytes: 1024,
	}
	out, err := FromProto(in.ToProto())
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("round trip:\n in  %+v\n out %+v", in, out)
	}
}

func TestDefaults(t *testing.T) {
	d := Namespace{KeyPrefix: "/x/", Write: WriteAround}.WithDefaults()
	if d.Sync != SyncWatch || d.Eviction != EvictSIEVE || d.Capacity != DefaultCapacity || d.MaxValueBytes != DefaultMaxValueBytes {
		t.Fatalf("%+v", d)
	}
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	wb := Namespace{KeyPrefix: "/x/", Write: WriteBehind}.WithDefaults()
	if wb.FlushInterval != DefaultFlushInterval || wb.MaxBatch != DefaultMaxBatch || wb.Sync != SyncNone {
		t.Fatalf("%+v", wb)
	}
	ra := Namespace{KeyPrefix: "/x/", Read: RefreshAhead, TTL: 4 * time.Second}.WithDefaults()
	if ra.RefreshAfter != 3*time.Second {
		t.Fatalf("refresh after %v", ra.RefreshAfter)
	}
}

func TestFromProtoRejectsNil(t *testing.T) {
	if _, err := FromProto(nil); err == nil {
		t.Fatal("expected error")
	}
}
