package protocol

import (
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeDefaults(t *testing.T) {
	s, err := Spec{KeyPrefix: "/p/"}.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	want := Spec{Read: ReadThrough, Write: WriteThrough, Sync: SyncWatch, KeyPrefix: "/p/", TTL: DefaultTTL, NegativeTTL: DefaultNegativeTTL}
	if s != want {
		t.Fatalf("got %v, want %v", s, want)
	}
	ra, err := Spec{KeyPrefix: "/p/", Read: RefreshAhead, TTL: time.Minute}.Normalize()
	if err != nil || ra.RefreshAfter != 45*time.Second {
		t.Fatalf("refresh-after default: %v %v", ra.RefreshAfter, err)
	}
	short, _ := Spec{KeyPrefix: "/p/", TTL: time.Second}.Normalize()
	if short.NegativeTTL != time.Second {
		t.Fatalf("negative TTL must not exceed TTL: %v", short.NegativeTTL)
	}
	off, _ := Spec{KeyPrefix: "/p/", NegativeTTL: -5}.Normalize()
	if off.NegativeCaching() || off.NegativeTTL != -1 {
		t.Fatalf("disabled negative caching: %v", off.NegativeTTL)
	}
}

func TestNormalizeRejects(t *testing.T) {
	for name, s := range map[string]Spec{
		"no prefix":           {},
		"ttl too short":       {KeyPrefix: "/p/", TTL: time.Millisecond},
		"refresh after ttl":   {KeyPrefix: "/p/", Read: RefreshAhead, TTL: time.Second, RefreshAfter: time.Second},
		"negative ttl > ttl":  {KeyPrefix: "/p/", TTL: time.Second, NegativeTTL: time.Minute},
		"unknown read":        {KeyPrefix: "/p/", Read: 9},
		"unknown write":       {KeyPrefix: "/p/", Write: 9},
		"write-behind (todo)": {KeyPrefix: "/p/", Write: 3},
	} {
		if _, err := s.Normalize(); err == nil {
			t.Errorf("%s: accepted %v", name, s)
		}
	}
}

func TestProtoRoundTrip(t *testing.T) {
	for _, s := range []Spec{
		{KeyPrefix: "/a/"},
		{KeyPrefix: "/b/", Read: CacheAside, Write: WriteAround, Sync: SyncNone, TTL: time.Hour, NegativeTTL: -1},
		{KeyPrefix: "/c/", Read: RefreshAhead, TTL: time.Minute, RefreshAfter: 10 * time.Second},
	} {
		n, err := s.Normalize()
		if err != nil {
			t.Fatal(err)
		}
		back, err := SpecFromProto(n.ToProto()).Normalize()
		if err != nil || back != n {
			t.Fatalf("round trip: %v -> %v (%v)", n, back, err)
		}
	}
}

func TestValidateNames(t *testing.T) {
	for _, ok := range []string{"magnetar", "a", "kuiper.v2", "a_b-c"} {
		if err := ValidateNamespace(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "Upper", "{tag}", "a:b", "-lead", strings.Repeat("a", 64)} {
		if ValidateNamespace(bad) == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	for _, bad := range []string{"", "a\x00b", "\xff", strings.Repeat("k", MaxKeyLen+1)} {
		if ValidateKey(bad) == nil {
			t.Errorf("key %q accepted", bad)
		}
	}
}

func TestReason(t *testing.T) {
	err := Status(codes.NotFound, ReasonNamespaceNotFound, "gone")
	if Reason(err) != ReasonNamespaceNotFound || status.Code(err) != codes.NotFound {
		t.Fatalf("reason of %v", err)
	}
	if Reason(errors.New("plain")) != "" || Reason(status.Error(codes.Internal, "x")) != "" {
		t.Fatal("errors without ErrorInfo have no reason")
	}
}
