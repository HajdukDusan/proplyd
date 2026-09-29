package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var policies = []Policy{LRU, LFU, TinyLFU, WTinyLFU, TwoQueue, FIFO, SIEVE}

func TestBasicOperations(t *testing.T) {
	for _, p := range policies {
		t.Run(string(p), func(t *testing.T) {
			s, err := NewHot(Config{Policy: p, Capacity: 16})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			s.Set("k", []byte("v"), 0)
			if v, ok := s.Get("k"); !ok || string(v) != "v" {
				t.Fatalf("get: %q %v", v, ok)
			}
			if v, ok := s.Peek("k"); !ok || string(v) != "v" {
				t.Fatalf("peek: %q %v", v, ok)
			}
			if !s.Delete("k") || s.Len() != 0 {
				t.Fatal("delete")
			}
			s.Set("a", []byte("1"), 0)
			s.Purge()
			if _, ok := s.Get("a"); ok {
				t.Fatal("purge")
			}
		})
	}
}

// Capacity must hold for every policy we allow, sharded or not.
func TestCapacityIsBounded(t *testing.T) {
	for _, capacity := range []int{100, 10_000} { // unsharded, sharded
		for _, p := range policies {
			t.Run(fmt.Sprintf("%s/%d", p, capacity), func(t *testing.T) {
				s, _ := NewHot(Config{Policy: p, Capacity: capacity})
				defer s.Close()
				for i := range capacity * 5 {
					k := fmt.Sprint(i)
					s.Get(k)
					s.Set(k, []byte("v"), 0)
				}
				// Sharding rounds the per-shard capacity up.
				if n := s.Len(); n > capacity+shardCount {
					t.Fatalf("len %d exceeds capacity %d", n, capacity)
				}
			})
		}
	}
}

func TestTTL(t *testing.T) {
	s, _ := NewHot(Config{Policy: LRU, Capacity: 10, JanitorInterval: 10 * time.Millisecond})
	defer s.Close()
	s.Set("short", []byte("v"), 20*time.Millisecond)
	s.Set("forever", []byte("v"), 0)
	time.Sleep(60 * time.Millisecond)
	if _, ok := s.Get("short"); ok {
		t.Fatal("expired entry returned")
	}
	if _, ok := s.Get("forever"); !ok {
		t.Fatal("entry without ttl expired")
	}
}

func TestUnknownPolicy(t *testing.T) {
	if _, err := NewHot(Config{Policy: "random", Capacity: 1}); err == nil {
		t.Fatal("expected error")
	}
	if _, err := NewHot(Config{Policy: LRU}); err == nil {
		t.Fatal("expected capacity error")
	}
}

// Run with -race: guards against unsynchronised shards.
func TestConcurrentAccess(t *testing.T) {
	s, _ := NewHot(Config{Policy: SIEVE, Capacity: 8192})
	defer s.Close()
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 5000 {
				k := fmt.Sprint((g * i) % 3000)
				switch i % 4 {
				case 0:
					s.Set(k, []byte(k), time.Minute)
				case 1:
					s.Get(k)
				case 2:
					s.Peek(k)
				default:
					s.Delete(k)
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkGet(b *testing.B) {
	s, _ := NewHot(Config{Policy: SIEVE, Capacity: 100_000})
	defer s.Close()
	for i := range 100_000 {
		s.Set(fmt.Sprint(i), make([]byte, 128), 0)
	}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(fmt.Sprint(i % 100_000))
			i++
		}
	})
}
