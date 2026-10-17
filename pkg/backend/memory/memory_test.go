package memory_test

import (
	"testing"

	"github.com/c12s/proplyd/pkg/backend/backendtest"
	"github.com/c12s/proplyd/pkg/backend/faulty"
	"github.com/c12s/proplyd/pkg/backend/memory"
)

func TestConformance(t *testing.T) {
	s := memory.New(memory.Options{})
	backendtest.Run(t, s, s.Compact)
}

// The fault wrapper must not change behaviour while no fault is active.
func TestFaultyConformance(t *testing.T) {
	s := memory.New(memory.Options{})
	backendtest.Run(t, faulty.New(s), s.Compact)
}

func TestKeepHistory(t *testing.T) {
	s := memory.New(memory.Options{KeepHistory: 10})
	for range 100 {
		if _, err := s.Put(t.Context(), "/k", []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if b := <-s.Watch(t.Context(), "/", 2); b.Err == nil {
		t.Fatal("old history should be compacted")
	}
}
