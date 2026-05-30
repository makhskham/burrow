package epoch_test

import (
	"testing"

	"github.com/makhskham/burrow/internal/replication/epoch"
)

func TestEpoch_StartsAtZero(t *testing.T) {
	s, err := epoch.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Current().Number != 0 {
		t.Errorf("initial epoch=%d want 0", s.Current().Number)
	}
}

func TestEpoch_IncrementAndPersist(t *testing.T) {
	dir := t.TempDir()
	s, err := epoch.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.Increment("broker1", 42)
	if err != nil {
		t.Fatal(err)
	}
	if e.Number != 1 {
		t.Errorf("epoch=%d want 1", e.Number)
	}
	if e.HW != 42 {
		t.Errorf("HW=%d want 42", e.HW)
	}
	s.Close()

	s2, err := epoch.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.Current().Number != 1 {
		t.Errorf("epoch after reopen=%d want 1", s2.Current().Number)
	}
}

func TestEpoch_CheckRejectsStale(t *testing.T) {
	s, err := epoch.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Increment("b1", 0)
	s.Increment("b2", 0)
	if err := s.Check(2); err != nil {
		t.Errorf("Check(2) should pass: %v", err)
	}
	if err := s.Check(1); err == nil {
		t.Error("Check(1) should fail with stale epoch")
	}
}
