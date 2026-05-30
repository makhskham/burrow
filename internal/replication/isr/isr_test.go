package isr_test

import (
	"context"
	"testing"
	"time"

	"github.com/makhskham/burrow/internal/replication/isr"
)

func TestISR_HWAdvancesWhenAllCaughtUp(t *testing.T) {
	m := isr.New(5 * time.Second)
	m.AddReplica("b2")
	m.AddReplica("b3")
	m.SetLeaderLEO(10)
	m.UpdateLEO("b2", 10)
	m.UpdateLEO("b3", 10)
	if m.HW() != 10 {
		t.Errorf("HW=%d want 10", m.HW())
	}
}

func TestISR_HWStalledBySlowestMember(t *testing.T) {
	m := isr.New(5 * time.Second)
	m.AddReplica("b2")
	m.AddReplica("b3")
	m.SetLeaderLEO(10)
	m.UpdateLEO("b2", 10)
	m.UpdateLEO("b3", 5)
	if m.HW() != 5 {
		t.Errorf("HW=%d want 5", m.HW())
	}
}

func TestISR_SlowReplicaRemoved(t *testing.T) {
	m := isr.New(50 * time.Millisecond)
	m.AddReplica("b2")
	m.SetLeaderLEO(100)
	time.Sleep(100 * time.Millisecond)
	m.Tick()
	for _, id := range m.ISR() {
		if id == "b2" {
			t.Error("b2 should be removed from ISR after lag timeout")
		}
	}
}

func TestISR_ReplicaRejoins(t *testing.T) {
	m := isr.New(50 * time.Millisecond)
	m.AddReplica("b2")
	m.SetLeaderLEO(100)
	time.Sleep(100 * time.Millisecond)
	m.Tick()
	m.SetLeaderLEO(200)
	m.UpdateLEO("b2", 200)
	m.Tick()
	found := false
	for _, id := range m.ISR() {
		if id == "b2" {
			found = true
		}
	}
	if !found {
		t.Error("b2 should rejoin ISR after catching up")
	}
}

func TestISR_WaitForHW(t *testing.T) {
	m := isr.New(5 * time.Second)
	m.AddReplica("b2")
	m.SetLeaderLEO(5)
	go func() {
		time.Sleep(20 * time.Millisecond)
		m.UpdateLEO("b2", 5)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := m.WaitForHW(ctx, 5); err != nil {
		t.Errorf("WaitForHW: %v", err)
	}
}
