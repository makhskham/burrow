// Package isr manages the in-sync replica set and high watermark for a partition.
package isr

import (
	"context"
	"sync"
	"time"
)

type replicaState struct {
	leo      int64
	lastSeen time.Time
	inISR    bool
}

// Manager tracks the ISR set and high watermark for one partition.
// The high watermark is min(LEO of all ISR members including leader).
// Producers using acks=all block until HW advances past their write.
type Manager struct {
	mu         sync.Mutex
	leaderLEO  int64
	replicas   map[string]*replicaState
	hw         int64
	lagTimeout time.Duration
	hwCond     *sync.Cond
}

// New creates a Manager. lagTimeout is how long before a lagging replica leaves ISR.
func New(lagTimeout time.Duration) *Manager {
	m := &Manager{replicas: make(map[string]*replicaState), lagTimeout: lagTimeout}
	m.hwCond = sync.NewCond(&m.mu)
	return m
}

// AddReplica registers a new follower broker in the ISR.
func (m *Manager) AddReplica(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replicas[id] = &replicaState{lastSeen: time.Now(), inISR: true}
}

// SetLeaderLEO updates the leader's own LEO after each local append.
func (m *Manager) SetLeaderLEO(leo int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaderLEO = leo
	m.recalcHW()
}

// UpdateLEO is called when a follower reports its new LEO after a fetch.
func (m *Manager) UpdateLEO(id string, leo int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.replicas[id]
	if !ok {
		r = &replicaState{}
		m.replicas[id] = r
	}
	r.leo = leo
	r.lastSeen = time.Now()
	if leo >= m.leaderLEO {
		r.inISR = true
	}
	m.recalcHW()
}

// recalcHW recomputes HW = min(LEO of all ISR members including leader).
// Must be called with m.mu held.
func (m *Manager) recalcHW() {
	min := m.leaderLEO
	for _, r := range m.replicas {
		if r.inISR && r.leo < min {
			min = r.leo
		}
	}
	if min > m.hw {
		m.hw = min
		m.hwCond.Broadcast()
	}
}

// HW returns the current high watermark.
func (m *Manager) HW() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hw
}

// ISR returns the current list of in-sync replica broker IDs.
func (m *Manager) ISR() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for id, r := range m.replicas {
		if r.inISR {
			out = append(out, id)
		}
	}
	return out
}

// Tick checks for lagging replicas and evicts them from ISR.
// Should be called periodically (e.g. every second).
func (m *Manager) Tick() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for _, r := range m.replicas {
		if r.inISR && now.Sub(r.lastSeen) > m.lagTimeout {
			r.inISR = false
		}
	}
	m.recalcHW()
}

// WaitForHW blocks until HW >= offset or ctx is cancelled.
func (m *Manager) WaitForHW(ctx context.Context, offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			m.hwCond.Broadcast()
		case <-done:
		}
	}()
	defer close(done)

	for m.hw < offset {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		m.hwCond.Wait()
	}
	return nil
}
