// Package epoch provides a durable, monotonically-increasing epoch store for
// one partition. The epoch is incremented on every leader election. Incoming
// write requests with a stale epoch are rejected, fencing old leaders.
package epoch

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrStaleEpoch is returned when an incoming epoch is less than the current one.
var ErrStaleEpoch = errors.New("epoch: stale epoch - request rejected")

// Epoch holds the state for one epoch entry.
type Epoch struct {
	Number   int64
	LeaderID string
	HW       int64
}

// Store durably persists the current epoch for one partition.
// Each entry is appended to an epoch.log file; on startup the file is
// replayed to find the latest entry.
type Store struct {
	mu      sync.RWMutex
	current Epoch
	file    *os.File
}

// OpenStore opens or creates the epoch log at dir/epoch.log.
func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "epoch.log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s := &Store{file: f}
	s.replay()
	return s, nil
}

// replay scans the epoch log to find the latest entry.
func (s *Store) replay() {
	data, err := os.ReadFile(s.file.Name())
	if err != nil {
		return
	}
	for pos := 0; pos+20 <= len(data); {
		number := int64(binary.BigEndian.Uint64(data[pos:]))
		hw := int64(binary.BigEndian.Uint64(data[pos+8:]))
		idLen := int(binary.BigEndian.Uint32(data[pos+16:]))
		pos += 20
		if pos+idLen > len(data) {
			break
		}
		s.current = Epoch{Number: number, HW: hw, LeaderID: string(data[pos : pos+idLen])}
		pos += idLen
	}
}

// Increment atomically bumps the epoch, persists it, and returns the new state.
func (s *Store) Increment(leaderID string, hw int64) (Epoch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := Epoch{Number: s.current.Number + 1, LeaderID: leaderID, HW: hw}
	idB := []byte(leaderID)
	entry := make([]byte, 20+len(idB))
	binary.BigEndian.PutUint64(entry[0:8], uint64(next.Number))
	binary.BigEndian.PutUint64(entry[8:16], uint64(hw))
	binary.BigEndian.PutUint32(entry[16:20], uint32(len(idB)))
	copy(entry[20:], idB)

	if _, err := s.file.Write(entry); err != nil {
		return Epoch{}, err
	}
	if err := s.file.Sync(); err != nil {
		return Epoch{}, err
	}
	s.current = next
	return next, nil
}

// Current returns the latest epoch.
func (s *Store) Current() Epoch {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}

// Check returns ErrStaleEpoch if incomingEpoch < current epoch.
func (s *Store) Check(incomingEpoch int64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if incomingEpoch < s.current.Number {
		return fmt.Errorf("%w: got %d current %d", ErrStaleEpoch, incomingEpoch, s.current.Number)
	}
	return nil
}

// Close closes the underlying file.
func (s *Store) Close() error { return s.file.Close() }
