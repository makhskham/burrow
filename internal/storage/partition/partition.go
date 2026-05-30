package partition

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/makhskham/burrow/internal/storage/segment"
)

// Partition is an ordered, append-only log for a single topic-partition.
// It manages a set of immutable Segments plus one active Segment.
type Partition struct {
	mu          sync.RWMutex
	dir         string
	segments    []*segment.Segment
	active      *segment.Segment
	maxSegBytes int64
}

// Open opens or creates a partition rooted at baseDir/topic-id/.
func Open(baseDir, topic string, id int32, maxSegBytes int64) (*Partition, error) {
	if maxSegBytes <= 0 {
		maxSegBytes = 512 << 20
	}
	dir := filepath.Join(baseDir, fmt.Sprintf("%s-%d", topic, id))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	p := &Partition{dir: dir, maxSegBytes: maxSegBytes}
	offsets, err := segment.ListSegmentBaseOffsets(dir)
	if err != nil {
		return nil, err
	}

	for _, off := range offsets {
		seg, err := segment.Open(dir, off, maxSegBytes)
		if err != nil {
			return nil, err
		}
		p.segments = append(p.segments, seg)
	}

	if len(p.segments) == 0 {
		seg, err := segment.New(dir, 0, maxSegBytes)
		if err != nil {
			return nil, err
		}
		p.segments = []*segment.Segment{seg}
	}

	p.active = p.segments[len(p.segments)-1]
	return p, nil
}

// Append writes payloads to the active segment, rolling if necessary.
// Returns the base offset of the first record appended.
func (p *Partition) Append(payloads [][]byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.active.IsFull() {
		seg, err := segment.New(p.dir, p.active.LEO(), p.maxSegBytes)
		if err != nil {
			return 0, err
		}
		p.segments = append(p.segments, seg)
		p.active = seg
	}
	return p.active.Append(payloads)
}

// Read returns records starting at offset, up to maxBytes total payload.
func (p *Partition) Read(offset int64, maxBytes int) ([]segment.Record, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for i := len(p.segments) - 1; i >= 0; i-- {
		if p.segments[i].BaseOffset <= offset {
			return p.segments[i].Read(offset, maxBytes)
		}
	}
	return nil, fmt.Errorf("partition: offset %d not found", offset)
}

// TruncateTo removes all records with offset >= target.
// Used during leader failover to discard uncommitted entries.
func (p *Partition) TruncateTo(target int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for len(p.segments) > 1 && p.segments[len(p.segments)-1].BaseOffset >= target {
		p.segments[len(p.segments)-1].Remove()
		p.segments = p.segments[:len(p.segments)-1]
	}
	p.active = p.segments[len(p.segments)-1]
	return p.active.TruncateTo(target)
}

// LEO returns the log end offset (next offset to be written).
func (p *Partition) LEO() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active.LEO()
}

// Close closes all segments.
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, seg := range p.segments {
		seg.Close()
	}
	return nil
}
