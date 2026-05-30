package segment

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	indexIntervalBytes = 4096
	recordHeaderSize   = 24 // 4(len)+4(crc)+8(offset)+8(ts)
	indexEntrySize     = 8  // 4(rel_offset)+4(file_pos)
)

var ErrCRC = errors.New("segment: CRC mismatch - record corrupt")

// Record is a single message stored on disk.
type Record struct {
	Offset    int64
	Timestamp int64
	Payload   []byte
}

// Segment manages one .log file and its paired sparse .index file.
type Segment struct {
	BaseOffset  int64
	maxBytes    int64
	logPath     string
	indexPath   string
	logFile     *os.File
	indexFile   *os.File
	logWriter   *bufio.Writer
	indexWriter *bufio.Writer
	logSize     int64
	lastIndexed int64
	nextOffset  int64
}

// New creates a new empty segment with the given base offset.
func New(dir string, baseOffset, maxBytes int64) (*Segment, error) {
	s := build(dir, baseOffset, maxBytes)
	var err error
	s.logFile, err = os.OpenFile(s.logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.indexFile, err = os.OpenFile(s.indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		s.logFile.Close()
		return nil, err
	}
	s.logWriter = bufio.NewWriterSize(s.logFile, 64<<10)
	s.indexWriter = bufio.NewWriterSize(s.indexFile, 4<<10)
	return s, nil
}

// Open opens an existing segment for reading and appending.
func Open(dir string, baseOffset, maxBytes int64) (*Segment, error) {
	s := build(dir, baseOffset, maxBytes)
	var err error
	s.logFile, err = os.OpenFile(s.logPath, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.indexFile, err = os.OpenFile(s.indexPath, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		s.logFile.Close()
		return nil, err
	}
	s.logWriter = bufio.NewWriterSize(s.logFile, 64<<10)
	s.indexWriter = bufio.NewWriterSize(s.indexFile, 4<<10)
	if err := s.scan(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func build(dir string, baseOffset, maxBytes int64) *Segment {
	name := fmt.Sprintf("%020d", baseOffset)
	return &Segment{
		BaseOffset: baseOffset,
		maxBytes:   maxBytes,
		nextOffset: baseOffset,
		logPath:    filepath.Join(dir, name+".log"),
		indexPath:  filepath.Join(dir, name+".index"),
	}
}

func (s *Segment) scan() error {
	info, err := s.logFile.Stat()
	if err != nil {
		return err
	}
	s.logSize = info.Size()
	if s.logSize == 0 {
		return nil
	}
	f, err := os.Open(s.logPath)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		rec, err := readRecord(r)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			// corrupt tail - stop scanning but don't fail Open
			break
		}
		if rec.Offset+1 > s.nextOffset {
			s.nextOffset = rec.Offset + 1
		}
	}
	return nil
}

// Append writes payloads to the segment, assigning sequential offsets.
// Returns the base offset of the first record written.
func (s *Segment) Append(payloads [][]byte) (int64, error) {
	base := s.nextOffset
	for _, p := range payloads {
		if err := s.writeRecord(Record{
			Offset:    s.nextOffset,
			Timestamp: time.Now().UnixMilli(),
			Payload:   p,
		}); err != nil {
			return 0, err
		}
		s.nextOffset++
	}
	s.logWriter.Flush()
	s.indexWriter.Flush()
	s.logFile.Sync()
	return base, nil
}

func (s *Segment) writeRecord(rec Record) error {
	frame := make([]byte, recordHeaderSize+len(rec.Payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(rec.Payload)))
	binary.BigEndian.PutUint64(frame[8:16], uint64(rec.Offset))
	binary.BigEndian.PutUint64(frame[16:24], uint64(rec.Timestamp))
	copy(frame[24:], rec.Payload)
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(frame[8:]))

	before := s.logSize
	n, err := s.logWriter.Write(frame)
	s.logSize += int64(n)
	if err != nil {
		return err
	}

	if s.logSize-s.lastIndexed >= indexIntervalBytes || s.lastIndexed == 0 {
		relOff := rec.Offset - s.BaseOffset
		if relOff > math.MaxUint32 {
			return fmt.Errorf("segment: relative offset overflow")
		}
		entry := make([]byte, indexEntrySize)
		binary.BigEndian.PutUint32(entry[0:4], uint32(relOff))
		binary.BigEndian.PutUint32(entry[4:8], uint32(before))
		s.indexWriter.Write(entry)
		s.lastIndexed = s.logSize
	}
	return nil
}

// Read returns records starting at offset, up to maxBytes total payload.
func (s *Segment) Read(offset int64, maxBytes int) ([]Record, error) {
	s.logWriter.Flush()
	startPos, err := s.findPosition(offset)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(s.logPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	f.Seek(startPos, io.SeekStart)
	r := bufio.NewReader(f)
	var records []Record
	totalBytes := 0
	for {
		rec, err := readRecord(r)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if rec.Offset < offset {
			continue
		}
		records = append(records, rec)
		totalBytes += len(rec.Payload)
		if maxBytes > 0 && totalBytes >= maxBytes {
			break
		}
	}
	return records, nil
}

func readRecord(r io.Reader) (Record, error) {
	header := make([]byte, recordHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return Record{}, err
	}
	payloadLen := int(binary.BigEndian.Uint32(header[0:4]))
	storedCRC := binary.BigEndian.Uint32(header[4:8])
	rest := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, rest); err != nil {
		return Record{}, err
	}
	computed := crc32.ChecksumIEEE(append(header[8:], rest...))
	if computed != storedCRC {
		return Record{}, ErrCRC
	}
	return Record{
		Offset:    int64(binary.BigEndian.Uint64(header[8:16])),
		Timestamp: int64(binary.BigEndian.Uint64(header[16:24])),
		Payload:   rest,
	}, nil
}

func (s *Segment) findPosition(offset int64) (int64, error) {
	s.indexWriter.Flush()
	data, err := os.ReadFile(s.indexPath)
	if err != nil || len(data) < indexEntrySize {
		return 0, nil
	}
	n := len(data) / indexEntrySize
	relTarget := uint32(offset - s.BaseOffset)
	pos := sort.Search(n, func(i int) bool {
		return binary.BigEndian.Uint32(data[i*indexEntrySize:]) > relTarget
	}) - 1
	if pos < 0 {
		return 0, nil
	}
	return int64(binary.BigEndian.Uint32(data[pos*indexEntrySize+4:])), nil
}

// LEO returns the next offset to be assigned.
func (s *Segment) LEO() int64 { return s.nextOffset }

// IsFull reports whether the segment has reached its size limit.
func (s *Segment) IsFull() bool { return s.logSize >= s.maxBytes }

// TruncateTo removes all records with offset >= target.
func (s *Segment) TruncateTo(target int64) error {
	s.logWriter.Flush()
	f, err := os.Open(s.logPath)
	if err != nil {
		return err
	}
	r := bufio.NewReader(f)
	var truncPos, pos int64
	for {
		rec, err := readRecord(r)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			f.Close()
			return err
		}
		if rec.Offset >= target {
			truncPos = pos
			break
		}
		pos += int64(recordHeaderSize + len(rec.Payload))
		truncPos = pos
	}
	f.Close()
	if err := s.logFile.Truncate(truncPos); err != nil {
		return err
	}
	s.logSize = truncPos
	s.nextOffset = target
	os.Remove(s.indexPath)
	s.indexFile.Close()
	s.indexFile, err = os.OpenFile(s.indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.indexWriter = bufio.NewWriterSize(s.indexFile, 4<<10)
	s.lastIndexed = 0
	return nil
}

// Close flushes and closes both files.
func (s *Segment) Close() error {
	s.logWriter.Flush()
	s.indexWriter.Flush()
	s.logFile.Sync()
	s.logFile.Close()
	return s.indexFile.Close()
}

// Remove deletes the segment files.
func (s *Segment) Remove() error {
	s.Close()
	os.Remove(s.logPath)
	return os.Remove(s.indexPath)
}

// ListSegmentBaseOffsets returns sorted base offsets of all segments in dir.
func ListSegmentBaseOffsets(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var offsets []int64
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			if off, err := strconv.ParseInt(strings.TrimSuffix(e.Name(), ".log"), 10, 64); err == nil {
				offsets = append(offsets, off)
			}
		}
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	return offsets, nil
}
