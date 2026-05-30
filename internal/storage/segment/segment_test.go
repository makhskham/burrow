package segment_test

import (
	"os"
	"testing"

	"github.com/makhskham/burrow/internal/storage/segment"
)

func TestSegment_AppendAndRead(t *testing.T) {
	seg, err := segment.New(t.TempDir(), 0, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	base, err := seg.Append([][]byte{[]byte("hello"), []byte("world"), []byte("burrow")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if base != 0 {
		t.Errorf("base=%d want 0", base)
	}

	records, err := seg.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("got %d records want 3", len(records))
	}
	if string(records[0].Payload) != "hello" {
		t.Errorf("records[0]=%q want hello", records[0].Payload)
	}
	if records[2].Offset != 2 {
		t.Errorf("records[2].Offset=%d want 2", records[2].Offset)
	}
}

func TestSegment_ReadFromOffset(t *testing.T) {
	seg, _ := segment.New(t.TempDir(), 0, 64<<20)
	defer seg.Close()
	payloads := make([][]byte, 10)
	for i := range payloads {
		payloads[i] = []byte{byte(i)}
	}
	seg.Append(payloads)
	records, err := seg.Read(5, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 5 {
		t.Fatalf("got %d records from offset 5 want 5", len(records))
	}
	if records[0].Offset != 5 {
		t.Errorf("first offset=%d want 5", records[0].Offset)
	}
}

func TestSegment_CRCCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	seg, _ := segment.New(dir, 0, 64<<20)
	seg.Append([][]byte{[]byte("data")})
	seg.Close()
	f, _ := os.OpenFile(dir+"/00000000000000000000.log", os.O_RDWR, 0)
	f.WriteAt([]byte{0xFF, 0xFF}, 4)
	f.Close()
	seg2, _ := segment.Open(dir, 0, 64<<20)
	defer seg2.Close()
	if _, err := seg2.Read(0, 1<<20); err == nil {
		t.Error("expected CRC error got nil")
	}
}

func TestSegment_IsFull(t *testing.T) {
	seg, _ := segment.New(t.TempDir(), 0, 50)
	defer seg.Close()
	seg.Append([][]byte{[]byte("hello world this is a long payload that fills the segment")})
	if !seg.IsFull() {
		t.Error("segment should be full")
	}
}

func TestSegment_Reopen(t *testing.T) {
	dir := t.TempDir()
	seg, _ := segment.New(dir, 0, 64<<20)
	seg.Append([][]byte{[]byte("a"), []byte("b")})
	seg.Close()
	seg2, err := segment.Open(dir, 0, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer seg2.Close()
	if seg2.LEO() != 2 {
		t.Errorf("LEO after reopen=%d want 2", seg2.LEO())
	}
}
