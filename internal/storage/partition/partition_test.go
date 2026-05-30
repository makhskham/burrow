package partition_test

import (
	"testing"

	"github.com/makhskham/burrow/internal/storage/partition"
)

func TestPartition_AppendAndRead(t *testing.T) {
	p, err := partition.Open(t.TempDir(), "test", 0, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	base, err := p.Append([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if base != 0 {
		t.Errorf("base=%d want 0", base)
	}
	if p.LEO() != 3 {
		t.Errorf("LEO=%d want 3", p.LEO())
	}
	records, err := p.Read(1, 1<<20)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records from offset 1 want 2", len(records))
	}
	if string(records[0].Payload) != "b" {
		t.Errorf("records[0]=%q want b", records[0].Payload)
	}
}

func TestPartition_TruncateTo(t *testing.T) {
	p, _ := partition.Open(t.TempDir(), "test", 0, 64<<20)
	defer p.Close()
	p.Append([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")})
	if err := p.TruncateTo(3); err != nil {
		t.Fatalf("TruncateTo: %v", err)
	}
	if p.LEO() != 3 {
		t.Errorf("LEO after truncate=%d want 3", p.LEO())
	}
	records, _ := p.Read(0, 1<<20)
	if len(records) != 3 {
		t.Fatalf("got %d records after truncate want 3", len(records))
	}
}

func TestPartition_Reopens(t *testing.T) {
	dir := t.TempDir()
	p, _ := partition.Open(dir, "test", 0, 64<<20)
	p.Append([][]byte{[]byte("persistent")})
	p.Close()
	p2, err := partition.Open(dir, "test", 0, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if p2.LEO() != 1 {
		t.Errorf("LEO after reopen=%d want 1", p2.LEO())
	}
}
