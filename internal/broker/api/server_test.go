package api_test

import (
	"context"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/makhskham/burrow/internal/broker/api"
	pb "github.com/makhskham/burrow/proto/gen"
)

func startBroker(t *testing.T, id, addr string) *api.Server {
	t.Helper()
	dir, err := os.MkdirTemp("", "burrow-test-*")
	if err != nil {
		t.Fatal(err)
	}
	srv := api.New(id, dir)
	if err := srv.EnsurePartition("test", 0, true, "", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	go srv.Listen(addr) //nolint
	time.Sleep(50 * time.Millisecond)
	t.Cleanup(func() {
		srv.Close()
		os.RemoveAll(dir)
	})
	return srv
}

func dial(t *testing.T, addr string) pb.BrokerServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewBrokerServiceClient(conn)
}

func TestBroker_ProduceAndFetch(t *testing.T) {
	startBroker(t, "b1", "127.0.0.1:19101")
	c := dial(t, "127.0.0.1:19101")

	reg, err := c.RegisterProducer(context.Background(), &pb.RegisterProducerRequest{})
	if err != nil {
		t.Fatal(err)
	}

	prodResp, err := c.Produce(context.Background(), &pb.ProduceRequest{
		Topic:      "test",
		Partition:  0,
		ProducerId: reg.ProducerId,
		SeqNum:     1,
		Acks:       1,
		Payloads:   [][]byte{[]byte("hello"), []byte("world")},
	})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if prodResp.BaseOffset != 0 {
		t.Errorf("base=%d want 0", prodResp.BaseOffset)
	}

	fetchResp, err := c.Fetch(context.Background(), &pb.FetchRequest{
		Topic:     "test",
		Partition: 0,
		Offset:    0,
		MaxBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(fetchResp.Batch.Payloads) != 2 {
		t.Errorf("got %d payloads want 2", len(fetchResp.Batch.Payloads))
	}
	if string(fetchResp.Batch.Payloads[0]) != "hello" {
		t.Errorf("payload[0]=%q want hello", fetchResp.Batch.Payloads[0])
	}
}

func TestBroker_DeduplicatesRetry(t *testing.T) {
	startBroker(t, "b1-dedup", "127.0.0.1:19102")
	c := dial(t, "127.0.0.1:19102")

	reg, _ := c.RegisterProducer(context.Background(), &pb.RegisterProducerRequest{})
	req := &pb.ProduceRequest{
		Topic: "test", Partition: 0,
		ProducerId: reg.ProducerId, SeqNum: 1,
		Acks: 1, Payloads: [][]byte{[]byte("once")},
	}
	c.Produce(context.Background(), req)
	c.Produce(context.Background(), req) // retry same seqNum

	fetchResp, _ := c.Fetch(context.Background(), &pb.FetchRequest{
		Topic: "test", Partition: 0, Offset: 0, MaxBytes: 1 << 20,
	})
	if len(fetchResp.Batch.Payloads) != 1 {
		t.Errorf("got %d payloads after dedup want 1", len(fetchResp.Batch.Payloads))
	}
}
