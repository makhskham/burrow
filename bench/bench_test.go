package bench_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/makhskham/burrow/internal/broker/api"
	"github.com/makhskham/burrow/pkg/consumer"
	"github.com/makhskham/burrow/pkg/producer"
)

func startBench(b *testing.B, addr string) {
	b.Helper()
	dir := b.TempDir()
	srv := api.New("bench-broker", dir)
	if err := srv.EnsurePartition("bench", 0, true, "", 10*time.Second); err != nil {
		b.Fatal(err)
	}
	go srv.Listen(addr) //nolint
	time.Sleep(50 * time.Millisecond)
	b.Cleanup(srv.Close)
}

// BenchmarkProduce_Sequential measures single-producer throughput with acks=1.
func BenchmarkProduce_Sequential(b *testing.B) {
	startBench(b, "127.0.0.1:19200")
	p, err := producer.New("127.0.0.1:19200")
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	payload := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		p.Send(ctx, "bench", 0, producer.AcksLeader, payload) //nolint
		cancel()
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/sec")
}

// BenchmarkProduce_Concurrent50 measures throughput under 50 concurrent producers.
func BenchmarkProduce_Concurrent50(b *testing.B) {
	benchConcurrent(b, 50, "127.0.0.1:19201")
}

// BenchmarkProduce_Concurrent500 measures throughput under 500 concurrent producers.
func BenchmarkProduce_Concurrent500(b *testing.B) {
	benchConcurrent(b, 500, "127.0.0.1:19202")
}

func benchConcurrent(b *testing.B, concurrency int, addr string) {
	b.Helper()
	startBench(b, addr)
	payload := make([]byte, 128)
	var ops atomic.Int64
	b.SetParallelism(concurrency)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		p, err := producer.New(addr)
		if err != nil {
			b.Error(err)
			return
		}
		defer p.Close()
		for pb.Next() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			p.Send(ctx, "bench", 0, producer.AcksLeader, payload) //nolint
			cancel()
			ops.Add(1)
		}
	})
	b.ReportMetric(float64(ops.Load())/b.Elapsed().Seconds(), "msgs/sec")
}

// BenchmarkConsume measures consumer fetch throughput on a pre-filled partition.
func BenchmarkConsume(b *testing.B) {
	startBench(b, "127.0.0.1:19203")
	p, err := producer.New("127.0.0.1:19203")
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 10000; i++ {
		p.Send(ctx, "bench", 0, producer.AcksLeader, []byte(fmt.Sprintf("m%d", i))) //nolint
	}
	p.Close()

	c, err := consumer.New("127.0.0.1:19203", "bench-group")
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	var offset int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs, _, _ := c.Fetch(ctx, "bench", 0, offset, 64<<10)
		if len(msgs) > 0 {
			offset = msgs[len(msgs)-1].Offset + 1
			if offset >= 10000 {
				offset = 0
			}
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "fetches/sec")
}
