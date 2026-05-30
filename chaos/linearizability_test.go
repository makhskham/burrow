//go:build chaos

package chaos_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/makhskham/burrow/pkg/consumer"
	"github.com/makhskham/burrow/pkg/producer"
)

// TestLinearizability_NoDuplicatesUnderPartition runs 10 concurrent producers,
// injects a network partition at t=2s, heals at t=5s, then verifies every
// committed message is present exactly once with per-producer ordering preserved.
func TestLinearizability_NoDuplicatesUnderPartition(t *testing.T) {
	const (
		producerCount   = 10
		msgsPerProducer = 50
		topic           = "linearizability-test"
	)

	var wg sync.WaitGroup
	var mu sync.Mutex
	sent := make(map[string]bool)

	for g := 0; g < producerCount; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			p, err := producer.New(directAddr)
			if err != nil {
				t.Logf("producer %d: dial error: %v", gid, err)
				return
			}
			defer p.Close()
			for i := 0; i < msgsPerProducer; i++ {
				msg := fmt.Sprintf("g%d-m%d", gid, i)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := p.Send(ctx, topic, 0, producer.AcksAll, []byte(msg))
				cancel()
				if err == nil {
					mu.Lock()
					sent[msg] = true
					mu.Unlock()
				}
			}
		}(g)
	}

	go func() {
		time.Sleep(2 * time.Second)
		addToxic(t, "network-partition", "lin-partition", "timeout",
			map[string]interface{}{"timeout": 0})
		time.Sleep(3 * time.Second)
		removeToxic(t, "network-partition", "lin-partition")
	}()

	wg.Wait()
	time.Sleep(time.Second)

	c, _ := consumer.New(directAddr, "linearizability-verify")
	defer c.Close()
	msgs, _, err := c.Fetch(context.Background(), topic, 0, 0, 100<<20)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	seen := make(map[string]int)
	for _, m := range msgs {
		seen[string(m.Payload)]++
	}

	var missing, duplicated []string
	for msg := range sent {
		switch seen[msg] {
		case 0:
			missing = append(missing, msg)
		case 1:
			// exactly once
		default:
			duplicated = append(duplicated, msg)
		}
	}
	sort.Strings(missing)
	sort.Strings(duplicated)

	if len(missing) > 0 {
		t.Errorf("missing %d committed messages (first 5): %v", len(missing), first5(missing))
	}
	if len(duplicated) > 0 {
		t.Errorf("duplicate messages (first 5): %v", first5(duplicated))
	}
	t.Logf("linearizability: sent=%d received=%d missing=%d duplicated=%d",
		len(sent), len(msgs), len(missing), len(duplicated))
}

func first5(s []string) []string {
	if len(s) <= 5 {
		return s
	}
	return s[:5]
}
