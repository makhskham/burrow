//go:build chaos

package chaos_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/makhskham/burrow/pkg/consumer"
	"github.com/makhskham/burrow/pkg/producer"
)

// These tests require a running 3-broker cluster + Toxiproxy.
// Run with: go test -tags chaos -v ./chaos/toxiproxy/...

const (
	toxiproxyAPI = "http://localhost:8474"
	directAddr   = "localhost:19092"
)

func addToxic(t *testing.T, proxy, name, kind string, attrs map[string]interface{}) {
	t.Helper()
	var parts []string
	for k, v := range attrs {
		parts = append(parts, fmt.Sprintf(`"%s":%v`, k, v))
	}
	body := fmt.Sprintf(`{"name":%q,"type":%q,"attributes":{%s}}`, name, kind, strings.Join(parts, ","))
	resp, err := http.Post(fmt.Sprintf("%s/proxies/%s/toxics", toxiproxyAPI, proxy), "application/json", strings.NewReader(body))
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("addToxic failed: %v", err)
	}
}

func removeToxic(t *testing.T, proxy, name string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/proxies/%s/toxics/%s", toxiproxyAPI, proxy, name), nil)
	http.DefaultClient.Do(req) //nolint
}

func TestChaos_Latency200ms_NoMessageLoss(t *testing.T) {
	const msgCount = 100

	addToxic(t, "leader-to-follower-latency", "latency", "latency",
		map[string]interface{}{"latency": 200, "jitter": 50})
	defer removeToxic(t, "leader-to-follower-latency", "latency")

	p, err := producer.New(directAddr)
	if err != nil {
		t.Skipf("broker not reachable: %v", err)
	}
	defer p.Close()

	for i := 0; i < msgCount; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := p.Send(ctx, "chaos-test", 0, producer.AcksAll, []byte(fmt.Sprintf("msg-%d", i)))
		cancel()
		if err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}

	removeToxic(t, "leader-to-follower-latency", "latency")
	time.Sleep(500 * time.Millisecond)

	c, _ := consumer.New(directAddr, "chaos-verify")
	defer c.Close()
	msgs, _, err := c.Fetch(context.Background(), "chaos-test", 0, 0, 10<<20)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) != msgCount {
		t.Errorf("got %d messages want %d - possible data loss", len(msgs), msgCount)
	}
	for i, m := range msgs {
		if string(m.Payload) != fmt.Sprintf("msg-%d", i) {
			t.Errorf("msg[%d]=%q want msg-%d", i, m.Payload, i)
		}
	}
}

func TestChaos_NetworkPartition_LeaderElection(t *testing.T) {
	addToxic(t, "network-partition", "partition", "timeout",
		map[string]interface{}{"timeout": 0})

	time.Sleep(4 * time.Second)

	p, err := producer.New("localhost:29092")
	if err != nil {
		t.Skipf("broker2 not reachable: %v", err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := p.Send(ctx, "chaos-test", 0, producer.AcksLeader, []byte("after-partition")); err != nil {
		t.Errorf("produce after partition: %v", err)
	}

	removeToxic(t, "network-partition", "partition")
}
