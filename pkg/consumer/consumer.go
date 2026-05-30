// Package consumer provides a high-level Burrow consumer SDK.
package consumer

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/makhskham/burrow/proto/gen"
)

// Message is a single record returned to the consumer.
type Message struct {
	Offset  int64
	Payload []byte
}

// Consumer fetches messages from a Burrow broker.
type Consumer struct {
	group  string
	conn   *grpc.ClientConn
	client pb.BrokerServiceClient
}

// New connects to brokerAddr as part of the given consumer group.
func New(brokerAddr, group string) (*Consumer, error) {
	conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("consumer: dial: %w", err)
	}
	return &Consumer{group: group, conn: conn, client: pb.NewBrokerServiceClient(conn)}, nil
}

// Fetch retrieves records starting at offset up to maxBytes.
// Returns the messages and the current high watermark.
func (c *Consumer) Fetch(ctx context.Context, topic string, partition int32, offset int64, maxBytes int32) ([]Message, int64, error) {
	resp, err := c.client.Fetch(ctx, &pb.FetchRequest{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		MaxBytes:  maxBytes,
	})
	if err != nil {
		return nil, 0, err
	}
	var msgs []Message
	off := resp.Batch.GetBaseOffset()
	for _, p := range resp.Batch.GetPayloads() {
		msgs = append(msgs, Message{Offset: off, Payload: p})
		off++
	}
	return msgs, resp.Hw, nil
}

// CommitOffset persists the consumer group's current offset.
func (c *Consumer) CommitOffset(ctx context.Context, topic string, partition int32, offset int64) error {
	_, err := c.client.CommitOffset(ctx, &pb.CommitOffsetRequest{
		Group:     c.group,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	})
	return err
}

// FetchOffset retrieves the last committed offset for topic/partition.
func (c *Consumer) FetchOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	resp, err := c.client.FetchOffset(ctx, &pb.FetchOffsetRequest{
		Group:     c.group,
		Topic:     topic,
		Partition: partition,
	})
	if err != nil {
		return 0, err
	}
	return resp.Offset, nil
}

// Poll fetches from the last committed offset and auto-commits on success.
func (c *Consumer) Poll(ctx context.Context, topic string, partition int32, maxBytes int32) ([]Message, error) {
	off, err := c.FetchOffset(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	msgs, _, err := c.Fetch(ctx, topic, partition, off, maxBytes)
	if err != nil {
		return nil, err
	}
	if len(msgs) > 0 {
		if err := c.CommitOffset(ctx, topic, partition, msgs[len(msgs)-1].Offset+1); err != nil {
			return nil, err
		}
	}
	return msgs, nil
}

// PollTimeout polls until at least one message is available or timeout elapses.
func (c *Consumer) PollTimeout(ctx context.Context, topic string, partition int32, maxBytes int32, timeout time.Duration) ([]Message, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs, err := c.Poll(ctx, topic, partition, maxBytes)
		if err != nil {
			return nil, err
		}
		if len(msgs) > 0 {
			return msgs, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil, fmt.Errorf("consumer: poll timeout after %v", timeout)
}

// Close releases the gRPC connection.
func (c *Consumer) Close() error { return c.conn.Close() }
