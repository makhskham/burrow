// Package producer provides a high-level Burrow producer SDK.
package producer

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/makhskham/burrow/proto/gen"
)

// AcksMode controls the delivery guarantee.
type AcksMode int32

const (
	AcksNone   AcksMode = 0  // fire and forget
	AcksLeader AcksMode = 1  // leader local write confirmed
	AcksAll    AcksMode = -1 // all ISR members confirmed (acks=-1)
)

// Producer sends messages to a Burrow broker.
type Producer struct {
	conn       *grpc.ClientConn
	client     pb.BrokerServiceClient
	producerID int64
	seqNum     atomic.Int64
}

// New connects to brokerAddr and registers a producer ID.
func New(brokerAddr string) (*Producer, error) {
	conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("producer: dial %s: %w", brokerAddr, err)
	}
	client := pb.NewBrokerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.RegisterProducer(ctx, &pb.RegisterProducerRequest{ClientId: "sdk"})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("producer: register: %w", err)
	}
	return &Producer{conn: conn, client: client, producerID: resp.ProducerId}, nil
}

// Send produces payloads to topic/partition with the given acks mode.
// Returns the base offset of the first record written.
func (p *Producer) Send(ctx context.Context, topic string, partition int32, acks AcksMode, payloads ...[]byte) (int64, error) {
	resp, err := p.client.Produce(ctx, &pb.ProduceRequest{
		Topic:      topic,
		Partition:  partition,
		ProducerId: p.producerID,
		SeqNum:     p.seqNum.Add(1),
		Acks:       int32(acks),
		Payloads:   payloads,
	})
	if err != nil {
		return 0, err
	}
	if resp.Redirect != "" {
		return 0, fmt.Errorf("producer: redirect to %s - reconnect required", resp.Redirect)
	}
	return resp.BaseOffset, nil
}

// Close releases the gRPC connection.
func (p *Producer) Close() error { return p.conn.Close() }
