// Package follower implements the replication pull loop.
// A follower continuously fetches records from the leader, appends them to its
// local partition, and reports its LEO back. The leader uses these LEO reports
// to advance the high watermark and maintain the ISR set.
package follower

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/makhskham/burrow/internal/storage/partition"
	pb "github.com/makhskham/burrow/proto/gen"
)

// Follower manages the pull replication loop for one partition.
type Follower struct {
	brokerID      string
	leaderAddr    string
	topic         string
	partitionID   int32
	part          *partition.Partition
	fetchInterval time.Duration
	stop          chan struct{}
	done          chan struct{}
}

// New creates a Follower. Call Start() to begin replication.
func New(brokerID, leaderAddr, topic string, partitionID int32, part *partition.Partition, fetchInterval time.Duration) *Follower {
	return &Follower{
		brokerID:      brokerID,
		leaderAddr:    leaderAddr,
		topic:         topic,
		partitionID:   partitionID,
		part:          part,
		fetchInterval: fetchInterval,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
}

// Start launches the pull loop with the current epoch number.
func (f *Follower) Start(epochNum int64) {
	go f.loop(epochNum)
}

// Stop signals the loop to exit and waits for it to finish.
func (f *Follower) Stop() {
	close(f.stop)
	<-f.done
}

func (f *Follower) loop(epoch int64) {
	defer close(f.done)

	conn, err := grpc.NewClient(f.leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error().Err(err).Str("leader", f.leaderAddr).Msg("follower: failed to connect")
		return
	}
	defer conn.Close()
	client := pb.NewBrokerServiceClient(conn)

	ticker := time.NewTicker(f.fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.stop:
			return
		case <-ticker.C:
			if err := f.fetch(client, epoch); err != nil {
				log.Warn().Err(err).Str("leader", f.leaderAddr).Msg("follower: fetch error")
			}
		}
	}
}

func (f *Follower) fetch(client pb.BrokerServiceClient, epoch int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ReplicateFetch(ctx, &pb.ReplicateFetchRequest{
		BrokerId:  f.brokerID,
		Topic:     f.topic,
		Partition: f.partitionID,
		Offset:    f.part.LEO(),
		MaxBytes:  1 << 20,
		Epoch:     epoch,
	})
	if err != nil {
		return err
	}

	if resp.Batch != nil && len(resp.Batch.Payloads) > 0 {
		if _, err := f.part.Append(resp.Batch.Payloads); err != nil {
			return err
		}
	}

	_, err = client.UpdateLEO(ctx, &pb.UpdateLEORequest{
		BrokerId:  f.brokerID,
		Topic:     f.topic,
		Partition: f.partitionID,
		Leo:       f.part.LEO(),
		Epoch:     epoch,
	})
	return err
}
