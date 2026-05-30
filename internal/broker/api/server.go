package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/makhskham/burrow/internal/replication/epoch"
	"github.com/makhskham/burrow/internal/replication/isr"
	"github.com/makhskham/burrow/internal/storage/partition"
	"github.com/makhskham/burrow/internal/storage/segment"
	pb "github.com/makhskham/burrow/proto/gen"
)

type partitionKey struct {
	Topic string
	ID    int32
}

type partitionState struct {
	part       *partition.Partition
	isrMgr     *isr.Manager
	epochStore *epoch.Store
	isLeader   bool
	leaderAddr string
}

// Server is the gRPC BrokerService implementation.
type Server struct {
	pb.UnimplementedBrokerServiceServer
	brokerID string
	dataDir  string
	mu       sync.RWMutex
	parts    map[partitionKey]*partitionState
	dedupMu  sync.Mutex
	dedup    map[string]int64
	nextPID  atomic.Int64
	offsetMu sync.Mutex
	offsets  map[string]int64
}

// New creates a Server backed by dataDir for storage.
func New(brokerID, dataDir string) *Server {
	return &Server{
		brokerID: brokerID,
		dataDir:  dataDir,
		parts:    make(map[partitionKey]*partitionState),
		dedup:    make(map[string]int64),
		offsets:  make(map[string]int64),
	}
}

// Listen starts the gRPC server on addr and blocks.
func (s *Server) Listen(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("broker listen: %w", err)
	}
	srv := grpc.NewServer()
	pb.RegisterBrokerServiceServer(srv, s)
	log.Info().Str("addr", addr).Str("broker", s.brokerID).Msg("Broker listening")
	return srv.Serve(lis)
}

// EnsurePartition opens or creates partition state for topic/id.
func (s *Server) EnsurePartition(topic string, id int32, isLeader bool, leaderAddr string, lagTimeout time.Duration) error {
	key := partitionKey{topic, id}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.parts[key]; ok {
		return nil
	}

	part, err := partition.Open(s.dataDir, topic, id, 0)
	if err != nil {
		return err
	}

	epochDir := fmt.Sprintf("%s/epoch/%s-%d", s.dataDir, topic, id)
	es, err := epoch.OpenStore(epochDir)
	if err != nil {
		part.Close()
		return err
	}

	ps := &partitionState{
		part:       part,
		epochStore: es,
		isLeader:   isLeader,
		leaderAddr: leaderAddr,
	}
	if isLeader {
		ps.isrMgr = isr.New(lagTimeout)
	}
	s.parts[key] = ps
	return nil
}

// RegisterProducer assigns a new producer ID.
func (s *Server) RegisterProducer(_ context.Context, _ *pb.RegisterProducerRequest) (*pb.RegisterProducerResponse, error) {
	return &pb.RegisterProducerResponse{ProducerId: s.nextPID.Add(1)}, nil
}

// Produce appends records to the partition. Redirects non-leaders.
func (s *Server) Produce(ctx context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	key := partitionKey{req.Topic, req.Partition}
	s.mu.RLock()
	ps, ok := s.parts[key]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "partition %s-%d not found", req.Topic, req.Partition)
	}
	if !ps.isLeader {
		return &pb.ProduceResponse{Redirect: ps.leaderAddr}, nil
	}

	cur := ps.epochStore.Current()

	// dedup: silently ignore retried produce with same seqNum
	dk := fmt.Sprintf("%d:%s:%d", req.ProducerId, req.Topic, req.Partition)
	s.dedupMu.Lock()
	if last, seen := s.dedup[dk]; seen && req.SeqNum <= last {
		s.dedupMu.Unlock()
		return &pb.ProduceResponse{BaseOffset: ps.part.LEO() - 1, Epoch: cur.Number}, nil
	}
	s.dedup[dk] = req.SeqNum
	s.dedupMu.Unlock()

	base, err := ps.part.Append(req.Payloads)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "append: %v", err)
	}

	newLEO := ps.part.LEO()
	if ps.isrMgr != nil {
		ps.isrMgr.SetLeaderLEO(newLEO)
	}

	// acks=all: wait for HW to advance past the written offset
	if req.Acks == -1 && ps.isrMgr != nil {
		wCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := ps.isrMgr.WaitForHW(wCtx, newLEO); err != nil {
			return nil, status.Errorf(codes.DeadlineExceeded, "acks=all timeout: %v", err)
		}
	}

	return &pb.ProduceResponse{BaseOffset: base, Epoch: cur.Number}, nil
}

// Fetch returns records starting at offset. Only serves up to the HW.
func (s *Server) Fetch(_ context.Context, req *pb.FetchRequest) (*pb.FetchResponse, error) {
	key := partitionKey{req.Topic, req.Partition}
	s.mu.RLock()
	ps, ok := s.parts[key]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "partition not found")
	}

	hw := ps.part.LEO()
	if ps.isrMgr != nil {
		hw = ps.isrMgr.HW()
	}

	maxBytes := int(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	records, err := ps.part.Read(req.Offset, maxBytes)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read: %v", err)
	}

	var payloads [][]byte
	for _, r := range records {
		if r.Offset >= hw {
			break
		}
		payloads = append(payloads, r.Payload)
	}

	return &pb.FetchResponse{
		Batch: &pb.RecordBatch{BaseOffset: req.Offset, Payloads: payloads},
		Hw:    hw,
	}, nil
}

// ReplicateFetch serves follower replication requests.
func (s *Server) ReplicateFetch(_ context.Context, req *pb.ReplicateFetchRequest) (*pb.ReplicateFetchResponse, error) {
	key := partitionKey{req.Topic, req.Partition}
	s.mu.RLock()
	ps, ok := s.parts[key]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "partition not found")
	}
	if !ps.isLeader {
		return nil, status.Errorf(codes.FailedPrecondition, "not leader")
	}
	if err := ps.epochStore.Check(req.Epoch); err != nil {
		if errors.Is(err, epoch.ErrStaleEpoch) {
			return nil, status.Errorf(codes.FailedPrecondition, "stale epoch: %v", err)
		}
	}

	maxBytes := int(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	records, _ := ps.part.Read(req.Offset, maxBytes)
	var payloads [][]byte
	for _, r := range records {
		payloads = append(payloads, r.Payload)
	}

	hw := int64(0)
	if ps.isrMgr != nil {
		hw = ps.isrMgr.HW()
	}

	return &pb.ReplicateFetchResponse{
		Batch:     &pb.RecordBatch{BaseOffset: req.Offset, Payloads: payloads},
		LeaderLeo: ps.part.LEO(),
		Hw:        hw,
		Epoch:     ps.epochStore.Current().Number,
	}, nil
}

// UpdateLEO is called by followers to report their current LEO.
func (s *Server) UpdateLEO(_ context.Context, req *pb.UpdateLEORequest) (*pb.UpdateLEOResponse, error) {
	key := partitionKey{req.Topic, req.Partition}
	s.mu.RLock()
	ps, ok := s.parts[key]
	s.mu.RUnlock()
	if !ok || ps.isrMgr == nil {
		return nil, status.Errorf(codes.NotFound, "partition not found or not leader")
	}
	if err := ps.epochStore.Check(req.Epoch); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "stale epoch")
	}
	ps.isrMgr.UpdateLEO(req.BrokerId, req.Leo)
	return &pb.UpdateLEOResponse{Hw: ps.isrMgr.HW()}, nil
}

// LeaderChanged handles a leader election notification.
func (s *Server) LeaderChanged(_ context.Context, req *pb.LeaderChangedRequest) (*pb.LeaderChangedResponse, error) {
	key := partitionKey{req.Topic, req.Partition}
	s.mu.Lock()
	defer s.mu.Unlock()

	ps, ok := s.parts[key]
	if !ok {
		return &pb.LeaderChangedResponse{}, nil
	}
	if req.Epoch <= ps.epochStore.Current().Number {
		return &pb.LeaderChangedResponse{}, nil
	}

	// truncate uncommitted entries beyond last known HW
	ps.part.TruncateTo(req.Hw)

	if req.LeaderId == s.brokerID {
		ps.isLeader = true
		ps.isrMgr = isr.New(10 * time.Second)
	} else {
		ps.isLeader = false
		ps.leaderAddr = req.LeaderAddr
		ps.isrMgr = nil
	}
	return &pb.LeaderChangedResponse{}, nil
}

// CommitOffset persists the consumer group offset.
func (s *Server) CommitOffset(_ context.Context, req *pb.CommitOffsetRequest) (*pb.CommitOffsetResponse, error) {
	k := fmt.Sprintf("%s:%s:%d", req.Group, req.Topic, req.Partition)
	s.offsetMu.Lock()
	s.offsets[k] = req.Offset
	s.offsetMu.Unlock()
	return &pb.CommitOffsetResponse{}, nil
}

// FetchOffset retrieves the last committed consumer group offset.
func (s *Server) FetchOffset(_ context.Context, req *pb.FetchOffsetRequest) (*pb.FetchOffsetResponse, error) {
	k := fmt.Sprintf("%s:%s:%d", req.Group, req.Topic, req.Partition)
	s.offsetMu.Lock()
	off := s.offsets[k]
	s.offsetMu.Unlock()
	return &pb.FetchOffsetResponse{Offset: off}, nil
}

// AddReplicaToBroker registers a follower with the ISR manager for a partition.
func (s *Server) AddReplicaToBroker(topic string, id int32, brokerID string) {
	key := partitionKey{topic, id}
	s.mu.RLock()
	ps, ok := s.parts[key]
	s.mu.RUnlock()
	if ok && ps.isrMgr != nil {
		ps.isrMgr.AddReplica(brokerID)
	}
}

// ISRTick triggers lag checks on all partitions. Call periodically.
func (s *Server) ISRTick() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ps := range s.parts {
		if ps.isrMgr != nil {
			ps.isrMgr.Tick()
		}
	}
}

// Close shuts down the server and releases all open file handles.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ps := range s.parts {
		ps.part.Close()
		ps.epochStore.Close()
	}
}

// PartitionRecords is a test helper that reads all records from a partition.
func (s *Server) PartitionRecords(topic string, id int32) ([]segment.Record, error) {
	key := partitionKey{topic, id}
	s.mu.RLock()
	ps, ok := s.parts[key]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("partition not found")
	}
	return ps.part.Read(0, 100<<20)
}
