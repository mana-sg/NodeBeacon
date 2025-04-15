package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/mana-sg/StreamDB/pkg/blockchain/fabric"
	"github.com/mana-sg/StreamDB/pkg/monitoring/kafka"
	"github.com/mana-sg/StreamDB/pkg/storage/ipfs"
	"github.com/sirupsen/logrus"
)

// Service represents the main storage service
type Service struct {
	nodes      map[string]*ipfs.Node
	blockchain *fabric.Client
	monitor    *kafka.Monitor
	logger     *logrus.Logger
	mu         sync.RWMutex
}

// NewService creates a new storage service
func NewService(
	ipfsNodes []string,
	fabricConfig string,
	fabricChannel string,
	fabricChaincode string,
	kafkaBrokers []string,
	kafkaTopic string,
	logger *logrus.Logger,
) (*Service, error) {
	// Initialize IPFS nodes
	nodes := make(map[string]*ipfs.Node)
	for _, addr := range ipfsNodes {
		node, err := ipfs.NewNode(addr, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create IPFS node: %v", err)
		}
		nodes[node.ID] = node
	}

	// Initialize blockchain client
	blockchain, err := fabric.NewClient(fabricConfig, fabricChannel, fabricChaincode, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create blockchain client: %v", err)
	}

	// Initialize Kafka monitor
	monitor, err := kafka.NewMonitor(kafkaBrokers, kafkaTopic, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka monitor: %v", err)
	}

	service := &Service{
		nodes:      nodes,
		blockchain: blockchain,
		monitor:    monitor,
		logger:     logger,
	}

	// Start monitoring
	if err := service.startMonitoring(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to start monitoring: %v", err)
	}

	return service, nil
}

// Store stores a file in the distributed storage system
func (s *Service) Store(ctx context.Context, reader io.Reader, owner string) (string, error) {
	// Calculate content hash
	hash := sha256.New()
	if _, err := io.Copy(hash, reader); err != nil {
		return "", fmt.Errorf("failed to calculate hash: %v", err)
	}
	contentHash := hex.EncodeToString(hash.Sum(nil))

	// Reset reader
	reader = io.NopCloser(io.MultiReader(io.NopCloser(reader), reader))

	// Store file in IPFS nodes
	var cid string
	var err error
	s.mu.RLock()
	for _, node := range s.nodes {
		cid, err = node.Store(ctx, reader)
		if err == nil {
			break
		}
	}
	s.mu.RUnlock()

	if err != nil {
		return "", fmt.Errorf("failed to store file in any node: %v", err)
	}

	// Create metadata
	metadata := fabric.FileMetadata{
		CID:         cid,
		Owner:       owner,
		ContentHash: contentHash,
		ReplicaNodes: make([]string, 0, len(s.nodes)),
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}

	// Get node IDs for replicas
	s.mu.RLock()
	for nodeID := range s.nodes {
		metadata.ReplicaNodes = append(metadata.ReplicaNodes, nodeID)
	}
	s.mu.RUnlock()

	// Store metadata in blockchain
	if err := s.blockchain.StoreMetadata(metadata); err != nil {
		return "", fmt.Errorf("failed to store metadata: %v", err)
	}

	return cid, nil
}

// Retrieve retrieves a file from the distributed storage system
func (s *Service) Retrieve(ctx context.Context, cid string) (io.ReadCloser, error) {
	// Get metadata from blockchain
	metadata, err := s.blockchain.GetMetadata(cid)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %v", err)
	}

	// Try to retrieve from replica nodes
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, nodeID := range metadata.ReplicaNodes {
		if node, ok := s.nodes[nodeID]; ok {
			reader, err := node.Retrieve(ctx, cid)
			if err == nil {
				return reader, nil
			}
		}
	}

	return nil, fmt.Errorf("failed to retrieve file from any replica node")
}

// startMonitoring starts the node monitoring system
func (s *Service) startMonitoring(ctx context.Context) error {
	// Register handlers for each node
	s.mu.RLock()
	for nodeID := range s.nodes {
		s.monitor.RegisterHandler(nodeID, func(status kafka.NodeStatus) {
			s.handleNodeStatus(status)
		})
	}
	s.mu.RUnlock()

	// Start monitoring
	return s.monitor.StartMonitoring(ctx)
}

// handleNodeStatus handles node status updates
func (s *Service) handleNodeStatus(status kafka.NodeStatus) {
	if status.Status != "healthy" {
		s.logger.WithField("nodeID", status.NodeID).Warn("Node reported unhealthy status")
		// TODO: Implement node recovery logic
	}
}

// Close closes the storage service
func (s *Service) Close() error {
	if err := s.monitor.Close(); err != nil {
		return fmt.Errorf("failed to close monitor: %v", err)
	}
	s.blockchain.Close()
	return nil
} 