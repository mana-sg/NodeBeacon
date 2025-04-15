package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

// NodeStatus represents the health status of a node
type NodeStatus struct {
	NodeID    string    `json:"nodeId"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// Monitor represents the Kafka-based node monitoring system
type Monitor struct {
	producer sarama.SyncProducer
	consumer sarama.Consumer
	topic    string
	logger   *logrus.Logger
	handlers map[string]func(NodeStatus)
}

// NewMonitor creates a new Kafka monitor
func NewMonitor(brokers []string, topic string, logger *logrus.Logger) (*Monitor, error) {
	// Create producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %v", err)
	}

	// Create consumer
	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		producer.Close()
		return nil, fmt.Errorf("failed to create consumer: %v", err)
	}

	return &Monitor{
		producer: producer,
		consumer: consumer,
		topic:    topic,
		logger:   logger,
		handlers: make(map[string]func(NodeStatus)),
	}, nil
}

// StartMonitoring starts monitoring node heartbeats
func (m *Monitor) StartMonitoring(ctx context.Context) error {
	// Get partition consumer
	partitionConsumer, err := m.consumer.ConsumePartition(m.topic, 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("failed to create partition consumer: %v", err)
	}

	go func() {
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				var status NodeStatus
				if err := json.Unmarshal(msg.Value, &status); err != nil {
					m.logger.WithError(err).Error("Failed to unmarshal node status")
					continue
				}

				// Call registered handlers
				if handler, ok := m.handlers[status.NodeID]; ok {
					handler(status)
				}
			case <-ctx.Done():
				partitionConsumer.Close()
				return
			}
		}
	}()

	return nil
}

// RegisterHandler registers a handler for node status updates
func (m *Monitor) RegisterHandler(nodeID string, handler func(NodeStatus)) {
	m.handlers[nodeID] = handler
}

// SendHeartbeat sends a node heartbeat
func (m *Monitor) SendHeartbeat(nodeID string) error {
	status := NodeStatus{
		NodeID:    nodeID,
		Status:    "healthy",
		Timestamp: time.Now().UTC(),
	}

	statusBytes, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("failed to marshal status: %v", err)
	}

	_, _, err = m.producer.SendMessage(&sarama.ProducerMessage{
		Topic: m.topic,
		Value: sarama.StringEncoder(statusBytes),
	})
	if err != nil {
		return fmt.Errorf("failed to send heartbeat: %v", err)
	}

	return nil
}

// Close closes the monitor
func (m *Monitor) Close() error {
	if err := m.producer.Close(); err != nil {
		return fmt.Errorf("failed to close producer: %v", err)
	}
	if err := m.consumer.Close(); err != nil {
		return fmt.Errorf("failed to close consumer: %v", err)
	}
	return nil
} 