package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mana-sg/StreamDB/pkg/storage"
	"github.com/sirupsen/logrus"
)

func main() {
	// Parse command line flags
	ipfsNodes := flag.String("ipfs-nodes", "localhost:5001", "Comma-separated list of IPFS node addresses")
	fabricConfig := flag.String("fabric-config", "config.json", "Path to Fabric network configuration")
	fabricChannel := flag.String("fabric-channel", "mychannel", "Fabric channel name")
	fabricChaincode := flag.String("fabric-chaincode", "storage", "Fabric chaincode ID")
	kafkaBrokers := flag.String("kafka-brokers", "localhost:9092", "Comma-separated list of Kafka brokers")
	kafkaTopic := flag.String("kafka-topic", "node-status", "Kafka topic for node status")
	flag.Parse()

	// Initialize logger
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetOutput(os.Stdout)

	// Create storage service
	service, err := storage.NewService(
		[]string{*ipfsNodes},
		*fabricConfig,
		*fabricChannel,
		*fabricChaincode,
		[]string{*kafkaBrokers},
		*kafkaTopic,
		logger,
	)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create storage service")
	}
	defer service.Close()

	// Create context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	logger.Info("Shutting down...")
	cancel()
}
