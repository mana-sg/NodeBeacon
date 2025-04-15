package fabric

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hyperledger/fabric-sdk-go/pkg/client/channel"
	"github.com/hyperledger/fabric-sdk-go/pkg/core/config"
	"github.com/hyperledger/fabric-sdk-go/pkg/fabsdk"
	"github.com/sirupsen/logrus"
)

// FileMetadata represents the metadata stored on the blockchain
type FileMetadata struct {
	CID         string    `json:"cid"`
	Owner       string    `json:"owner"`
	ContentHash string    `json:"contentHash"`
	ReplicaNodes []string  `json:"replicaNodes"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// Client represents the Hyperledger Fabric client
type Client struct {
	sdk        *fabsdk.FabricSDK
	channel    *channel.Client
	chaincode  string
	logger     *logrus.Logger
}

// NewClient creates a new Fabric client
func NewClient(configPath, channelID, chaincodeID string, logger *logrus.Logger) (*Client, error) {
	// Initialize the SDK
	sdk, err := fabsdk.New(config.FromRaw([]byte(configPath), "json"))
	if err != nil {
		return nil, fmt.Errorf("failed to create SDK: %v", err)
	}

	// Create channel client
	channelClient, err := channel.New(sdk.ChannelContext(channelID))
	if err != nil {
		return nil, fmt.Errorf("failed to create channel client: %v", err)
	}

	return &Client{
		sdk:       sdk,
		channel:   channelClient,
		chaincode: chaincodeID,
		logger:    logger,
	}, nil
}

// StoreMetadata stores file metadata on the blockchain
func (c *Client) StoreMetadata(metadata FileMetadata) error {
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	// Invoke chaincode to store metadata
	_, err = c.channel.Execute(channel.Request{
		ChaincodeID: c.chaincode,
		Fcn:         "storeMetadata",
		Args:        [][]byte{metadataBytes},
	})
	if err != nil {
		return fmt.Errorf("failed to store metadata: %v", err)
	}

	return nil
}

// GetMetadata retrieves file metadata from the blockchain
func (c *Client) GetMetadata(cid string) (*FileMetadata, error) {
	// Query chaincode to get metadata
	response, err := c.channel.Query(channel.Request{
		ChaincodeID: c.chaincode,
		Fcn:         "getMetadata",
		Args:        [][]byte{[]byte(cid)},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query metadata: %v", err)
	}

	var metadata FileMetadata
	if err := json.Unmarshal(response.Payload, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %v", err)
	}

	return &metadata, nil
}

// UpdateReplicas updates the replica nodes for a file
func (c *Client) UpdateReplicas(cid string, replicaNodes []string) error {
	replicaBytes, err := json.Marshal(replicaNodes)
	if err != nil {
		return fmt.Errorf("failed to marshal replica nodes: %v", err)
	}

	// Invoke chaincode to update replicas
	_, err = c.channel.Execute(channel.Request{
		ChaincodeID: c.chaincode,
		Fcn:         "updateReplicas",
		Args:        [][]byte{[]byte(cid), replicaBytes},
	})
	if err != nil {
		return fmt.Errorf("failed to update replicas: %v", err)
	}

	return nil
}

// Close closes the Fabric client
func (c *Client) Close() {
	if c.sdk != nil {
		c.sdk.Close()
	}
} 