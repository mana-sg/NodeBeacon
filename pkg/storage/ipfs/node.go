package ipfs

import (
	"context"
	"io"
	"time"

	shell "github.com/ipfs/go-ipfs-api"
	"github.com/sirupsen/logrus"
)

// Node represents an IPFS storage node
type Node struct {
	sh      *shell.Shell
	ID      string
	Address string
	logger  *logrus.Logger
}

// NewNode creates a new IPFS node
func NewNode(ipfsAPI string, logger *logrus.Logger) (*Node, error) {
	sh := shell.NewShell(ipfsAPI)
	
	// Get node ID
	id, err := sh.ID()
	if err != nil {
		return nil, err
	}

	return &Node{
		sh:      sh,
		ID:      id.ID,
		Address: ipfsAPI,
		logger:  logger,
	}, nil
}

// Store uploads a file to IPFS and returns its CID
func (n *Node) Store(ctx context.Context, reader io.Reader) (string, error) {
	cid, err := n.sh.Add(reader)
	if err != nil {
		n.logger.WithError(err).Error("Failed to store file in IPFS")
		return "", err
	}
	return cid, nil
}

// Retrieve downloads a file from IPFS using its CID
func (n *Node) Retrieve(ctx context.Context, cid string) (io.ReadCloser, error) {
	return n.sh.Cat(cid)
}

// Pin pins a file to ensure it's not garbage collected
func (n *Node) Pin(ctx context.Context, cid string) error {
	return n.sh.Pin(cid)
}

// Unpin removes a pin from a file
func (n *Node) Unpin(ctx context.Context, cid string) error {
	return n.sh.Unpin(cid)
}

// GetNodeInfo returns information about the IPFS node
func (n *Node) GetNodeInfo() map[string]interface{} {
	return map[string]interface{}{
		"id":      n.ID,
		"address": n.Address,
		"time":    time.Now().UTC(),
	}
} 