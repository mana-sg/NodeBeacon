package ipfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	shell "github.com/ipfs/go-ipfs-api"
	"github.com/ipfs/go-cid"
)

// Node represents an IPFS storage node
type Node struct {
	shell *shell.Shell
	addr  string
}

// StreamCh is used to communicate stream events
type StreamCh struct {
	Event string
	Data  interface{}
	Error error
}

// NewNode creates a new IPFS node instance
func NewNode(ipfsAddr string) (*Node, error) {
	sh := shell.NewShell(ipfsAddr)
	if !sh.IsUp() {
		return nil, ErrIPFSNotAvailable
	}

	return &Node{
		shell: sh,
		addr:  ipfsAddr,
	}, nil
}

// Store streams a file to IPFS and returns its CID
func (n *Node) Store(ctx context.Context, filepath string) (cid.Cid, <-chan StreamCh, error) {
	streamCh := make(chan StreamCh, 1)

	// Open file for first operation
	file1, err := os.Open(filepath)
	if err != nil {
		return cid.Cid{}, nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file1.Close()

	// Start the add operation
	cidStr, err := n.shell.Add(file1)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	// Parse the CID
	c, err := cid.Parse(cidStr)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	// Open file again for MFS operation
	file2, err := os.Open(filepath)
	if err != nil {
		return c, nil, fmt.Errorf("failed to reopen file: %v", err)
	}
	defer file2.Close()

	// Get just the filename from the path
	filename := path.Base(filepath)

	// Add to MFS (Files API) to make it visible in WebUI
	err = n.shell.FilesMkdir(ctx, "/my-files", shell.FilesMkdir.Parents(true))
	if err != nil {
		return c, nil, fmt.Errorf("failed to create directory: %v", err)
	}

	err = n.shell.FilesWrite(ctx, "/my-files/"+filename, file2, shell.FilesWrite.Create(true), shell.FilesWrite.Parents(true))
	if err != nil {
		return c, nil, fmt.Errorf("failed to add to MFS: %v", err)
	}

	// Send progress update
	streamCh <- StreamCh{
		Event: "progress",
		Data: map[string]interface{}{
			"cid":    c.String(),
			"status": "stored",
			"name":   filename,
		},
	}
	close(streamCh)

	return c, streamCh, nil
}

// Retrieve fetches a file from IPFS by its CID
func (n *Node) Retrieve(ctx context.Context, c cid.Cid) (io.ReadCloser, error) {
	return n.shell.Cat(c.String())
}

// Pin adds a CID to the local storage
func (n *Node) Pin(ctx context.Context, c cid.Cid) error {
	return n.shell.Pin(c.String())
}

// Unpin removes a CID from local storage
func (n *Node) Unpin(ctx context.Context, c cid.Cid) error {
	return n.shell.Unpin(c.String())
}

// IsAvailable checks if the node is responsive
func (n *Node) IsAvailable() bool {
	return n.shell.IsUp()
}

// GetAddr returns the node's address
func (n *Node) GetAddr() string {
	return n.addr
}

// Errors
var (
	ErrIPFSNotAvailable = errors.New("IPFS node is not available")
)