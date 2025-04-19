package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/mana-sg/StreamDB/pkg/ipfs"
)

func main() {
	// Create a new cluster with 1MB chunk size (smaller chunks for testing)
	cluster := ipfs.NewCluster(256 * 1024) // 256KB chunks for testing

	// Add IPFS nodes to the cluster - try different ports if you have multiple nodes
	nodes := []string{
		"localhost:5001", // Primary node
	}

	// Add more nodes if you have them
	additionalNodes := []string{
		"localhost:5002",
		"localhost:5003",
	}

	// First add the main node
	for _, addr := range nodes {
		if err := cluster.AddNode(addr); err != nil {
			log.Fatalf("Failed to add primary node %s: %v", addr, err)
		}
	}

	// Try to add additional nodes but don't fail if they're not available
	for _, addr := range additionalNodes {
		if err := cluster.AddNode(addr); err != nil {
			log.Printf("Note: Additional node %s not available: %v", addr, err)
		}
	}

	// Check if we have at least one node
	availableNodes := cluster.GetAvailableNodes()
	if len(availableNodes) == 0 {
		log.Fatalf("No IPFS nodes available. Please start at least one IPFS daemon.")
	}
	
	log.Printf("Working with %d available nodes: %v", len(availableNodes), availableNodes)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start heartbeat monitor
	go cluster.StartHeartbeatMonitor(ctx)

	// Handle interrupts
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	// Example: Store a file
	if len(os.Args) > 1 {
		filePath := os.Args[1]

		fmt.Printf("Storing file: %s\n", filePath)
		
		// Split file into chunks
		chunks, err := cluster.CM.SplitFile(filePath)
		if err != nil {
			log.Fatalf("Failed to split file: %v", err)
		}

		// Create a channel to track chunk storage progress
		progressCh := make(chan ipfs.StreamCh, len(chunks))
		var wg sync.WaitGroup

		// Store each chunk on a different node
		for i, chunk := range chunks {
			wg.Add(1)
			go func(idx int, ch *ipfs.Chunk) {
				defer wg.Done()

				// Get least loaded node
				nodeAddr, err := cluster.GetLeastLoadedNode()
				if err != nil {
					progressCh <- ipfs.StreamCh{
						Event: "error",
						Error: fmt.Errorf("no available nodes for chunk %d: %v", idx, err),
					}
					return
				}

				// Increment node load
				cluster.IncrementNodeLoad(nodeAddr)
				defer cluster.DecrementNodeLoad(nodeAddr)

				// Get node and store chunk
				node := cluster.Nodes[nodeAddr]
				cidStr, err := node.StoreChunk(ctx, ch)
				if err != nil {
					progressCh <- ipfs.StreamCh{
						Event: "error",
						Error: fmt.Errorf("failed to store chunk %d: %v", idx, err),
					}
					return
				}

				ch.CID = cidStr
				progressCh <- ipfs.StreamCh{
					Event: "progress",
					Data: map[string]interface{}{
						"chunk": idx,
						"cid":   cidStr,
						"node":  nodeAddr,
					},
				}
			}(i, chunk)
		}

		// Monitor progress
		go func() {
			for event := range progressCh {
				if event.Error != nil {
					log.Printf("Error: %v", event.Error)
					continue
				}
				fmt.Printf("Progress: %+v\n", event.Data)
			}
		}()

		// Wait for all chunks to be stored
		wg.Wait()
		close(progressCh)

		// Print chunk information
		fmt.Println("\nChunk Information:")
		for _, chunk := range chunks {
			fmt.Printf("Chunk %d: CID=%s, Checksum=%s\n", 
				chunk.Index, chunk.CID, chunk.Checksum)
		}

		// Store the original file in MFS so it appears in WebUI
		node := cluster.Nodes[availableNodes[0]]
		filename := filepath.Base(filePath)
		tempOutputFile := filepath.Join(os.TempDir(), filename)
		
		// Combine chunks back first
		err = cluster.CM.CombineChunks(chunks, tempOutputFile)
		if err != nil {
			log.Printf("Warning: Failed to combine chunks: %v", err)
		} else {
			// Store the combined file in MFS
			file, err := os.Open(tempOutputFile)
			if err != nil {
				log.Printf("Warning: Failed to open temporary file: %v", err)
			} else {
				defer file.Close()
				defer os.Remove(tempOutputFile)
				
				// Use Node.Store method which internally handles MFS storage
				c, stream, err := node.Store(ctx, tempOutputFile)
				if err != nil {
					log.Printf("Warning: Failed to store file in MFS: %v", err)
				} else {
					for event := range stream {
						if event.Error != nil {
							log.Printf("Error during file storage: %v", event.Error)
						}
					}
					fmt.Printf("\nFile stored in MFS with CID: %s\n", c.String())
					fmt.Printf("You can access it in the IPFS WebUI under Files tab.\n")
				}
			}
		}

		// Example: Retrieve a chunk
		if len(chunks) > 0 {
			fmt.Println("\nRetrieving first chunk as example:")
			nodeAddr, err := cluster.GetLeastLoadedNode()
			if err != nil {
				log.Printf("Warning: Failed to get node for retrieval: %v", err)
			} else {
				node := cluster.Nodes[nodeAddr]
				data, err := node.RetrieveChunk(ctx, chunks[0].CID)
				if err != nil {
					log.Printf("Warning: Failed to retrieve chunk: %v", err)
				} else {
					fmt.Printf("Retrieved chunk data length: %d bytes\n", len(data))
				}
			}
		}
	} else {
		fmt.Println("Usage: go run main.go <file_path>")
	}
}