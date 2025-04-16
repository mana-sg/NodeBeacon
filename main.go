package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/mana-sg/StreamDB/pkg/ipfs"
)

func main() {
	// Create a new IPFS node (assuming IPFS daemon is running on default port)
	node, err := ipfs.NewNode("localhost:5001")
	if err != nil {
		log.Fatalf("Failed to create IPFS node: %v", err)
	}

	// Check if IPFS node is available
	if !node.IsAvailable() {
		log.Fatal("IPFS node is not available")
	}

	fmt.Println("IPFS node is available and ready!")

	// Example 1: Store a file
	if len(os.Args) > 1 {
		filePath := os.Args[1]

		fmt.Printf("Storing file: %s\n", filePath)
		c, stream, err := node.Store(context.Background(), filePath)
		if err != nil {
			log.Fatalf("Failed to store file: %v", err)
		}

		// Monitor the upload progress
		for event := range stream {
			if event.Error != nil {
				log.Fatalf("Error during upload: %v", event.Error)
			}
			fmt.Printf("Progress: %+v\n", event.Data)
		}

		fmt.Printf("File stored successfully! CID: %s\n", c.String())

		// Verify the file is accessible
		reader, err := node.Retrieve(context.Background(), c)
		if err != nil {
			log.Printf("Warning: Failed to verify file: %v", err)
		} else {
			defer reader.Close()
			fmt.Println("File verified and accessible")
		}
	} else {
		fmt.Println("Usage: go run main.go <file_path>")
	}
}