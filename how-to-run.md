# How to Run the StreamDB Code

## Overview

This document contains step-by-step instructions for setting up and running the StreamDB decentralized cloud storage system. The system combines IPFS, Hyperledger Fabric, and Apache Kafka to create a fault-tolerant, self-healing storage solution.

## Prerequisites

Before running the code, ensure you have the following installed:

1. Go 1.21 or later
2. Docker and Docker Compose
3. Git

## Step 1: Clone the Repository

```bash
git clone https://github.com/mana-sg/StreamDB.git
cd StreamDB
```

## Step 2: Install Dependencies

### Install Go Dependencies

```bash
go mod download
```

### Install IPFS

```bash
# Download and install IPFS
wget https://dist.ipfs.tech/kubo/v0.18.1/kubo_v0.18.1_linux-amd64.tar.gz
tar -xvzf kubo_v0.18.1_linux-amd64.tar.gz
cd kubo
sudo bash install.sh

# Initialize IPFS
ipfs init

# Start IPFS daemon
ipfs daemon
```

### Set Up Hyperledger Fabric

```bash
# Download Fabric samples and binaries
curl -sSL https://bit.ly/2ysbOFE | bash -s

# Start the Fabric network
cd fabric-samples/test-network
./network.sh up createChannel -c mychannel -ca
```

## Step 3: Start Required Services

### Start Kafka and IPFS using Docker Compose

```bash
# Start Kafka and IPFS
docker-compose up -d
```

This will start:
- Zookeeper (required for Kafka)
- Kafka broker
- IPFS node

## Step 4: Verify Services are Running

### Check IPFS

```bash
ipfs id
```

You should see output showing your IPFS node ID and addresses.

### Check Kafka

```bash
docker-compose ps
```

All services should show as "Up".

### Check Hyperledger Fabric

```bash
docker ps | grep hyperledger
```

You should see several Fabric containers running.

## Step 5: Run the Application

```bash
go run main.go \
  --ipfs-nodes=localhost:5001 \
  --fabric-config=config.json \
  --fabric-channel=mychannel \
  --fabric-chaincode=storage \
  --kafka-brokers=localhost:9092 \
  --kafka-topic=node-status
```

## Configuration Options

The application can be configured using the following command-line flags:

- `--ipfs-nodes`: Comma-separated list of IPFS node addresses (default: "localhost:5001")
- `--fabric-config`: Path to Fabric network configuration (default: "config.json")
- `--fabric-channel`: Fabric channel name (default: "mychannel")
- `--fabric-chaincode`: Fabric chaincode ID (default: "storage")
- `--kafka-brokers`: Comma-separated list of Kafka brokers (default: "localhost:9092")
- `--kafka-topic`: Kafka topic for node status (default: "node-status")

## Troubleshooting

If you encounter issues:

1. **IPFS Connection Issues**:
   - Ensure the IPFS daemon is running: `ipfs daemon`
   - Check if the IPFS API is accessible: `curl http://localhost:5001/api/v0/id`

2. **Kafka Connection Issues**:
   - Check if Kafka is running: `docker-compose ps`
   - Verify Kafka is accessible: `nc -zv localhost 9092`

3. **Fabric Connection Issues**:
   - Ensure the Fabric network is running: `docker ps | grep hyperledger`
   - Check if the Fabric certificates are in the correct location as specified in `config.json`

4. **General Issues**:
   - Check the application logs for error messages
   - Ensure all required ports are open and not blocked by firewalls
   - Verify the configuration in `config.json` matches your Fabric network setup

## Project Structure

```
.
├── main.go                 # Application entry point
├── config.json            # Fabric network configuration
├── docker-compose.yml     # Docker Compose configuration
├── pkg/
│   ├── storage/          # Storage service implementation
│   │   ├── ipfs/        # IPFS node interface
│   │   └── service.go   # Main storage service
│   ├── blockchain/      # Blockchain integration
│   │   └── fabric/      # Hyperledger Fabric client
│   └── monitoring/      # Node monitoring
│       └── kafka/       # Kafka-based monitoring
└── go.mod               # Go module definition
```

## Next Steps

After successfully running the application, you can:

1. Upload files to the decentralized storage
2. Retrieve files using their Content Identifiers (CIDs)
3. Monitor node health through the Kafka-based monitoring system
4. View file metadata stored on the Hyperledger Fabric blockchain

For more information, refer to the main README.md file. 