# StreamDB - Decentralized Cloud Storage

A decentralized, fault-tolerant, and self-healing cloud storage system that combines IPFS, Hyperledger Fabric, and Apache Kafka.

## Prerequisites

1. Go 1.21 or later
2. IPFS daemon
3. Hyperledger Fabric network
4. Apache Kafka
5. Docker and Docker Compose (for running dependencies)

## Setup

1. Install IPFS:
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

2. Install and start Kafka:
```bash
# Using Docker Compose
docker-compose up -d
```

3. Set up Hyperledger Fabric:
```bash
# Download Fabric samples and binaries
curl -sSL https://bit.ly/2ysbOFE | bash -s

# Start the Fabric network
cd fabric-samples/test-network
./network.sh up createChannel -c mychannel -ca
```

4. Install Go dependencies:
```bash
go mod download
```

## Running the Application

1. Start the IPFS daemon (if not already running):
```bash
ipfs daemon
```

2. Start Kafka (if using Docker Compose):
```bash
docker-compose up -d
```

3. Run the application:
```bash
go run main.go \
  --ipfs-nodes=localhost:5001 \
  --fabric-config=config.json \
  --fabric-channel=mychannel \
  --fabric-chaincode=storage \
  --kafka-brokers=localhost:9092 \
  --kafka-topic=node-status
```

## Configuration

The application can be configured using command-line flags:

- `--ipfs-nodes`: Comma-separated list of IPFS node addresses (default: "localhost:5001")
- `--fabric-config`: Path to Fabric network configuration (default: "config.json")
- `--fabric-channel`: Fabric channel name (default: "mychannel")
- `--fabric-chaincode`: Fabric chaincode ID (default: "storage")
- `--kafka-brokers`: Comma-separated list of Kafka brokers (default: "localhost:9092")
- `--kafka-topic`: Kafka topic for node status (default: "node-status")

## Project Structure

```
.
├── main.go                 # Application entry point
├── config.json            # Fabric network configuration
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

## License

MIT License
