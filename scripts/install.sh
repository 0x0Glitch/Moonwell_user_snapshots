#!/bin/bash

echo "Installing dependencies for Moonwell User Balance Snapshot Tool..."

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo "Go is not installed. Please install Go first."
    exit 1
fi

# Get current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Change to project directory
cd "$PROJECT_DIR"

# Initialize Go module if not already initialized
if [ ! -f "go.mod" ]; then
    echo "Initializing Go module..."
    go mod init moonwell-snapshots
fi

# Install required dependencies
echo "Installing required Go packages..."
go get github.com/ethereum/go-ethereum@v1.12.0
go get github.com/lib/pq

echo "Downloading contract ABIs..."
# Ensure abi directory exists
mkdir -p abi

# Download Multicall3 ABI if it doesn't exist
if [ ! -f "abi/Multicall3.json" ]; then
    echo "Downloading Multicall3 ABI..."
    curl -s https://raw.githubusercontent.com/mds1/multicall/main/src/Multicall3.sol/Multicall3.json > abi/Multicall3.json
fi

echo "Installation complete!"
echo "You can now run the application with higher performance using multicall."
echo "Recommended settings:"
echo "  WORKER_CONCURRENCY=10"
echo "  BATCH_SIZE=100" 