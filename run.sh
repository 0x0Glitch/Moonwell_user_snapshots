#!/bin/bash

# Moonwell User Balance Snapshot Tool Runner

# Flag to temporarily skip database operations for testing
export SKIP_DATABASE="false"

# Default configuration - adjust as needed
WORKER_CONCURRENCY=${WORKER_CONCURRENCY:-10}
BATCH_SIZE=${BATCH_SIZE:-100}
FETCH_INTERVAL_HOURS=${FETCH_INTERVAL_HOURS:-2}
USE_MULTICALL=${USE_MULTICALL:-true}

# Set default environment variables if not already set
export RPC_URL=${RPC_URL:-"https://your-ethereum-rpc-url"}
export PG_DSN=${PG_DSN:-"postgresql://username:password@hostname:port/database"}

# Check if RPC_URL is set
if [ -z "$RPC_URL" ]; then
    echo "Warning: RPC_URL is not set. Using the default value."
    echo "For better performance, set your own RPC endpoint:"
    echo "  export RPC_URL=https://your-rpc-endpoint"
fi

# Check if PG_DSN is set
if [ -z "$PG_DSN" ]; then
    echo "Warning: PG_DSN is not set. Using the default value."
    echo "For production use, set your PostgreSQL connection string:"
    echo "  export PG_DSN=postgresql://username:password@host:port/database"
fi

# Print configuration
echo "====== Moonwell User Balance Snapshot Tool ======"
echo "WORKER_CONCURRENCY: $WORKER_CONCURRENCY"
echo "BATCH_SIZE: $BATCH_SIZE"
echo "FETCH_INTERVAL_HOURS: $FETCH_INTERVAL_HOURS"
echo "USE_MULTICALL: $USE_MULTICALL"
echo "SKIP_DATABASE: ${SKIP_DATABASE:-false}"
echo "Data Format: [mToken, underlying]"
echo "-------------------------------------------"

# Export environment variables
export WORKER_CONCURRENCY
export BATCH_SIZE
export FETCH_INTERVAL_HOURS
export USE_MULTICALL

# Run the program
cd "$(dirname "$0")"
go run cmd/main/main.go cmd/main/mtoken_bindings.go 