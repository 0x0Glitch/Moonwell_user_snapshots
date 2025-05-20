#!/bin/bash

# Moonwell Database Reset Script

# Set the PG_DSN environment variable with the connection string
export PG_DSN=${PG_DSN:-"postgresql://username:password@hostname:port/database"}

echo "=== Moonwell Database Reset Tool ==="
echo
echo "This script will:"
echo "1. Create or recreate the moonwell_user_balances table"
echo "2. Clear existing data"
echo
echo "Connection: $PG_DSN"
echo

# First step: Set up the database structure
echo "Step 1: Setting up database structure..."
cd "$(dirname "$0")"
go run cmd/db_setup/main.go

if [ $? -ne 0 ]; then
    echo "Error: Database setup failed"
    exit 1
fi

echo
echo "Step 2: Clearing existing data and adding users..."
go run cmd/clear_and_reset/main.go

if [ $? -ne 0 ]; then
    echo "Error: Database reset failed"
    exit 1
fi
