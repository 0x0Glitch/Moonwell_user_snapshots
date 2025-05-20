#!/bin/bash

# Moonwell Database Check Script

# Set the PG_DSN environment variable with the connection string
export PG_DSN="postgresql://postgres:hC2dDg4ikpabSKoJ@db.jthueqjhtuohvjewliup.supabase.co:5432/postgres"

echo "=== Moonwell Database Check Tool ==="
echo
echo "This script will:"
echo "1. Check if the moonwell_user_balances table exists"
echo "2. Verify that each address appears only once"
echo "3. Show sample data from the table"
echo
echo "Connection: $PG_DSN"
echo

# Run the check utility
cd "$(dirname "$0")"
go run cmd/db_check/main.go

# Check exit code
if [ $? -ne 0 ]; then
    echo "Error: Database check failed"
    exit 1
fi

echo
echo "Database check completed successfully!" 