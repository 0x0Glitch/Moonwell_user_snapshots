#!/bin/bash

# Generate Go bindings for MToken ABI
abigen --abi ../abi/MToken.json --pkg main --type MToken --out ../cmd/mtoken_bindings.go

echo "Bindings generated successfully"
