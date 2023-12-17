#!/bin/bash

# Get the directory where the script resides
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Define the full path to the api.proto file
PROTO_FILE="$SCRIPT_DIR/api.proto"

# Run the protoc command with the proto_path and output directories
protoc --proto_path="$SCRIPT_DIR" \
  --go_out="$SCRIPT_DIR" --go_opt=paths=source_relative \
  --go-grpc_out="$SCRIPT_DIR" --go-grpc_opt=paths=source_relative \
  "$(basename "$PROTO_FILE")"

# protoc --go_out=. --go_opt=paths=source_relative \
#  --go-grpc_out=. --go-grpc_opt=paths=source_relative api.proto

