#!/bin/bash

# Check for API key
if [ -z "$ANTHROPIC_API_KEY" ]; then
    echo "⚠️  ANTHROPIC_API_KEY not set!"
    echo "Please set it with: export ANTHROPIC_API_KEY='your-key-here'"
    exit 1
fi

# Build the image if it doesn't exist
if ! docker images | grep -q spark-3.5.6-java8-sandbox; then
    echo "Building Docker image..."
    docker build -t spark-3.5.6-java8-sandbox .
fi

# Start or attach to container
if docker ps -a | grep -q spark-dev-sandbox; then
    echo "Starting existing container..."
    docker start -i spark-dev-sandbox
else
    echo "Creating new container with unrestricted Claude access..."
    docker run -it \
      -v /Users/igor.b/workspace/spark:/workspace/spark \
      -v /tmp:/tmp \
      -e ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
      --name spark-dev-sandbox \
      spark-3.5.6-java8-sandbox
fi