#!/bin/bash
# ==============================================================================
# build.sh — Clean, compile, package fat JAR, and build Docker image
# Usage: ./scripts/build.sh
# ==============================================================================

set -e

echo "===================================================================="
echo " Starting IoT Telemetry Pipeline Build Process"
echo "===================================================================="

# 1. Clean and package the JAR with all dependencies
echo "Step 1: Cleaning and packaging Java fat JAR with dependencies..."
mvn clean package -DskipTests

# 2. Build the Docker image
echo ""
echo "Step 2: Building Docker image 'telemetry-pipeline'..."
docker build -t telemetry-pipeline .

echo "===================================================================="
echo " Build Success!"
echo " Image 'telemetry-pipeline' is ready."
echo "===================================================================="
