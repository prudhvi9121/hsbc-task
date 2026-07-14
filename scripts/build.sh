#!/bin/bash
# ==============================================================================
# build.sh - Build the IoT Telemetry Pipeline Docker Image
#
# Usage:
#   ./scripts/build.sh
#
# Requires only Docker. Maven and Java are not required locally as the code
# is compiled inside the container's build environment.
# ==============================================================================

set -e

echo "===================================================================="
echo " Starting IoT Telemetry Pipeline Build Process"
echo "===================================================================="

# --- Step 1: Verify Docker is available ---------------------------------------
echo ""
echo "Checking Docker availability..."
if ! command -v docker &>/dev/null; then
    echo ""
    echo "ERROR: 'docker' was not found on PATH."
    echo "Please install Docker from: https://docs.docker.com/get-docker/"
    exit 1
fi
echo "  docker found: $(docker --version)"

# --- Step 2: Build Docker image -----------------------------------------------
echo ""
echo "Building Docker image 'telemetry-pipeline' from source..."
echo "This compiles the Java application and downloads Kafka internally inside the container."
docker build -t telemetry-pipeline .

echo ""
echo "===================================================================="
echo " Build Success! Image 'telemetry-pipeline' is ready."
echo ""
echo " Run the full pipeline with:"
echo "   ./scripts/run.sh"
echo "   -- or (single container stand-alone mode) --"
echo "   docker run --memory=2g --cpus=4 --rm telemetry-pipeline all"
echo "===================================================================="
