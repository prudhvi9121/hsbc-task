#!/bin/bash
# ==============================================================================
# build.sh — Build the IoT Telemetry Pipeline
#
# Usage:
#   ./scripts/build.sh               # Local Maven build + Docker image
#   ./scripts/build.sh --docker-only # Skip local Maven; Docker builds the JAR
#
# The --docker-only flag is useful on machines that do not have Maven or Java
# installed locally. The Dockerfile installs Maven and Kafka internally and
# compiles the source code inside the build layer.
# ==============================================================================

set -e

DOCKER_ONLY=false
if [ "${1}" = "--docker-only" ]; then
    DOCKER_ONLY=true
fi

echo "===================================================================="
echo " Starting IoT Telemetry Pipeline Build Process"
echo "===================================================================="

# ── Step 1: Verify Docker is available ───────────────────────────────────────
echo ""
echo "Checking Docker availability..."
if ! command -v docker &>/dev/null; then
    echo ""
    echo "ERROR: 'docker' was not found on PATH."
    echo "Please install Docker from: https://docs.docker.com/get-docker/"
    exit 1
fi
echo "  docker found: $(docker --version)"

# ── Step 2: Optional local Maven build ───────────────────────────────────────
if [ "$DOCKER_ONLY" = true ]; then
    echo ""
    echo "Step 1: Skipping local Maven build (--docker-only flag set)."
    echo "        The Dockerfile will compile the source code internally."
else
    echo ""
    echo "Step 1: Cleaning and packaging Java fat JAR with Maven..."

    if ! command -v mvn &>/dev/null; then
        echo ""
        echo "WARNING: 'mvn' (Maven) was not found on PATH."
        echo ""
        echo "  Option A — Install Maven:"
        echo "    https://maven.apache.org/install.html"
        echo "    macOS (brew): brew install maven"
        echo "    Ubuntu/Debian: sudo apt-get install maven"
        echo ""
        echo "  Option B — Skip local Maven (Docker builds the JAR for you):"
        echo "    ./scripts/build.sh --docker-only"
        echo ""
        exit 1
    fi

    mvn clean package -DskipTests
    echo "  Maven build complete."
fi

# ── Step 3: Build Docker image ────────────────────────────────────────────────
echo ""
echo "Step 2: Building Docker image 'telemetry-pipeline'..."
docker build -t telemetry-pipeline .

echo ""
echo "===================================================================="
echo " Build Success! Image 'telemetry-pipeline' is ready."
echo ""
echo " Run the full pipeline with:"
echo "   ./scripts/run.sh"
echo "   -- or (single container) --"
echo "   docker run --memory=2g --cpus=4 --rm telemetry-pipeline all"
echo "===================================================================="
