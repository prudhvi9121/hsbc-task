#!/bin/bash
# ==============================================================================
# run.sh — Spin up Kafka, auto-create topics, produce 50M records, and route them.
# Usage: ./scripts/run.sh
# ==============================================================================

set -e

echo "===================================================================="
echo " Starting IoT Telemetry Pipeline Services"
echo "===================================================================="

# 1. Start Kafka and wait for topics to be initialized
echo "Step 1: Launching Kafka (KRaft mode) & topic-init containers..."
docker compose up -d kafka kafka-init

echo "Waiting for Kafka broker to be healthy and topics to be initialized..."
docker compose wait kafka-init

# 2. Produce 50 million telemetry records into the 'source' topic
echo ""
echo "Step 2: Generating and producing 50,000,000 telemetry records to 'source' topic..."
docker compose run --name telemetry-generator-run --rm telemetry-app fullrun

# 3. Start the telemetry pipeline application in the background to consume and route all records
echo ""
echo "Step 3: Launching telemetry-app routing container to process all records..."
docker compose up -d telemetry-app

echo "Waiting for telemetry-app to finish routing all records..."
docker compose wait telemetry-app

echo "===================================================================="
echo " Complete Pipeline Executed Successfully!"
echo " All records generated, produced, consumed, and routed."
echo " Use './scripts/verify.sh' to inspect database counts."
echo "===================================================================="
