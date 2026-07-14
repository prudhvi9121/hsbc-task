#!/bin/bash
# create-topics.sh — Create all required Kafka topics
# Usage: ./scripts/create-topics.sh
# Run AFTER Kafka is healthy (docker compose up -d)

set -e

BOOTSTRAP="localhost:9092"
KAFKA_BIN="/opt/kafka/bin"
TOPICS=("source" "critical" "nominal" "regional_archive")

echo "=== Creating Kafka Topics ==="
echo "Bootstrap: $BOOTSTRAP"
echo ""

for TOPIC in "${TOPICS[@]}"; do
    echo "Creating topic: $TOPIC"
    $KAFKA_BIN/kafka-topics.sh \
        --create \
        --topic "$TOPIC" \
        --bootstrap-server "$BOOTSTRAP" \
        --if-not-exists \
        --partitions 3 \
        --replication-factor 1
    echo "  ✓ $TOPIC created"
done

echo ""
echo "=== Verifying Topics ==="
$KAFKA_BIN/kafka-topics.sh --list --bootstrap-server "$BOOTSTRAP"
echo ""
echo "=== Done ==="
