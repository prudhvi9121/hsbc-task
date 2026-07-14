#!/bin/bash
# ==============================================================================
# entrypoint.sh — Self-contained entrypoint for the IoT Telemetry Pipeline
# ==============================================================================

set -e

# Define directories
KAFKA_HOME="/opt/kafka"
CLUSTER_ID="MkU3OEVBNTcwNTJENDM2Qk"

# Determine if we should spin up a local KRaft broker.
# If KAFKA_BOOTSTRAP_SERVERS points to localhost (default) and no external Kafka is detected, 
# we start Kafka locally within this container.
# Otherwise, we assume an external broker is provided (e.g. during docker-compose).
RUN_LOCAL_KAFKA=false
if [ -z "$KAFKA_BOOTSTRAP_SERVERS" ] || [ "$KAFKA_BOOTSTRAP_SERVERS" = "localhost:9092" ]; then
    RUN_LOCAL_KAFKA=true
    export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
fi

if [ "$RUN_LOCAL_KAFKA" = true ]; then
    echo "===================================================================="
    echo " Starting Local Kafka Broker (KRaft Mode) inside container..."
    echo "===================================================================="
    
    # 1. Format Storage Directory
    echo "Formatting Kafka log storage..."
    $KAFKA_HOME/bin/kafka-storage.sh format \
        -t "$CLUSTER_ID" \
        -c $KAFKA_HOME/config/kraft/server.properties \
        --ignore-formatted
    
    # 2. Limit Kafka Heap to 256MB to fit within 2GB container constraints
    export KAFKA_HEAP_OPTS="-Xms256m -Xmx256m"
    
    # 3. Start Kafka in the background
    echo "Launching Kafka server..."
    $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/kraft/server.properties > /dev/null 2>&1 &
    KAFKA_PID=$!
    
    # 4. Wait for Kafka to be healthy
    echo "Waiting for Kafka broker to start..."
    RETRIES=20
    while [ $RETRIES -gt 0 ]; do
        if $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
            break
        fi
        sleep 1
        RETRIES=$((RETRIES - 1))
    done
    
    if [ $RETRIES -eq 0 ]; then
        echo "Failed to start Kafka broker!"
        exit 1
    fi
    echo "Kafka broker is healthy."

    # 5. Create Kafka topics with 3 partitions
    echo "Creating Kafka topics (source, critical, nominal, regional_archive)..."
    TOPICS=("source" "critical" "nominal" "regional_archive")
    for TOPIC in "${TOPICS[@]}"; do
        $KAFKA_HOME/bin/kafka-topics.sh \
            --create \
            --topic "$TOPIC" \
            --bootstrap-server localhost:9092 \
            --partitions 3 \
            --replication-factor 1 \
            --if-not-exists
    done
    echo "Topics created successfully."
fi

# 6. Execute user command
echo "===================================================================="
echo " Starting Java Application Command: java -jar /app/app.jar $@"
echo "===================================================================="

# Bounded Java Heap (512MB initial, 1024MB max) to stay within 2GB total resource constraint
# Run application and catch exit status
EXIT_STATUS=0
java -Xms512m -Xmx1024m -jar /app/app.jar "$@" || EXIT_STATUS=$?

# 7. Clean up local Kafka broker if we started it
if [ "$RUN_LOCAL_KAFKA" = true ] && [ -n "$KAFKA_PID" ]; then
    echo "===================================================================="
    echo " Shutting down Local Kafka Broker..."
    echo "===================================================================="
    kill -15 "$KAFKA_PID"
    wait "$KAFKA_PID" 2>/dev/null || true
    echo "Kafka stopped."
fi

exit $EXIT_STATUS
