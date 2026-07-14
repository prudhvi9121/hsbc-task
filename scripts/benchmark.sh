#!/bin/bash
# ==============================================================================
# Milestone 4 Performance Tuning & JVM Benchmarking Script
# ==============================================================================
# This script runs the Java application with fixed heap settings and GC logging.
# Configured Heap: 512 MB initial (Xms), 1024 MB max (Xmx)
# Configured GC Logging: log to logs/gc.log with rotation
# ==============================================================================

JAR_FILE="target/iot-telemetry-pipeline-1.0-SNAPSHOT-jar-with-dependencies.jar"
GC_LOG="logs/gc.log"

if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: Fat JAR not found. Run 'mvn package' first."
    exit 1
fi

mkdir -p logs

echo "===================================================================="
echo " Starting Telemetry Pipeline Routing Benchmark"
echo " JVM Settings: -Xms512m -Xmx1024m"
echo " GC Log File:  $GC_LOG"
echo "===================================================================="

# Running JVM with explicit memory constraints and detailed GC telemetry logging.
# We skip printing kafka clients info log to keep GC logs clean.
java -Xms512m -Xmx1024m -Xlog:gc*:file=logs/gc.log \
     -jar "$JAR_FILE" routeall

echo "===================================================================="
echo " Benchmark Complete. GC log written to: $GC_LOG"
echo "===================================================================="
