#!/bin/bash
# ==============================================================================
# benchmark.sh — Producer & routing throughput benchmark with GC telemetry
# ==============================================================================
# Runs the full routing pipeline with a bounded JVM heap and GC logging enabled.
# Use this to measure end-to-end routing throughput and inspect garbage collection
# behaviour under the 2 GB container memory constraint.
#
# Heap:    -Xms512m (initial) / -Xmx1024m (max)
# GC log:  logs/gc.log (rotated, human-readable format)
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
