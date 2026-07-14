# ==============================================================================
# benchmark.ps1 — Producer & routing throughput benchmark with GC telemetry
# ==============================================================================
# Runs the full routing pipeline with a bounded JVM heap and GC logging enabled.
# Use this to measure end-to-end routing throughput and inspect garbage collection
# behaviour under the 2 GB container memory constraint.
#
# Heap:    -Xms512m (initial) / -Xmx1024m (max)
# GC log:  logs/gc.log (rotated, human-readable format)
# ==============================================================================

$JarFile = "target\iot-telemetry-pipeline-1.0-SNAPSHOT-jar-with-dependencies.jar"
$GcLog = "logs\gc.log"

if (-not (Test-Path $JarFile)) {
    Write-Error "Fat JAR not found. Run 'mvn package' first."
    exit 1
}

if (-not (Test-Path logs)) {
    New-Item -ItemType Directory -Path logs | Out-Null
}

Write-Host "====================================================================" -ForegroundColor Green
Write-Host " Starting Telemetry Pipeline Routing Benchmark (PowerShell)" -ForegroundColor Green
Write-Host " JVM Settings: -Xms512m -Xmx1024m"
Write-Host " GC Log File:  $GcLog"
Write-Host "====================================================================" -ForegroundColor Green

# Start JVM routing run
java -Xms512m -Xmx1024m -Xlog:gc*:file=logs/gc.log -jar $JarFile routeall

Write-Host "====================================================================" -ForegroundColor Green
Write-Host " Benchmark Complete. GC log written to: $GcLog"
Write-Host "====================================================================" -ForegroundColor Green
