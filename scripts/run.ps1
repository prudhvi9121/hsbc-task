# ==============================================================================
# run.ps1 - Spin up Kafka, auto-create topics, produce 50M records, and route them.
# Usage: .\scripts\run.ps1
# ==============================================================================

$ErrorActionPreference = "Stop"

Write-Host "====================================================================" -ForegroundColor Green
Write-Host " Starting IoT Telemetry Pipeline Services (PowerShell)" -ForegroundColor Green
Write-Host "====================================================================" -ForegroundColor Green

# 1. Start Kafka and wait for topics to be initialized
Write-Host "Step 1: Launching Kafka (KRaft mode) & topic-init containers..." -ForegroundColor Cyan
docker compose up -d kafka kafka-init

Write-Host "Waiting for Kafka broker to be healthy and topics to be initialized..." -ForegroundColor Yellow
docker compose wait kafka-init

# 2. Produce 50 million telemetry records into the 'source' topic
Write-Host "" -ForegroundColor Cyan
Write-Host "Step 2: Generating and producing 50,000,000 telemetry records to 'source' topic..." -ForegroundColor Cyan
docker compose run --name telemetry-generator-run --rm telemetry-app fullrun

# 3. Start the telemetry pipeline application in the background to consume and route all records
Write-Host "" -ForegroundColor Cyan
Write-Host "Step 3: Launching telemetry-app routing container to process all records..." -ForegroundColor Cyan
docker compose up -d telemetry-app

Write-Host "Waiting for telemetry-app to finish routing all records..." -ForegroundColor Yellow
docker compose wait telemetry-app

Write-Host "====================================================================" -ForegroundColor Green
Write-Host " Complete Pipeline Executed Successfully!" -ForegroundColor Green
Write-Host " All records generated, produced, consumed, and routed." -ForegroundColor Green
Write-Host " Use '.\scripts\verify.ps1' to inspect database counts." -ForegroundColor Green
Write-Host "====================================================================" -ForegroundColor Green
