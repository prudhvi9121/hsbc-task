# ==============================================================================
# build.ps1 — Clean, compile, package fat JAR, and build Docker image (PowerShell)
# Usage: .\scripts\build.ps1
# ==============================================================================

$ErrorActionPreference = "Stop"

Write-Host "====================================================================" -ForegroundColor Green
Write-Host " Starting IoT Telemetry Pipeline Build Process (PowerShell)" -ForegroundColor Green
Write-Host "====================================================================" -ForegroundColor Green

# 1. Clean and package the JAR with all dependencies
Write-Host "Step 1: Cleaning and packaging Java fat JAR with dependencies..." -ForegroundColor Cyan
mvn clean package -DskipTests

# 2. Build the Docker image
Write-Host ""
Write-Host "Step 2: Building Docker image 'telemetry-pipeline'..." -ForegroundColor Cyan
docker build -t telemetry-pipeline .

Write-Host "====================================================================" -ForegroundColor Green
Write-Host " Build Success!" -ForegroundColor Green
Write-Host " Image 'telemetry-pipeline' is ready." -ForegroundColor Green
Write-Host "====================================================================" -ForegroundColor Green
