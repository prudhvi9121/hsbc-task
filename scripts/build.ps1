# ==============================================================================
# build.ps1 - Build the IoT Telemetry Pipeline Docker Image (PowerShell)
#
# Usage:
#   .\scripts\build.ps1
#
# Requires only Docker. Maven and Java are not required locally as the code
# is compiled inside the container's build environment.
# ==============================================================================

$ErrorActionPreference = "Stop"

Write-Host "====================================================================" -ForegroundColor Green
Write-Host " Starting IoT Telemetry Pipeline Build Process (PowerShell)"        -ForegroundColor Green
Write-Host "====================================================================" -ForegroundColor Green

# --- Step 1: Verify Docker is available ---------------------------------------
Write-Host ""
Write-Host "Checking Docker availability..." -ForegroundColor Cyan
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host ""
    Write-Host "ERROR: 'docker' was not found on PATH." -ForegroundColor Red
    Write-Host "Please install Docker Desktop from: https://www.docker.com/products/docker-desktop" -ForegroundColor Yellow
    exit 1
}
Write-Host "  docker found: $(docker --version)" -ForegroundColor Green

# --- Step 2: Build Docker image -----------------------------------------------
Write-Host ""
Write-Host "Building Docker image 'telemetry-pipeline' from source..." -ForegroundColor Cyan
Write-Host "This compiles the Java application and downloads Kafka internally inside the container." -ForegroundColor Gray
docker build -t telemetry-pipeline .

Write-Host ""
Write-Host "====================================================================" -ForegroundColor Green
Write-Host " Build Success! Image 'telemetry-pipeline' is ready."                -ForegroundColor Green
Write-Host ""
Write-Host " Run the full pipeline with:"                                         -ForegroundColor Cyan
Write-Host "   .\scripts\run.ps1"                                                 -ForegroundColor White
Write-Host "   -- or (single container stand-alone mode) --"                      -ForegroundColor White
Write-Host "   docker run --memory=2g --cpus=4 --rm telemetry-pipeline all"      -ForegroundColor White
Write-Host "====================================================================" -ForegroundColor Green
