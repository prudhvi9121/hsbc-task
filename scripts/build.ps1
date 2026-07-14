# ==============================================================================
# build.ps1 — Build the IoT Telemetry Pipeline (PowerShell)
#
# Usage:
#   .\scripts\build.ps1              # Local Maven build + Docker image
#   .\scripts\build.ps1 -DockerOnly  # Skip local Maven; Docker builds the JAR
#
# The -DockerOnly flag is useful on machines that do not have Maven or Java
# installed locally. The Dockerfile installs Maven and Kafka internally and
# compiles the source code inside the build layer.
# ==============================================================================

param(
    [switch]$DockerOnly
)

$ErrorActionPreference = "Stop"

Write-Host "====================================================================" -ForegroundColor Green
Write-Host " Starting IoT Telemetry Pipeline Build Process (PowerShell)"        -ForegroundColor Green
Write-Host "====================================================================" -ForegroundColor Green

# ── Step 1: Verify Docker is available ───────────────────────────────────────
Write-Host ""
Write-Host "Checking Docker availability..." -ForegroundColor Cyan
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host ""
    Write-Host "ERROR: 'docker' was not found on PATH." -ForegroundColor Red
    Write-Host "Please install Docker Desktop from: https://www.docker.com/products/docker-desktop" -ForegroundColor Yellow
    exit 1
}
Write-Host "  docker found: $(docker --version)" -ForegroundColor Green

# ── Step 2: Optional local Maven build ───────────────────────────────────────
if ($DockerOnly) {
    Write-Host ""
    Write-Host "Step 1: Skipping local Maven build (-DockerOnly flag set)." -ForegroundColor Yellow
    Write-Host "        The Dockerfile will compile the source code internally." -ForegroundColor Yellow
} else {
    Write-Host ""
    Write-Host "Step 1: Cleaning and packaging Java fat JAR with Maven..." -ForegroundColor Cyan

    # Check if Maven is installed
    if (-not (Get-Command mvn -ErrorAction SilentlyContinue)) {
        Write-Host ""
        Write-Host "WARNING: 'mvn' (Maven) was not found on PATH." -ForegroundColor Yellow
        Write-Host ""
        Write-Host "  Option A — Install Maven:" -ForegroundColor Cyan
        Write-Host "    https://maven.apache.org/install.html" -ForegroundColor White
        Write-Host "    Or via winget: winget install Apache.Maven" -ForegroundColor White
        Write-Host ""
        Write-Host "  Option B — Skip local Maven (Docker builds the JAR for you):" -ForegroundColor Cyan
        Write-Host "    .\scripts\build.ps1 -DockerOnly" -ForegroundColor White
        Write-Host ""
        exit 1
    }

    mvn clean package -DskipTests
    Write-Host "  Maven build complete." -ForegroundColor Green
}

# ── Step 3: Build Docker image ────────────────────────────────────────────────
Write-Host ""
Write-Host "Step 2: Building Docker image 'telemetry-pipeline'..." -ForegroundColor Cyan
docker build -t telemetry-pipeline .

Write-Host ""
Write-Host "====================================================================" -ForegroundColor Green
Write-Host " Build Success! Image 'telemetry-pipeline' is ready."                -ForegroundColor Green
Write-Host ""
Write-Host " Run the full pipeline with:"                                         -ForegroundColor Cyan
Write-Host "   .\scripts\run.ps1"                                                 -ForegroundColor White
Write-Host "   -- or (single container) --"                                       -ForegroundColor White
Write-Host "   docker run --memory=2g --cpus=4 --rm telemetry-pipeline all"      -ForegroundColor White
Write-Host "====================================================================" -ForegroundColor Green
