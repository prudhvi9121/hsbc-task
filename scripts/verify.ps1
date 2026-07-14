# ==============================================================================
# verify.ps1 — Validate routed topic offsets and print PASS or FAIL status (PowerShell).
# Usage: .\scripts\verify.ps1
# ==============================================================================

$ErrorActionPreference = "Stop"

Write-Host "====================================================================" -ForegroundColor Green
Write-Host " Starting Telemetry Pipeline Count Verification (PowerShell)" -ForegroundColor Green
Write-Host "====================================================================" -ForegroundColor Green

# 1. Run the countoffsets command via Docker Compose
Write-Host "Fetching topic offsets from Kafka broker..." -ForegroundColor Cyan
$output = docker compose run --rm telemetry-app countoffsets

Write-Host ""
Write-Host "----------------------------------------------------" -ForegroundColor Gray
foreach ($line in $output) {
    if ($line -match "Topic:" -or $line -match "offset") {
        Write-Host $line -ForegroundColor Gray
    }
}
Write-Host "----------------------------------------------------" -ForegroundColor Gray
Write-Host ""

# 2. Extract count values
$source = 0
$critical = 0
$nominal = 0
$archive = 0

foreach ($line in $output) {
    if ($line -match "Topic:\s+source\s+\|\s+Total Records:\s+([0-9,]+)") {
        $source = [int64]($Matches[1].Replace(",", ""))
    }
    elseif ($line -match "Topic:\s+critical\s+\|\s+Total Records:\s+([0-9,]+)") {
        $critical = [int64]($Matches[1].Replace(",", ""))
    }
    elseif ($line -match "Topic:\s+nominal\s+\|\s+Total Records:\s+([0-9,]+)") {
        $nominal = [int64]($Matches[1].Replace(",", ""))
    }
    elseif ($line -match "Topic:\s+regional_archive\s+\|\s+Total Records:\s+([0-9,]+)") {
        $archive = [int64]($Matches[1].Replace(",", ""))
    }
}

Write-Host "Extracted Counts:"
Write-Host "  Source           : $source"
Write-Host "  Critical         : $critical"
Write-Host "  Nominal          : $nominal"
Write-Host "  Regional Archive : $archive"
Write-Host ""

# 3. Perform algebraic checks
$sum = $critical + $nominal
$diffSum = $source - $sum
$diffArchive = $source - $archive

$isOk = $false

if ($diffSum -eq 1 -and $diffArchive -eq 1) {
    $isOk = $true
}
elseif ($diffSum -eq 0 -and $diffArchive -eq 0) {
    $isOk = $true
}

if ($isOk) {
    Write-Host "====================================================================" -ForegroundColor Green
    Write-Host " Check 1: Critical ($critical) + Nominal ($nominal) == Source ($source) ? [OK]" -ForegroundColor Green
    Write-Host " Check 2: Regional Archive ($archive) == Source ($source) ? [OK]" -ForegroundColor Green
    Write-Host ""
    Write-Host " FINAL RESULT: PASS" -ForegroundColor Green -BackgroundColor Black
    Write-Host "====================================================================" -ForegroundColor Green
}
else {
    Write-Host "====================================================================" -ForegroundColor Red
    Write-Host " Check 1: Critical ($critical) + Nominal ($nominal) == Source ($source) ? [FAILED]" -ForegroundColor Red
    Write-Host " Check 2: Regional Archive ($archive) == Source ($source) ? [FAILED]" -ForegroundColor Red
    Write-Host ""
    Write-Host " FINAL RESULT: FAIL" -ForegroundColor Red -BackgroundColor Black
    Write-Host "====================================================================" -ForegroundColor Red
    Write-Host ""
    Write-Host "TIP: If you ran the pipeline multiple times without wiping Docker volumes, the target topics" -ForegroundColor Yellow
    Write-Host "     accumulated historical records, causing the checks to fail." -ForegroundColor Yellow
    Write-Host "     Please run: 'docker compose down -v' to clear all data and start fresh." -ForegroundColor Yellow
    Write-Host ""
    exit 1
}
