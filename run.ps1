# GroundHog — one-command setup & run (Windows PowerShell)
# Usage: .\run.ps1

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# 1. Create venv if missing
if (-not (Test-Path ".venv\Scripts\activate.ps1")) {
    Write-Host "Creating virtual environment ..." -ForegroundColor Cyan
    python -m venv .venv
}

# 2. Activate venv
. .venv\Scripts\activate.ps1

# 3. Install dependencies
Write-Host "Installing dependencies ..." -ForegroundColor Cyan
pip install -q -r requirements.txt

# 4. Download NASR data if APT.txt is missing
if (-not (Test-Path "faa_nasr\extract\APT.txt")) {
    Write-Host "Downloading FAA NASR data ..." -ForegroundColor Cyan
    python scripts/fetch_nasr.py
}

# 5. Build airports solver if CSV is missing
if (-not (Test-Path "mvp_backend\airports_solver.csv")) {
    Write-Host "Building airports solver ..." -ForegroundColor Cyan
    python -m mvp_backend.build_airports_solver
}

# 6. Kill stale server on target port if still running
$port = 8000
$existing = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue |
    Where-Object State -eq "Listen" |
    Select-Object -First 1
if ($existing) {
    Write-Host "Stopping previous server (PID $($existing.OwningProcess)) on port $port ..." -ForegroundColor Yellow
    Stop-Process -Id $existing.OwningProcess -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
}

# 7. Start server
Write-Host ""
Write-Host "Starting GroundHog at http://localhost:$port" -ForegroundColor Green
uvicorn mvp_backend.server:app --host 0.0.0.0 --port $port
