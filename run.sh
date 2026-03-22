#!/usr/bin/env bash
# GroundHog — one-command setup & run (Linux / macOS)
# Usage: ./run.sh
set -e
cd "$(dirname "$0")"

# 1. Create venv if missing
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment ..."
    python3 -m venv .venv
fi

# 2. Activate venv
source .venv/bin/activate

# 3. Install dependencies
echo "Installing dependencies ..."
pip install -q -r requirements.txt

# 4. Download NASR data if APT.txt is missing
if [ ! -f "faa_nasr/extract/APT.txt" ]; then
    echo "Downloading FAA NASR data ..."
    python scripts/fetch_nasr.py
fi

# 5. Build airports solver if CSV is missing
if [ ! -f "mvp_backend/airports_solver.csv" ]; then
    echo "Building airports solver ..."
    python -m mvp_backend.build_airports_solver
fi

# 6. Start server
echo ""
echo "Starting GroundHog at http://localhost:8000"
uvicorn mvp_backend.server:app --host 0.0.0.0 --port 8000
