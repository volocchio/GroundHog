#!/usr/bin/env bash
# deploy.sh — Pull latest code and restart GroundHog on the VPS
# Run as root or with sudo.  Adjust REPO_DIR / USER as needed.
set -euo pipefail

REPO_DIR="/opt/groundhog"
SERVICE="groundhog"
BRANCH="main"           # change to "master" if that's your default branch
VENV="$REPO_DIR/.venv"

echo "=== GroundHog deploy $(date) ==="

# 1. Pull latest code
cd "$REPO_DIR"
git fetch origin
git reset --hard "origin/$BRANCH"

# 2. Create venv if missing
if [ ! -d "$VENV" ]; then
    python3 -m venv "$VENV"
fi

# 3. Install / update dependencies
"$VENV/bin/pip" install -q -r requirements.txt

# 4. Download FAA NASR data if missing
if [ ! -f "faa_nasr/extract/APT.txt" ]; then
    echo "Downloading FAA NASR data ..."
    "$VENV/bin/python" scripts/fetch_nasr.py
fi

# 5. Rebuild airports solver if missing
if [ ! -f "mvp_backend/airports_solver.csv" ]; then
    echo "Building airports solver ..."
    "$VENV/bin/python" -m mvp_backend.build_airports_solver
fi

# 6. Restart the systemd service
systemctl restart "$SERVICE"
echo "=== Deploy complete — $SERVICE restarted ==="
