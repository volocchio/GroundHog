#!/usr/bin/env bash
# deploy.sh — Pull latest code and rebuild GroundHog Docker container on the VPS
# Run as root or with sudo.
set -euo pipefail

REPO_DIR="/opt/groundhog"
BRANCH="main"

echo "=== GroundHog deploy $(date) ==="

# 1. Pull latest code
cd "$REPO_DIR"
git fetch origin
git reset --hard "origin/$BRANCH"

# 2. Rebuild and restart container
docker compose up --build -d

# 3. Clean up old images
docker image prune -f

echo "=== Deploy complete ==="
docker compose ps
