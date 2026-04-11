#!/bin/bash
# Refresh FAA data and rebuild GroundHog container.
# Install in cron:  0 8 1,15 * * /root/.openclaw/workspace-volo_coding/GroundHog/refresh_and_rebuild.sh >> /var/log/groundhog-refresh.log 2>&1

set -e

LOCKFILE="/tmp/groundhog-refresh.lock"
LOGFILE="/var/log/groundhog-refresh.log"

# Prevent concurrent runs
exec 200>"$LOCKFILE"
if ! flock -n 200; then
    echo "$(date -u) SKIP: another refresh is already running" >> "$LOGFILE"
    exit 0
fi

cd /root/.openclaw/workspace-volo_coding/GroundHog

echo "=== GroundHog FAA refresh $(date -u) ==="

# Pull latest code
git pull origin main 2>&1 || echo "WARN: git pull failed (non-fatal)"

# Run FAA data refresh inside container — data lands in Docker volumes
if docker exec groundhog python3 -m mvp_backend.refresh_faa 2>&1; then
    echo "FAA data refresh succeeded."
else
    echo "WARN: FAA data refresh had errors — continuing with rebuild."
fi

# Rebuild and restart with fresh code + data
docker compose down 2>&1 || true
docker compose up -d --build 2>&1

# Verify health
for i in 1 2 3 4 5; do
    sleep 5
    if curl -sf http://127.0.0.1:8504/ > /dev/null 2>&1; then
        echo "Health check passed after rebuild."
        break
    fi
    echo "Waiting for GroundHog to start... ($i/5)"
done

echo "=== Done $(date -u) ==="
