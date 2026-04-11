#!/bin/bash
# Refresh FAA data and rebuild GroundHog container.
# Install in cron:  0 8 1,15 * * /root/.openclaw/workspace-volo_coding/GroundHog/refresh_and_rebuild.sh >> /var/log/groundhog-refresh.log 2>&1

set -e
cd /root/.openclaw/workspace-volo_coding/GroundHog

echo "=== GroundHog FAA refresh $(date -u) ==="

# Pull latest code
git pull origin main 2>&1

# Run FAA data refresh inside current container (updates .csv/.sqlite in volumes)
docker exec groundhog python3 -m mvp_backend.refresh_faa 2>&1

# Rebuild and restart with fresh code + data
docker compose down
docker compose up -d --build 2>&1

echo "=== Done $(date -u) ==="
