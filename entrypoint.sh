#!/bin/bash
# Entrypoint: warm up data in background, start uvicorn immediately.

set -e

echo "=== GroundHog startup $(date -u) ==="

# Pre-fetch SRTM tiles for CONUS in background (skips already-cached tiles)
if [ ! -f /app/mvp_backend/srtm_cache/.conus_done ]; then
    echo "Pre-fetching SRTM tiles for CONUS in background..."
    (python3 -m mvp_backend.prefetch_srtm && \
        touch /app/mvp_backend/srtm_cache/.conus_done || \
        echo "SRTM prefetch had errors (non-fatal)") &
fi

# Start uvicorn immediately — routes will download tiles on-demand if needed
exec uvicorn mvp_backend.server:app --host 0.0.0.0 --port 8000
