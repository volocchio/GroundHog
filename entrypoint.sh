#!/bin/bash
# Entrypoint: warm up data on first run, then start uvicorn.

set -e

echo "=== GroundHog startup $(date -u) ==="

# Pre-fetch SRTM tiles for CONUS (skips already-cached tiles)
if [ ! -f /app/mvp_backend/srtm_cache/.conus_done ]; then
    echo "Pre-fetching SRTM tiles for CONUS (first run only)..."
    python3 -m mvp_backend.prefetch_srtm && \
        touch /app/mvp_backend/srtm_cache/.conus_done || \
        echo "SRTM prefetch had errors (non-fatal, will download on demand)"
fi

# Start uvicorn
exec uvicorn mvp_backend.server:app --host 0.0.0.0 --port 8000
