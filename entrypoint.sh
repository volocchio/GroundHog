#!/bin/bash
# Entrypoint: warm up data in background, start uvicorn immediately.

set -e

# Forward SIGTERM/SIGINT to child processes for clean shutdown
trap 'kill -TERM $(jobs -p) 2>/dev/null; wait' TERM INT

echo "=== GroundHog startup $(date -u) ==="
echo "  SRTM tiles: $(ls /app/mvp_backend/srtm_cache/*.hgt 2>/dev/null | wc -l) cached"
echo "  Airports:   $(wc -l < /app/mvp_backend/airports_solver.csv 2>/dev/null || echo 0) rows"
echo "  Obstacles:  $(python3 -c "import sqlite3,os; p='/app/mvp_backend/obstacle_data/obstacles.sqlite'; print(sqlite3.connect(p).execute('SELECT COUNT(*) FROM obstacles').fetchone()[0]) if os.path.exists(p) else print(0)" 2>/dev/null)"
echo "  Airspace:   $(python3 -c "import sqlite3,os; p='/app/mvp_backend/airspace_data/airspace.sqlite'; print(sqlite3.connect(p).execute('SELECT COUNT(*) FROM airspace').fetchone()[0]) if os.path.exists(p) else print(0)" 2>/dev/null)"

# Pre-fetch SRTM tiles for CONUS in background (skips already-cached tiles)
if [ ! -f /app/mvp_backend/srtm_cache/.conus_done ]; then
    echo "Pre-fetching SRTM tiles for CONUS in background..."
    (python3 -m mvp_backend.prefetch_srtm && \
        touch /app/mvp_backend/srtm_cache/.conus_done || \
        echo "SRTM prefetch had errors (non-fatal)") &
else
    echo "SRTM CONUS tiles already cached."
fi

# Start uvicorn — routes will download tiles on-demand if background hasn't finished
exec uvicorn mvp_backend.server:app --host 0.0.0.0 --port 8000 --workers 2
