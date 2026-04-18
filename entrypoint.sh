#!/bin/bash
# Entrypoint: warm up data in background, start uvicorn immediately.

set -e

# Forward SIGTERM/SIGINT to child processes for clean shutdown
trap 'kill -TERM $(jobs -p) 2>/dev/null; wait' TERM INT

echo "=== GroundHog startup $(date -u) ==="

mkdir -p /app/mvp_backend/obstacle_data

_obs_count() {
  python3 -c "import os,sqlite3; p='/app/mvp_backend/obstacle_data/obstacles.sqlite'; print(sqlite3.connect(p).execute('SELECT COUNT(*) FROM obstacles').fetchone()[0]) if os.path.exists(p) else print(0)" 2>/dev/null
}

# Ensure obstacles DB exists in mounted volume.
# The obstacle volume can be empty on fresh VPS deploys even when the image has
# seed data; repopulate from DOF.CSV or download/build once when missing.
if [ "$(_obs_count)" = "0" ]; then
    if [ ! -f /app/mvp_backend/obstacle_data/DOF.CSV ] && [ -f /app_seed/obstacle_data/DOF.CSV ]; then
        echo "Seeding DOF.CSV into obstacle volume..."
        cp /app_seed/obstacle_data/DOF.CSV /app/mvp_backend/obstacle_data/DOF.CSV || true
    fi

    if [ -f /app/mvp_backend/obstacle_data/DOF.CSV ]; then
        echo "Building obstacles.sqlite from DOF.CSV..."
        python3 -c "from mvp_backend.refresh_faa import _build_obstacles_db; _build_obstacles_db()" \
            || echo "WARN: obstacle DB build failed"
    else
        echo "DOF.CSV missing; downloading latest FAA DOF to build obstacles..."
        python3 -c "from mvp_backend.refresh_faa import download_dof; download_dof()" \
            || echo "WARN: DOF download/build failed"
    fi
fi

# Rebuild airports_solver.csv from FAA NASR volume data if available
if [ -f /app/faa_nasr/extract/APT.txt ]; then
    echo "Rebuilding airports_solver.csv from FAA NASR data..."
    python3 -m mvp_backend.build_airports_solver 2>&1 || echo "WARN: CSV rebuild failed (using bundled copy)"
fi

echo "  SRTM tiles: $(ls /app/mvp_backend/srtm_cache/*.hgt 2>/dev/null | wc -l) cached"
echo "  Airports:   $(wc -l < /app/mvp_backend/airports_solver.csv 2>/dev/null || echo 0) rows"
echo "  Obstacles:  $(_obs_count)"
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
