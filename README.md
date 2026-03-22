# GroundHog (MVP)

Terrain-avoidance + fuel-stop route planner MVP for helicopters.

## What it does (today)
- FAA NASR 28-day subscription ingest to derive fuel availability by type (100LL/Jet-A) + public-use flag.
- SRTM terrain-avoidance pathfinding on a grid (A*), constrained by:
  - max MSL
  - min AGL
- Route search minimizing total time.
- Web UI (Leaflet) served by FastAPI.

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 1) Download NASR + build airports table
python -m mvp_backend.build_airports_solver

# 2) Run server
uvicorn mvp_backend.server:app --host 0.0.0.0 --port 8000
```

Open:
- http://localhost:8000/

## Notes
- The first route solve in a region will download and cache SRTM tiles into `mvp_backend/srtm_cache/`.
- Fuel availability comes from NASR `APT.txt` field A70.

## Known limitations
- Elevation model + search bounds are MVP-grade.
- Fuel join coverage needs improvement (some airports show no fuel when they should).
