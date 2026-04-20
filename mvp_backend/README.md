# MVP Terrain + Fuel Route Planner (Backend)

## Run

```bash
cd /root/.openclaw/workspace-volo_coding
source .venv/bin/activate
uvicorn mvp_backend.server:app --reload --port 8000
```

## Endpoints

- `GET /health`
- `POST /route`

## Notes
- Uses FAA 28-day NASR subscription for fuel types (APT.txt).
- Uses OpenTopoData SRTM90m for elevations (MVP). Replace with local SRTM tiles later.

## Helicopter Ceiling Curves
- Optional file: `mvp_backend/helicopter_ceiling_curves.json`
- Purpose: provide chart-digitized enroute ceiling density-altitude curves vs gross weight for each helicopter type.
- If a type has a curve, the app uses that chart-derived curve for weight-sensitive ceiling estimation.
- If no curve is provided, the app falls back to the conservative service-ceiling/HIGE proxy model.
