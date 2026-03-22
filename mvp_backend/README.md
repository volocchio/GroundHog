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
