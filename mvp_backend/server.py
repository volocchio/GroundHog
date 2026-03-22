from __future__ import annotations

import json
import math
import os

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field
from typing import List

from mvp_backend.planner import load_airports_solver, plan_route_multi_stop, terrain_avoid_leg_streaming
from mvp_backend.srtm_local import SRTMProvider
from mvp_backend.grid_astar import _haversine_nm

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))



app = FastAPI(title="Terrain+Fuel Route Planner MVP")


class RouteRequest(BaseModel):
    dep_icao: str
    arr_icao: str

    cruise_speed_kt: float = Field(gt=0)
    usable_fuel_gal: float = Field(gt=0)
    fuel_burn_gph: float = Field(gt=0)
    reserve_min: float = Field(ge=0)

    min_agl_ft: float = Field(ge=0)
    max_msl_ft: float = Field(gt=0)

    required_fuel: str = Field(pattern=r"^(100LL|JETA)$")

    max_detour_factor: float = Field(default=1.8, ge=1.0, le=5.0)


@app.get("/", response_class=HTMLResponse)
def index():
    # Simple single-file webapp
    import os
    here = os.path.dirname(__file__)
    with open(os.path.join(here, "webapp.html"), "r", encoding="utf-8") as f:
        return f.read()


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/airports")
def airports_list():
    """Return all public-use airports for map display."""
    airports = load_airports_solver()
    return [
        {
            "icao": ap.icao,
            "name": ap.name,
            "lat": ap.lat,
            "lon": ap.lon,
            "elev": ap.elevation_ft,
            "fuel_100ll": ap.fuel_100ll,
            "fuel_jeta": ap.fuel_jeta,
        }
        for ap in airports.values()
    ]


@app.post("/route")
def route(req: RouteRequest):
    airports = load_airports_solver()
    dep = airports.get(req.dep_icao.strip().upper())
    arr = airports.get(req.arr_icao.strip().upper())
    if not dep:
        raise HTTPException(404, f"Unknown dep_icao {req.dep_icao}")
    if not arr:
        raise HTTPException(404, f"Unknown arr_icao {req.arr_icao}")

    result = plan_route_multi_stop(
        dep=dep,
        arr=arr,
        airports=airports,
        cruise_speed_kt=req.cruise_speed_kt,
        usable_fuel_gal=req.usable_fuel_gal,
        burn_gph=req.fuel_burn_gph,
        reserve_min=req.reserve_min,
        max_msl_ft=req.max_msl_ft,
        min_agl_ft=req.min_agl_ft,
        required_fuel=req.required_fuel,
        max_detour_factor=req.max_detour_factor,
    )
    return result


@app.post("/route/stream")
def route_stream(req: RouteRequest):
    """SSE endpoint that streams A* exploration events for visualization."""
    airports = load_airports_solver()
    dep = airports.get(req.dep_icao.strip().upper())
    arr = airports.get(req.arr_icao.strip().upper())
    if not dep:
        raise HTTPException(404, f"Unknown dep_icao {req.dep_icao}")
    if not arr:
        raise HTTPException(404, f"Unknown arr_icao {req.arr_icao}")

    def generate():
        # First run the streaming terrain leg for the direct pair
        yield f"data: {json.dumps({'type': 'leg_start', 'from': dep.icao, 'to': arr.icao})}\n\n"
        for event in terrain_avoid_leg_streaming(
            dep, arr,
            max_msl_ft=req.max_msl_ft,
            min_agl_ft=req.min_agl_ft,
            max_detour_factor=req.max_detour_factor,
        ):
            yield f"data: {json.dumps(event)}\n\n"
        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


class ProfileRequest(BaseModel):
    path: List[List[float]]  # [[lat, lon], ...]
    max_msl_ft: float = Field(gt=0)
    min_agl_ft: float = Field(ge=0)


@app.post("/elevation-profile")
def elevation_profile(req: ProfileRequest):
    """Return ground elevation profile along a route path."""
    from mvp_backend.terrain_provider import meters_to_feet

    if len(req.path) < 2:
        raise HTTPException(400, "Path must have at least 2 points")

    # Subsample if path has too many points (keep it fast)
    pts = req.path
    max_samples = 300
    if len(pts) > max_samples:
        step = len(pts) / max_samples
        pts = [pts[int(i * step)] for i in range(max_samples)]
        pts.append(req.path[-1])

    provider = SRTMProvider(cache_dir=os.path.join(ROOT, "mvp_backend", "srtm_cache"))
    coords = [(p[0], p[1]) for p in pts]
    elev_m = provider.get_many_m(coords)
    elev_ft = [meters_to_feet(m) if m == m else 0.0 for m in elev_m]

    # Build cumulative distance
    dist_nm = [0.0]
    for i in range(1, len(coords)):
        d = _haversine_nm(coords[i-1][0], coords[i-1][1], coords[i][0], coords[i][1])
        dist_nm.append(dist_nm[-1] + d)

    return {
        "dist_nm": [round(d, 2) for d in dist_nm],
        "ground_ft": [round(e, 0) for e in elev_ft],
        "max_msl_ft": req.max_msl_ft,
        "min_agl_ft": req.min_agl_ft,
    }
