from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from mvp_backend.planner import load_airports_solver, plan_route_multi_stop


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
