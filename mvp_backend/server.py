from __future__ import annotations

import json
import math
import os
import threading
import time

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field
from typing import List

from mvp_backend.planner import load_airports_solver, plan_route_multi_stop, terrain_avoid_leg, terrain_avoid_leg_streaming, leg_fuel_ok, plan_stop_sequence
from mvp_backend.srtm_local import SRTMProvider
from mvp_backend.grid_astar import _haversine_nm
from mvp_backend import route_cache

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


route_cache.init_db()

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

    max_climb_fpm: float = Field(default=0, ge=0)
    max_descent_fpm: float = Field(default=0, ge=0)

    climb_speed_kt: float = Field(default=0, ge=0)
    descent_speed_kt: float = Field(default=0, ge=0)

    terrain_follow: bool = Field(default=False)


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

    route_cache.record_request(dep.icao, arr.icao, req.max_msl_ft, req.min_agl_ft, req.max_detour_factor,
                                 cruise_kt=req.cruise_speed_kt, fuel_gal=req.usable_fuel_gal,
                                 burn_gph=req.fuel_burn_gph, reserve_min=req.reserve_min,
                                 fuel_type=req.required_fuel,
                                 max_climb_fpm=req.max_climb_fpm, max_descent_fpm=req.max_descent_fpm,
                                 climb_speed_kt=req.climb_speed_kt, descent_speed_kt=req.descent_speed_kt)

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
        max_climb_fpm=req.max_climb_fpm,
        max_descent_fpm=req.max_descent_fpm,
        climb_speed_kt=req.climb_speed_kt,
        descent_speed_kt=req.descent_speed_kt,
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

    route_cache.record_request(dep.icao, arr.icao, req.max_msl_ft, req.min_agl_ft, req.max_detour_factor,
                                 cruise_kt=req.cruise_speed_kt, fuel_gal=req.usable_fuel_gal,
                                 burn_gph=req.fuel_burn_gph, reserve_min=req.reserve_min,
                                 fuel_type=req.required_fuel,
                                 max_climb_fpm=req.max_climb_fpm, max_descent_fpm=req.max_descent_fpm,
                                 climb_speed_kt=req.climb_speed_kt, descent_speed_kt=req.descent_speed_kt)

    def generate():
        blocked_pairs = set()
        max_retries = 3

        for attempt in range(max_retries + 1):
            sequence = plan_stop_sequence(
                dep=dep, arr=arr, airports=airports,
                cruise_speed_kt=req.cruise_speed_kt,
                usable_fuel_gal=req.usable_fuel_gal,
                burn_gph=req.fuel_burn_gph,
                reserve_min=req.reserve_min,
                required_fuel=req.required_fuel,
                max_detour_factor=req.max_detour_factor,
                blocked_pairs=blocked_pairs,
                max_msl_ft=req.max_msl_ft,
                min_agl_ft=req.min_agl_ft,
                max_climb_fpm=req.max_climb_fpm,
                max_descent_fpm=req.max_descent_fpm,
                climb_speed_kt=req.climb_speed_kt,
                descent_speed_kt=req.descent_speed_kt,
            )

            if sequence is None:
                yield f"data: {json.dumps({'type': 'no_path', 'message': 'No fuel-feasible route found (all alternatives exhausted).'})}\n\n"
                yield f"data: {json.dumps({'type': 'done'})}\n\n"
                return

            num_legs = len(sequence) - 1
            stops = [ap.icao for ap in sequence[1:-1]]

            yield f"data: {json.dumps({'type': 'route_plan', 'stops': stops, 'num_legs': num_legs})}\n\n"

            leg_failed = False
            for i in range(num_legs):
                from_ap = sequence[i]
                to_ap = sequence[i + 1]

                yield f"data: {json.dumps({'type': 'leg_start', 'from': from_ap.icao, 'to': to_ap.icao, 'leg_index': i})}\n\n"

                for event in terrain_avoid_leg_streaming(
                    from_ap, to_ap,
                    max_msl_ft=req.max_msl_ft,
                    min_agl_ft=req.min_agl_ft,
                    max_detour_factor=req.max_detour_factor,
                    max_climb_fpm=req.max_climb_fpm,
                    max_descent_fpm=req.max_descent_fpm,
                    cruise_speed_kt=req.cruise_speed_kt,
                    climb_speed_kt=req.climb_speed_kt,
                    descent_speed_kt=req.descent_speed_kt,
                ):
                    if event.get("type") == "path":
                        event["leg_index"] = i
                        event["from"] = from_ap.icao
                        event["to"] = to_ap.icao
                    if event.get("type") == "no_path":
                        # Block this pair and retry with a new stop sequence
                        blocked_pairs.add((from_ap.icao, to_ap.icao))
                        leg_failed = True
                        if attempt < max_retries:
                            yield f"data: {json.dumps({'type': 'reroute', 'message': f'Leg {from_ap.icao} → {to_ap.icao} blocked by terrain — replanning...', 'blocked': list(blocked_pairs)})}\n\n"
                        else:
                            event["leg_index"] = i
                            event["from"] = from_ap.icao
                            event["to"] = to_ap.icao
                            event["message"] = f"No terrain-avoiding path found for leg {i+1}: {from_ap.icao} \u2192 {to_ap.icao} (retries exhausted)"
                            yield f"data: {json.dumps(event)}\n\n"
                            yield f"data: {json.dumps({'type': 'done'})}\n\n"
                            return
                        break
                    yield f"data: {json.dumps(event)}\n\n"

                if leg_failed:
                    break

            if not leg_failed:
                yield f"data: {json.dumps({'type': 'done'})}\n\n"
                return

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


# ── cache stats endpoint ─────────────────────────────────────────────

@app.get("/cache/stats")
def cache_stats():
    """Return cache and route history statistics."""
    return route_cache.cache_stats()


# ── background precomputation ─────────────────────────────────────────

_bg_thread: threading.Thread | None = None
_bg_stop = threading.Event()


def _background_precompute():
    """Precompute A* legs for popular routes that aren't yet cached."""
    while not _bg_stop.is_set():
        _bg_stop.wait(60)  # run every 60 seconds
        if _bg_stop.is_set():
            break
        try:
            popular = route_cache.popular_routes(limit=10)
            if not popular:
                continue
            airports = load_airports_solver()
            for entry in popular:
                if _bg_stop.is_set():
                    break
                dep = airports.get(entry["dep"])
                arr = airports.get(entry["arr"])
                if not dep or not arr:
                    continue
                # plan fuel stop sequence to get the individual legs
                seq = plan_stop_sequence(
                    dep=dep, arr=arr, airports=airports,
                    cruise_speed_kt=entry.get("cruise_kt", 120),
                    usable_fuel_gal=entry.get("fuel_gal", 48),
                    burn_gph=entry.get("burn_gph", 13),
                    reserve_min=entry.get("reserve_min", 30),
                    required_fuel=entry.get("fuel_type", "100LL"),
                    max_detour_factor=entry["detour_fac"],
                    max_msl_ft=entry["max_msl_ft"],
                    min_agl_ft=entry["min_agl_ft"],
                    max_climb_fpm=entry.get("max_climb_fpm", 0),
                    max_descent_fpm=entry.get("max_descent_fpm", 0),
                    climb_speed_kt=entry.get("climb_speed_kt", 0),
                    descent_speed_kt=entry.get("descent_speed_kt", 0),
                )
                if not seq:
                    continue
                # precompute each leg (cache lookup inside will skip already-cached)
                for i in range(len(seq) - 1):
                    if _bg_stop.is_set():
                        break
                    terrain_avoid_leg(
                        seq[i], seq[i + 1],
                        max_msl_ft=entry["max_msl_ft"],
                        min_agl_ft=entry["min_agl_ft"],
                        max_detour_factor=entry["detour_fac"],
                        max_climb_fpm=entry.get("max_climb_fpm", 0),
                        max_descent_fpm=entry.get("max_descent_fpm", 0),
                        cruise_speed_kt=entry.get("cruise_kt", 120),
                        climb_speed_kt=entry.get("climb_speed_kt", 0),
                        descent_speed_kt=entry.get("descent_speed_kt", 0),
                    )
        except Exception:
            pass  # background worker should never crash the app


@app.on_event("startup")
def _start_bg_worker():
    global _bg_thread
    _bg_thread = threading.Thread(target=_background_precompute, daemon=True)
    _bg_thread.start()


@app.on_event("shutdown")
def _stop_bg_worker():
    _bg_stop.set()
    if _bg_thread:
        _bg_thread.join(timeout=5)
