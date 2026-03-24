from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
import time

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field
from typing import List

from mvp_backend.planner import load_airports_solver, plan_route_multi_stop, terrain_avoid_leg, terrain_avoid_leg_streaming, leg_fuel_ok, plan_stop_sequence, plan_stop_sequences
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

    obstacle_radius_nm: float = Field(default=0.5, ge=0, le=5.0)
    obstacle_clearance_ft: float = Field(default=500, ge=0, le=2000)

    adsb_out: bool = Field(default=True)
    avoid_airspace: list[str] = Field(default_factory=lambda: ["P", "R"])

    waypoints: list[str] = Field(default_factory=list)


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
    """Return all airports for map display."""
    airports = load_airports_solver()
    return [
        {
            "icao": ap.icao,
            "name": ap.name,
            "lat": ap.lat,
            "lon": ap.lon,
            "elev": ap.elevation_ft,
            "facility_use": ap.facility_use,
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

    # Resolve waypoints into airport objects; -NF suffix = no fuel
    waypoint_aps = []
    waypoint_fuel = {}  # icao → True (refuel) / False (no fuel)
    for wp_raw in (req.waypoints or []):
        raw = wp_raw.strip().upper()
        if raw.endswith('-NF'):
            icao = raw[:-3].strip()
            fuel = False
        else:
            icao = raw
            fuel = True
        wp = airports.get(icao)
        if not wp:
            raise HTTPException(404, f"Unknown waypoint airport: {icao}")
        waypoint_aps.append(wp)
        waypoint_fuel[icao] = fuel

    # Segments: dep → wp1, wp1 → wp2, ..., wpN → arr
    segment_endpoints = [dep] + waypoint_aps + [arr]

    route_cache.record_request(dep.icao, arr.icao, req.max_msl_ft, req.min_agl_ft, req.max_detour_factor,
                                 cruise_kt=req.cruise_speed_kt, fuel_gal=req.usable_fuel_gal,
                                 burn_gph=req.fuel_burn_gph, reserve_min=req.reserve_min,
                                 fuel_type=req.required_fuel,
                                 max_climb_fpm=req.max_climb_fpm, max_descent_fpm=req.max_descent_fpm,
                                 climb_speed_kt=req.climb_speed_kt, descent_speed_kt=req.descent_speed_kt)

    def generate():
        # ── ADS-B compliance check ──
        if not req.adsb_out:
            all_aps = [dep] + waypoint_aps + [arr]
            hits = _airports_in_airspace(all_aps, classes=("B", "C"))
            if hits:
                yield f"data: {json.dumps({'type': 'adsb_warning', 'airports': hits})}\n\n"

        # First pass: plan all segments to get total leg count and stops
        all_sequences = []
        blocked_pairs = set()
        seg_last_leg_dist = 0.0  # actual A* distance of last leg in prev segment
        for si in range(len(segment_endpoints) - 1):
            seg_dep = segment_endpoints[si]
            seg_arr = segment_endpoints[si + 1]

            # Determine starting fuel: full if first segment or if we refuel here
            if si == 0 or waypoint_fuel.get(seg_dep.icao, True):
                start_fuel = req.usable_fuel_gal
            else:
                # No fuel at this waypoint — estimate remaining from last leg
                last_leg_time = seg_last_leg_dist / req.cruise_speed_kt if req.cruise_speed_kt > 0 else 0
                fuel_used = last_leg_time * req.fuel_burn_gph
                start_fuel = max(0.0, req.usable_fuel_gal - fuel_used)

            max_retries = 5
            segment_ok = False
            for attempt in range(max_retries + 1):
                # Escalate detour factor on retries (+0.15 per attempt)
                eff_detour = req.max_detour_factor + attempt * 0.15
                # Last-ditch: expand search budget
                eff_expansions = 5000 if attempt >= max_retries else 2000

                # Get up to 3 alternative stop sequences
                sequences_pool = plan_stop_sequences(
                    dep=seg_dep, arr=seg_arr, airports=airports,
                    cruise_speed_kt=req.cruise_speed_kt,
                    usable_fuel_gal=req.usable_fuel_gal,
                    start_fuel_gal=start_fuel,
                    burn_gph=req.fuel_burn_gph,
                    reserve_min=req.reserve_min,
                    required_fuel=req.required_fuel,
                    max_detour_factor=eff_detour,
                    blocked_pairs=blocked_pairs,
                    max_msl_ft=req.max_msl_ft,
                    min_agl_ft=req.min_agl_ft,
                    max_climb_fpm=req.max_climb_fpm,
                    max_descent_fpm=req.max_descent_fpm,
                    climb_speed_kt=req.climb_speed_kt,
                    descent_speed_kt=req.descent_speed_kt,
                    k=3,
                    max_expansions=eff_expansions,
                )

                if not sequences_pool:
                    yield f"data: {json.dumps({'type': 'no_path', 'message': f'No fuel-feasible route found for segment {seg_dep.icao} → {seg_arr.icao} (detour {eff_detour:.1f}x, attempt {attempt+1}/{max_retries+1}).'})}\n\n"
                    yield f"data: {json.dumps({'type': 'done'})}\n\n"
                    return

                # Try each candidate sequence until one succeeds all legs
                seq_succeeded = False
                for seq_idx, sequence in enumerate(sequences_pool):
                    seg_num_legs = len(sequence) - 1
                    leg_offset = sum(len(s) - 1 for s in all_sequences)

                    # Build flattened stops list for route_plan event
                    flat_stops = []
                    for prev_seq in all_sequences:
                        flat_stops.extend(ap.icao for ap in prev_seq[1:])
                    flat_stops.extend(ap.icao for ap in sequence[1:-1])
                    for fi in range(si + 1, len(segment_endpoints) - 1):
                        flat_stops.append(segment_endpoints[fi + 1].icao if fi + 1 < len(segment_endpoints) - 1 else None)
                    flat_stops = [s for s in flat_stops if s and s != arr.icao]

                    total_legs_so_far = leg_offset + seg_num_legs
                    yield f"data: {json.dumps({'type': 'route_plan', 'stops': flat_stops, 'num_legs': total_legs_so_far})}\n\n"

                    leg_failed = False
                    last_fail_event = None
                    for i in range(seg_num_legs):
                        from_ap = sequence[i]
                        to_ap = sequence[i + 1]
                        global_leg = leg_offset + i

                        prev_pt = None
                        if i > 0:
                            prev_pt = (sequence[i - 1].lat, sequence[i - 1].lon)
                        elif si > 0:
                            prev_seq = all_sequences[-1] if all_sequences else None
                            if prev_seq and len(prev_seq) >= 2:
                                prev_pt = (prev_seq[-2].lat, prev_seq[-2].lon)

                        yield f"data: {json.dumps({'type': 'leg_start', 'from': from_ap.icao, 'to': to_ap.icao, 'leg_index': global_leg})}\n\n"

                        for event in terrain_avoid_leg_streaming(
                            from_ap, to_ap,
                            max_msl_ft=req.max_msl_ft,
                            min_agl_ft=req.min_agl_ft,
                            max_detour_factor=eff_detour,
                            max_climb_fpm=req.max_climb_fpm,
                            max_descent_fpm=req.max_descent_fpm,
                            cruise_speed_kt=req.cruise_speed_kt,
                            climb_speed_kt=req.climb_speed_kt,
                            descent_speed_kt=req.descent_speed_kt,
                            avoid_airspace=req.avoid_airspace or [],
                            obstacle_radius_nm=req.obstacle_radius_nm,
                            obstacle_clearance_ft=req.obstacle_clearance_ft,
                            prev_point=prev_pt,
                        ):
                            if event.get("type") == "path":
                                event["leg_index"] = global_leg
                                event["from"] = from_ap.icao
                                event["to"] = to_ap.icao
                                seg_last_leg_dist = event.get("dist_nm", 0.0)
                            if event.get("type") == "no_path":
                                blocked_pairs.add((from_ap.icao, to_ap.icao))
                                leg_failed = True
                                last_fail_event = event
                                last_fail_event["from"] = from_ap.icao
                                last_fail_event["to"] = to_ap.icao
                                last_fail_event["leg_index"] = global_leg
                                break
                            yield f"data: {json.dumps(event)}\n\n"

                        if leg_failed:
                            break

                    if not leg_failed:
                        all_sequences.append(sequence)
                        segment_ok = True
                        seq_succeeded = True
                        break
                    # else: try next candidate sequence from the pool

                if seq_succeeded:
                    break  # segment done, move to next

                # All candidate sequences failed for this attempt — reroute or give up
                if attempt < max_retries:
                    yield f"data: {json.dumps({'type': 'reroute', 'message': f'Attempt {attempt+1} failed — replanning segment with wider detour...', 'blocked': [list(p) for p in blocked_pairs], 'keep_legs': sum(len(s) - 1 for s in all_sequences)})}\n\n"
                else:
                    # Final failure — include diagnostic detail
                    msg = f"No terrain-avoiding path for leg: {last_fail_event['from']} → {last_fail_event['to']} (all {max_retries+1} attempts exhausted)"
                    detail = last_fail_event.get("detail", "")
                    if detail:
                        msg += f"\n{detail}"
                    last_fail_event["message"] = msg
                    yield f"data: {json.dumps(last_fail_event)}\n\n"
                    yield f"data: {json.dumps({'type': 'done'})}\n\n"
                    return

            if not segment_ok:
                return  # already yielded no_path/done above

        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


class ProfileRequest(BaseModel):
    path: List[List[float]]  # [[lat, lon], ...]
    max_msl_ft: float = Field(gt=0)
    min_agl_ft: float = Field(ge=0)
    avoid_airspace: List[str] = []


def _profile_airspace_zones(
    coords: list, elev_ft: list, avoid_classes: list[str], min_agl_ft: float,
) -> list[dict]:
    """Return airspace zones that overlap the path, with MSL floor/ceiling per sample point."""
    import json as _json
    from mvp_backend.planner import _point_in_polygon

    if not avoid_classes:
        return []

    db_path = os.path.join(ROOT, "mvp_backend", "airspace_data", "airspace.sqlite")
    if not os.path.exists(db_path):
        return []

    lats = [c[0] for c in coords]
    lons = [c[1] for c in coords]
    bbox_min_lat, bbox_max_lat = min(lats), max(lats)
    bbox_min_lon, bbox_max_lon = min(lons), max(lons)

    ph = ",".join("?" for _ in avoid_classes)
    sql = f"""
        SELECT name, class, lower_alt, lower_code, upper_alt, upper_code, geometry
        FROM airspace
        WHERE class IN ({ph})
          AND max_lat >= ? AND min_lat <= ?
          AND max_lon >= ? AND min_lon <= ?
    """
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            sql, avoid_classes + [bbox_min_lat, bbox_max_lat, bbox_min_lon, bbox_max_lon],
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return []

    zones: list[dict] = []
    for name, cls, lower_alt, lower_code, upper_alt, upper_code, geom_str in rows:
        if lower_alt is None or upper_alt is None:
            continue
        geom = _json.loads(geom_str)
        gtype = geom.get("type", "")
        if gtype == "Polygon":
            rings = [geom["coordinates"][0]]
        elif gtype == "MultiPolygon":
            rings = [poly[0] for poly in geom["coordinates"]]
        else:
            continue

        # Test each sample point against this airspace polygon
        inside = [False] * len(coords)
        for ring in rings:
            rlons = [c[0] for c in ring]
            rlats = [c[1] for c in ring]
            rmin_lat, rmax_lat = min(rlats), max(rlats)
            rmin_lon, rmax_lon = min(rlons), max(rlons)
            for idx, (lat, lon) in enumerate(coords):
                if inside[idx]:
                    continue
                if lat < rmin_lat or lat > rmax_lat or lon < rmin_lon or lon > rmax_lon:
                    continue
                if _point_in_polygon(lon, lat, ring):
                    inside[idx] = True

        # Build contiguous index ranges where path is inside this airspace
        i = 0
        while i < len(inside):
            if not inside[i]:
                i += 1
                continue
            start = i
            while i < len(inside) and inside[i]:
                i += 1
            end = i - 1  # inclusive

            # Compute MSL floor/ceiling for each sample in [start, end]
            floors = []
            ceilings = []
            for idx in range(start, end + 1):
                terrain = elev_ft[idx]
                lc = (lower_code or "").upper()
                if lc == "SFC":
                    f = terrain
                elif lc == "AGL":
                    f = terrain + lower_alt
                else:
                    f = lower_alt
                uc = (upper_code or "").upper()
                if uc == "FL":
                    c = upper_alt * 100
                elif uc == "AGL":
                    c = terrain + upper_alt
                else:
                    c = upper_alt
                floors.append(round(f))
                ceilings.append(round(c))

            zones.append({
                "name": name or cls,
                "cls": cls,
                "start_idx": start,
                "end_idx": end,
                "floor_msl": floors,
                "ceiling_msl": ceilings,
            })

    return zones


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

    # Compute airspace zones along the path
    airspace_zones = _profile_airspace_zones(
        coords, elev_ft, req.avoid_airspace, req.min_agl_ft,
    )

    return {
        "dist_nm": [round(d, 2) for d in dist_nm],
        "ground_ft": [round(e, 0) for e in elev_ft],
        "max_msl_ft": req.max_msl_ft,
        "min_agl_ft": req.min_agl_ft,
        "airspace_zones": airspace_zones,
    }


# ── cache stats endpoint ─────────────────────────────────────────────

@app.get("/cache/stats")
def cache_stats():
    """Return cache and route history statistics."""
    return route_cache.cache_stats()


# ── obstacles endpoint ───────────────────────────────────────────────

_OBS_DB = os.path.join(os.path.dirname(__file__), "obstacle_data", "obstacles.sqlite")

@app.get("/obstacles")
def get_obstacles(
    south: float = Query(...), north: float = Query(...),
    west: float = Query(...), east: float = Query(...),
    min_agl: int = Query(200),
):
    """Return obstacles within a bounding box, filtered by min AGL height."""
    if not os.path.exists(_OBS_DB):
        return []
    conn = sqlite3.connect(_OBS_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT lat, lon, agl, amsl, type, lit FROM obstacles "
        "WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ? AND agl >= ? "
        "LIMIT 5000",
        (south, north, west, east, min_agl),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── airspace endpoint ────────────────────────────────────────────────

_AIRSPACE_DB = os.path.join(os.path.dirname(__file__), "airspace_data", "airspace.sqlite")


def _airports_in_airspace(airports_list, classes=("B", "C")):
    """Return list of {icao, airspace_class, airspace_name} for airports
    that fall inside any of the given airspace classes."""
    if not os.path.exists(_AIRSPACE_DB) or not airports_list:
        return []
    placeholders = ",".join("?" for _ in classes)
    conn = sqlite3.connect(_AIRSPACE_DB)
    results = []
    for ap in airports_list:
        rows = conn.execute(
            f"SELECT class, name, geometry FROM airspace "
            f"WHERE class IN ({placeholders}) "
            f"AND min_lat <= ? AND max_lat >= ? AND min_lon <= ? AND max_lon >= ?",
            list(classes) + [ap.lat, ap.lat, ap.lon, ap.lon]
        ).fetchall()
        for cls, name, geom_str in rows:
            import json as _json
            geom = _json.loads(geom_str)
            gtype = geom.get("type", "")
            if gtype == "Polygon":
                rings = [geom["coordinates"][0]]
            elif gtype == "MultiPolygon":
                rings = [poly[0] for poly in geom["coordinates"]]
            else:
                continue
            for ring in rings:
                if _point_in_ring(ap.lon, ap.lat, ring):
                    results.append({"icao": ap.icao, "airspace_class": cls, "airspace_name": name})
                    break
            else:
                continue
            break  # found one match for this airport, skip remaining polygons
    conn.close()
    return results


def _point_in_ring(px, py, ring):
    """Ray-casting point-in-polygon."""
    n = len(ring)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if ((yi > py) != (yj > py)) and (px < (xj - xi) * (py - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside

@app.get("/airspace")
def get_airspace(
    south: float = Query(...), north: float = Query(...),
    west: float = Query(...), east: float = Query(...),
    classes: str = Query("B,C,D,R,P,W,MOA,A"),
):
    """Return airspace polygons that intersect a bounding box."""
    if not os.path.exists(_AIRSPACE_DB):
        return []
    cls_list = [c.strip().upper() for c in classes.split(",") if c.strip()]
    if not cls_list:
        return []
    conn = sqlite3.connect(_AIRSPACE_DB)
    conn.row_factory = sqlite3.Row
    placeholders = ",".join("?" for _ in cls_list)
    rows = conn.execute(
        f"SELECT ident, icao_id, name, class, local_type, "
        f"lower_alt, lower_code, upper_alt, upper_code, times_of_use, geometry "
        f"FROM airspace "
        f"WHERE max_lat >= ? AND min_lat <= ? AND max_lon >= ? AND min_lon <= ? "
        f"AND class IN ({placeholders}) "
        f"LIMIT 2000",
        (south, north, west, east, *cls_list),
    ).fetchall()
    conn.close()
    import json as _json
    result = []
    for r in rows:
        d = dict(r)
        d["geometry"] = _json.loads(d["geometry"])
        result.append(d)
    return result


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
