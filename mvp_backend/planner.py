from __future__ import annotations

import csv
import math
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from mvp_backend.grid_astar import GridSpec, astar_path, astar_path_streaming, path_nm
from mvp_backend.terrain_provider import meters_to_feet
from mvp_backend.srtm_local import SRTMProvider


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_AIRPORTS_SOLVER = os.path.join(ROOT, "mvp_backend", "airports_solver.csv")
CACHE_DB = os.path.join(ROOT, "mvp_backend", "elev_cache.sqlite")


@dataclass
class Airport:
    icao: str
    name: str
    lat: float
    lon: float
    elevation_ft: float
    facility_use: str
    fuel_100ll: int
    fuel_jeta: int


def load_airports_solver(path: str = DATA_AIRPORTS_SOLVER) -> Dict[str, Airport]:
    out: Dict[str, Airport] = {}
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            icao = (row.get("icao") or "").strip().upper()
            lid = (row.get("lid") or "").strip().upper()
            if not icao and not lid:
                continue
            ap = Airport(
                icao=icao or lid,
                name=row.get("name") or "",
                lat=float(row.get("lat") or 0),
                lon=float(row.get("lon") or 0),
                elevation_ft=float(row.get("elevation_ft") or 0),
                facility_use=(row.get("facility_use") or "").strip().upper(),
                fuel_100ll=int(row.get("fuel_100ll") or 0),
                fuel_jeta=int(row.get("fuel_jeta") or 0),
            )
            # Index by ICAO code
            if icao:
                out[icao] = ap
            # Also index by LID so users can type either
            if lid and lid not in out:
                out[lid] = ap
    return out


def _deg_per_km_lat() -> float:
    return 1.0 / 110.574


def _deg_per_km_lon(lat: float) -> float:
    return 1.0 / (111.320 * math.cos(math.radians(lat)) + 1e-9)


def _direct_nm(a: Airport, b: Airport) -> float:
    # reuse haversine from grid_astar
    from mvp_backend.grid_astar import _haversine_nm
    return _haversine_nm(a.lat, a.lon, b.lat, b.lon)


@dataclass
class LegResult:
    dist_nm: float
    path_latlon: List[Tuple[float, float]]


def terrain_avoid_leg(
    a: Airport,
    b: Airport,
    max_msl_ft: float,
    min_agl_ft: float,
    max_detour_factor: float,
    cell_km: float = 1.0,
    initial_margin_km: float = 40.0,
    margin_step_km: float = 40.0,
    max_margin_km: float = 400.0,
) -> Optional[LegResult]:
    """Find terrain-avoiding path between airports using grid A*.

    Expands a bounding-box margin until a path is found with acceptable detour.
    """

    provider = SRTMProvider(cache_dir=os.path.join(ROOT, "mvp_backend", "srtm_cache"))

    direct_nm = _direct_nm(a, b)
    direct_km = direct_nm * 1.852
    detour_limit_nm = max_detour_factor * direct_nm

    ceiling_ft = max_msl_ft - min_agl_ft

    mid_lat = 0.5 * (a.lat + b.lat)
    dlat = cell_km * _deg_per_km_lat()
    dlon = cell_km * _deg_per_km_lon(mid_lat)

    margin_km = initial_margin_km
    while margin_km <= max_margin_km:
        # bbox expanded
        min_lat = min(a.lat, b.lat) - margin_km * _deg_per_km_lat()
        max_lat = max(a.lat, b.lat) + margin_km * _deg_per_km_lat()
        min_lon = min(a.lon, b.lon) - margin_km * _deg_per_km_lon(mid_lat)
        max_lon = max(a.lon, b.lon) + margin_km * _deg_per_km_lon(mid_lat)

        n_lat = int(math.ceil((max_lat - min_lat) / dlat)) + 1
        n_lon = int(math.ceil((max_lon - min_lon) / dlon)) + 1

        # Hard cap to keep compute sane for MVP
        if n_lat * n_lon > 250_000:
            margin_km += margin_step_km
            continue

        grid = GridSpec(lat0=min_lat, lon0=min_lon, n_lat=n_lat, n_lon=n_lon, dlat=dlat, dlon=dlon)

        # generate points in row-major order for batch elevation
        points = [grid.idx_to_latlon(i, j) for i in range(n_lat) for j in range(n_lon)]
        elev_m = provider.get_many_m(points)
        elev_ft = [meters_to_feet(m) if m == m else float("inf") for m in elev_m]

        passable: List[List[bool]] = [[True] * n_lon for _ in range(n_lat)]
        k = 0
        for i in range(n_lat):
            row = passable[i]
            for j in range(n_lon):
                row[j] = elev_ft[k] <= ceiling_ft
                k += 1

        start = grid.latlon_to_idx(a.lat, a.lon)
        goal = grid.latlon_to_idx(b.lat, b.lon)
        if not passable[start[0]][start[1]] or not passable[goal[0]][goal[1]]:
            # start/goal blocked at this ceiling
            margin_km += margin_step_km
            continue

        path_idx = astar_path(grid, passable, start, goal)
        if not path_idx:
            margin_km += margin_step_km
            continue

        dist_nm = path_nm(grid, path_idx)
        if dist_nm <= detour_limit_nm:
            path_latlon = [grid.idx_to_latlon(i, j) for i, j in path_idx]
            return LegResult(dist_nm=dist_nm, path_latlon=path_latlon)

        # Found path but too long: expand margin (sometimes finds a shorter corridor)
        margin_km += margin_step_km

    return None


def leg_fuel_ok(dist_nm: float, cruise_speed_kt: float, usable_fuel_gal: float, burn_gph: float, reserve_min: float) -> tuple[bool, float, float]:
    time_hr = dist_nm / max(1e-6, cruise_speed_kt)
    reserve_gal = (reserve_min / 60.0) * burn_gph
    needed_gal = time_hr * burn_gph + reserve_gal
    return (needed_gal <= usable_fuel_gal, time_hr, needed_gal)


def plan_stop_sequence(
    dep: Airport,
    arr: Airport,
    airports: Dict[str, Airport],
    cruise_speed_kt: float,
    usable_fuel_gal: float,
    burn_gph: float,
    reserve_min: float,
    required_fuel: str,
    max_detour_factor: float,
    max_expansions: int = 400,
    max_neighbors: int = 60,
) -> Optional[List[Airport]]:
    """Quickly determine fuel stop sequence using straight-line distances.

    Returns ordered list of airports [dep, stop1, ..., arr] or None if no route.
    Uses only haversine distances (no SRTM/A*), so it's very fast.
    """
    import heapq

    # Check if direct is fuel-feasible
    direct_nm = _direct_nm(dep, arr)
    ok, _, _ = leg_fuel_ok(direct_nm * max_detour_factor, cruise_speed_kt,
                           usable_fuel_gal, burn_gph, reserve_min)
    if ok:
        return [dep, arr]

    want_100ll = required_fuel.upper() == "100LL"

    max_leg_time = (usable_fuel_gal / burn_gph) - (reserve_min / 60.0)
    if max_leg_time <= 0:
        return None
    max_leg_nm = max_leg_time * cruise_speed_kt

    # Build candidate fuel stops
    stop_candidates: List[Airport] = []
    for ap in airports.values():
        if ap.facility_use != "PU":
            continue
        if want_100ll and ap.fuel_100ll != 1:
            continue
        if (not want_100ll) and ap.fuel_jeta != 1:
            continue
        stop_candidates.append(ap)

    # Dijkstra over straight-line distances
    best_time: Dict[str, float] = {dep.icao: 0.0}
    prev: Dict[str, str] = {}
    pq: list[tuple[float, str]] = [(0.0, dep.icao)]
    expanded = 0
    radius_nm = max_leg_nm * max_detour_factor

    def get_airport(code: str) -> Airport:
        if code == dep.icao:
            return dep
        if code == arr.icao:
            return arr
        return airports[code]

    while pq and expanded < max_expansions:
        cur_t, cur_code = heapq.heappop(pq)
        if cur_t != best_time.get(cur_code, float("inf")):
            continue
        if cur_code == arr.icao:
            break

        expanded += 1
        cur_ap = get_airport(cur_code)

        neigh: List[tuple[float, Airport]] = []
        d_to_arr = _direct_nm(cur_ap, arr)
        if d_to_arr <= radius_nm:
            neigh.append((d_to_arr, arr))

        for ap in stop_candidates:
            if ap.icao == cur_code:
                continue
            d = _direct_nm(cur_ap, ap)
            if d <= radius_nm:
                neigh.append((d, ap))

        neigh.sort(key=lambda x: x[0])
        neigh = neigh[:max_neighbors]

        for d, nxt_ap in neigh:
            # Use straight-line distance with detour factor for fuel check
            ok, t_hr, _ = leg_fuel_ok(d * max_detour_factor, cruise_speed_kt,
                                      usable_fuel_gal, burn_gph, reserve_min)
            if not ok:
                continue
            new_t = cur_t + t_hr
            if new_t < best_time.get(nxt_ap.icao, float("inf")):
                best_time[nxt_ap.icao] = new_t
                prev[nxt_ap.icao] = cur_code
                heapq.heappush(pq, (new_t, nxt_ap.icao))

    if arr.icao not in best_time:
        return None

    # Reconstruct stop sequence
    sequence = []
    cur = arr.icao
    while cur != dep.icao:
        sequence.append(get_airport(cur))
        p = prev.get(cur)
        if not p:
            return None
        cur = p
    sequence.append(dep)
    sequence.reverse()
    return sequence


def plan_route_multi_stop(
    dep: Airport,
    arr: Airport,
    airports: Dict[str, Airport],
    cruise_speed_kt: float,
    usable_fuel_gal: float,
    burn_gph: float,
    reserve_min: float,
    max_msl_ft: float,
    min_agl_ft: float,
    required_fuel: str,
    max_detour_factor: float,
    max_expansions: int = 400,
    max_neighbors: int = 60,
) -> dict:
    """Multi-stop route planner (unbounded stops) minimizing total time.

    Implementation: Dijkstra over airports.
    - Nodes: dep, arr, and public-use fuel airports (for intermediate stops)
    - Edge feasibility: terrain_avoid_leg + fuel constraint

    Notes:
    - This is MVP-grade: we aggressively prune neighbor candidates by straight-line range
      and only expand up to max_expansions nodes to keep runtime bounded.
    """

    want_100ll = required_fuel.upper() == "100LL"

    # Max leg range based on fuel after reserve
    max_leg_time = (usable_fuel_gal / burn_gph) - (reserve_min / 60.0)
    if max_leg_time <= 0:
        return {
            "type": "no_route",
            "message": "No route found: reserve exceeds usable fuel.",
            "suggestions": ["Decrease reserve", "Increase usable fuel"],
        }
    max_leg_nm = max_leg_time * cruise_speed_kt

    # Build candidate stop list once
    stop_candidates: List[Airport] = []
    for ap in airports.values():
        if ap.facility_use != "PU":
            continue
        if want_100ll and ap.fuel_100ll != 1:
            continue
        if (not want_100ll) and ap.fuel_jeta != 1:
            continue
        stop_candidates.append(ap)

    # include destination as possible neighbor even if it lacks fuel
    # origin is always allowed

    import heapq

    # state keyed by airport ICAO
    best_time: Dict[str, float] = {dep.icao: 0.0}
    prev: Dict[str, tuple[str, dict]] = {}

    # cache terrain legs to avoid recomputation
    leg_cache: Dict[tuple[str, str], tuple[float, float, float, List[tuple[float, float]]]] = {}
    # (from,to) -> (dist_nm, time_hr, fuel_gal, path)

    pq: list[tuple[float, str]] = [(0.0, dep.icao)]
    expanded = 0

    def get_airport(code: str) -> Airport:
        if code == dep.icao:
            return dep
        if code == arr.icao:
            return arr
        return airports[code]

    while pq and expanded < max_expansions:
        cur_t, cur_code = heapq.heappop(pq)
        if cur_t != best_time.get(cur_code, float("inf")):
            continue
        if cur_code == arr.icao:
            break

        expanded += 1
        cur_ap = get_airport(cur_code)

        # Candidate neighbors: fuel stops + destination
        # Prune by straight-line range (allow some detour via factor)
        radius_nm = max_leg_nm * max_detour_factor

        neigh: List[tuple[float, Airport]] = []
        # destination
        d_to_arr = _direct_nm(cur_ap, arr)
        if d_to_arr <= radius_nm:
            neigh.append((d_to_arr, arr))

        for ap in stop_candidates:
            if ap.icao == cur_code:
                continue
            d = _direct_nm(cur_ap, ap)
            if d <= radius_nm:
                # additional prune: don't move away from destination too much (loose)
                neigh.append((d, ap))

        neigh.sort(key=lambda x: x[0])
        neigh = neigh[:max_neighbors]

        for _, nxt_ap in neigh:
            key = (cur_code, nxt_ap.icao)
            cached = leg_cache.get(key)
            if cached is None:
                leg = terrain_avoid_leg(cur_ap, nxt_ap, max_msl_ft, min_agl_ft, max_detour_factor)
                if not leg:
                    leg_cache[key] = (float("inf"), float("inf"), float("inf"), [])
                    continue
                ok, t_hr, gal = leg_fuel_ok(leg.dist_nm, cruise_speed_kt, usable_fuel_gal, burn_gph, reserve_min)
                if not ok:
                    leg_cache[key] = (float("inf"), float("inf"), float("inf"), [])
                    continue
                cached = (leg.dist_nm, t_hr, gal, leg.path_latlon)
                leg_cache[key] = cached

            dist_nm, t_hr, gal, path = cached
            if not math.isfinite(t_hr):
                continue

            new_t = cur_t + t_hr
            if new_t < best_time.get(nxt_ap.icao, float("inf")):
                best_time[nxt_ap.icao] = new_t
                prev[nxt_ap.icao] = (cur_code, {"from": cur_code, "to": nxt_ap.icao, "dist_nm": dist_nm, "time_hr": t_hr, "fuel_gal": gal, "path": path})
                heapq.heappush(pq, (new_t, nxt_ap.icao))

    if arr.icao not in best_time:
        return {
            "type": "no_route",
            "message": "No route found under current constraints.",
            "suggestions": [
                "Increase max detour factor",
                "Increase max MSL",
                "Decrease min AGL",
            ],
            "debug": {
                "max_leg_nm": max_leg_nm,
                "expanded_nodes": expanded,
                "stop_candidates": len(stop_candidates),
            },
        }

    # reconstruct legs
    legs_rev: List[dict] = []
    cur = arr.icao
    while cur != dep.icao:
        p = prev.get(cur)
        if not p:
            break
        cur, leg = p
        legs_rev.append(leg)

    legs = list(reversed(legs_rev))
    total_dist = sum(l["dist_nm"] for l in legs)
    total_time = sum(l["time_hr"] for l in legs)
    stops = [l["to"] for l in legs[:-1]]

    route_type = "direct" if len(legs) == 1 else "multi_stop"

    return {
        "type": route_type,
        "legs": legs,
        "total_time_hr": total_time,
        "total_dist_nm": total_dist,
        "stops": stops,
    }


def terrain_avoid_leg_streaming(
    a: Airport,
    b: Airport,
    max_msl_ft: float,
    min_agl_ft: float,
    max_detour_factor: float,
    cell_km: float = 1.0,
    initial_margin_km: float = 40.0,
    margin_step_km: float = 40.0,
    max_margin_km: float = 400.0,
):
    """Generator that yields A* exploration events for one leg.

    Yields dicts:
      {"type": "grid", "bounds": [sw_lat, sw_lon, ne_lat, ne_lon]}
      {"type": "explore", "cells": [[lat, lon], ...]}
      {"type": "path", "coords": [[lat, lon], ...], "dist_nm": float}
      {"type": "no_path"}
    """
    from mvp_backend.grid_astar import astar_path_streaming

    provider = SRTMProvider(cache_dir=os.path.join(ROOT, "mvp_backend", "srtm_cache"))

    direct_nm = _direct_nm(a, b)
    detour_limit_nm = max_detour_factor * direct_nm
    ceiling_ft = max_msl_ft - min_agl_ft

    mid_lat = 0.5 * (a.lat + b.lat)
    dlat = cell_km * _deg_per_km_lat()
    dlon = cell_km * _deg_per_km_lon(mid_lat)

    margin_km = initial_margin_km
    while margin_km <= max_margin_km:
        min_lat = min(a.lat, b.lat) - margin_km * _deg_per_km_lat()
        max_lat = max(a.lat, b.lat) + margin_km * _deg_per_km_lat()
        min_lon = min(a.lon, b.lon) - margin_km * _deg_per_km_lon(mid_lat)
        max_lon = max(a.lon, b.lon) + margin_km * _deg_per_km_lon(mid_lat)

        n_lat = int(math.ceil((max_lat - min_lat) / dlat)) + 1
        n_lon = int(math.ceil((max_lon - min_lon) / dlon)) + 1

        if n_lat * n_lon > 250_000:
            margin_km += margin_step_km
            continue

        grid = GridSpec(lat0=min_lat, lon0=min_lon, n_lat=n_lat, n_lon=n_lon, dlat=dlat, dlon=dlon)

        yield {"type": "grid", "bounds": [min_lat, min_lon, max_lat, max_lon]}

        points = [grid.idx_to_latlon(i, j) for i in range(n_lat) for j in range(n_lon)]
        elev_m = provider.get_many_m(points)
        elev_ft = [meters_to_feet(m) if m == m else float("inf") for m in elev_m]

        passable: List[List[bool]] = [[True] * n_lon for _ in range(n_lat)]
        k = 0
        for i in range(n_lat):
            row = passable[i]
            for j in range(n_lon):
                row[j] = elev_ft[k] <= ceiling_ft
                k += 1

        start = grid.latlon_to_idx(a.lat, a.lon)
        goal = grid.latlon_to_idx(b.lat, b.lon)
        if not passable[start[0]][start[1]] or not passable[goal[0]][goal[1]]:
            margin_km += margin_step_km
            continue

        for event in astar_path_streaming(grid, passable, start, goal, yield_every=30):
            if event["type"] == "path" and event["dist_nm"] > detour_limit_nm:
                break  # too long, try wider margin
            yield event
            if event["type"] in ("path", "no_path"):
                return

        margin_km += margin_step_km

    yield {"type": "no_path"}
